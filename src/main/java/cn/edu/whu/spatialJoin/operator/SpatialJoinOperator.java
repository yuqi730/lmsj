package cn.edu.whu.spatialJoin.operator;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.core.algorithms.SpatialJoinAlgorithms;
import cn.edu.whu.spatialJoin.common.Summary;
import cn.edu.whu.spatialJoin.common.SummaryHelper;
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeature;
import cn.edu.whu.spatialJoin.core.partitioning.helper.PartitionAssignmentStrategyHelper;
import cn.edu.whu.spatialJoin.core.partitioning.LAHilbertPartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.SpatialPartitioner;
import cn.edu.whu.spatialJoin.cost.PartCost;
import cn.edu.whu.spatialJoin.cost.PartProfile;
import cn.edu.whu.spatialJoin.execution.JobQueueExecutionPlan;
import cn.edu.whu.spatialJoin.execution.JobExecutionResult;
import cn.edu.whu.spatialJoin.execution.JobGroup;
import cn.edu.whu.spatialJoin.execution.JobDispatcher;
import cn.edu.whu.spatialJoin.execution.JobPlanner;
import cn.edu.whu.spatialJoin.enums.PartitionComplexityType;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.Feature;
import cn.edu.whu.spatialJoin.geom.IFeature;
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper;
import cn.edu.whu.spatialJoin.index.IndexHelper;
import cn.edu.whu.spatialJoin.utils.IntArray;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkEnv;
import org.apache.spark.rdd.PartitionPruningRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Spatial join entry for the unified operator layer.
 *
 * <p>The current implementation intentionally focuses on a verifiable first step:
 * it normalizes the left/right inputs, reads both sides through the common reader entry,
 * projects them to a compact join-time feature shape, and runs a local plane-sweep based
 * intersects preview using the existing join algorithms.
 *
 * <p>This keeps the operator contract stable now while leaving room to replace the local
 * execution path with a distributed partition-aware join later.
 */
public final class SpatialJoinOperator implements Serializable {

  private static final long serialVersionUID = 1L;
  /**
   * Raw prepared feature inputs stay on disk to keep executor heap available for the later
   * partition-local join phase.
   */
  private static final StorageLevel PREPARED_FEATURE_STORAGE_LEVEL = StorageLevel.DISK_ONLY();
  /**
   * Disk-hybrid execution keeps the temporary `(oid, feature)` staging data on disk only.
   *
   * <p>This working set belongs strictly to the load/materialization phase. It should not compete
   * with executor heap while the final partition files are being prepared, because the later
   * compute phase already reloads partition files on demand.
   */
  private static final StorageLevel DISK_HYBRID_PREPARED_FEATURE_STORAGE_LEVEL = StorageLevel.DISK_ONLY();
  /**
   * Final (partitionId, feature) working sets stay serialized in memory so later jobs can reuse
   * already partitioned data without going back to the OID-assignment join path.
   */
  private static final StorageLevel PREPARED_PARTITION_STORAGE_LEVEL = StorageLevel.MEMORY_ONLY_SER();
  private static final byte MATERIALIZED_RECORD_END = 0;
  private static final byte MATERIALIZED_RECORD_FEATURE_FAST = 1;
  private static final byte MATERIALIZED_RECORD_GENERIC = 2;
  private static final long LEFT_SAMPLE_SALT = 0x4c454654L;
  private static final long RIGHT_SAMPLE_SALT = 0x5249474854L;
  private static final long SAMPLE_GOLDEN_GAMMA = -7046029254386353131L;
  private static final long SAMPLE_MIX_MULTIPLIER_1 = -4658895280553007687L;
  private static final long SAMPLE_MIX_MULTIPLIER_2 = -7723592293110705685L;
  /**
   * Disk-hybrid partition construction uses a coarser synopsis grid than the eventual execution
   * partitions. A few extra orders keep the synopsis-driven candidate layout expressive enough
   * while still bounding driver memory.
   */
  private static final int DISK_HYBRID_SYNOPSIS_EXTRA_ORDERS = 4;
  private static final int DISK_HYBRID_EXACT_REFINEMENT_HISTOGRAM_BINS = 128;
  private static final int DISK_HYBRID_EXACT_REFINEMENT_MAX_ROUNDS = 8;
  private static final int FRONTIER_SIDE_LEFT = 0;
  private static final int FRONTIER_SIDE_RIGHT = 1;
  private static final int FRONTIER_SIDE_COUNT = 2;

  private final SpatialJoinOptions options;
  private transient long lastPreparationIntensityElapsedNanos;
  private transient long lastPartitionConstructElapsedNanos;
  private transient long lastPartitionMaterializationElapsedNanos;
  private transient int lastLightweightRefinementPassCount;
  private transient int lastLightweightRefinementFullInputScanCount;
  private transient int lastLightweightRefinementFrontierRemapCount;
  private transient int lastFullMaterializationPassCount;

  SpatialJoinOperator(SpatialJoinOptions options) {
    this.options = new SpatialJoinOptions(options);
  }

  /** Returns a defensive copy of the current join options. */
  public SpatialJoinOptions getOptions() {
    return new SpatialJoinOptions(options);
  }

  /** Returns the configured left input path. */
  public String getLeftInputPath() {
    String path = options.getString(SpatialJoinConstants.Operator.LEFT_INPUT_PATH);
    if (path == null || path.trim().isEmpty()) {
      throw new IllegalStateException("Missing left input path option: " + SpatialJoinConstants.Operator.LEFT_INPUT_PATH);
    }
    return path;
  }

  /** Returns the configured right input path. */
  public String getRightInputPath() {
    String path = options.getString(SpatialJoinConstants.Operator.RIGHT_INPUT_PATH);
    if (path == null || path.trim().isEmpty()) {
      throw new IllegalStateException("Missing right input path option: " + SpatialJoinConstants.Operator.RIGHT_INPUT_PATH);
    }
    return path;
  }

  /** Returns the normalized left input format. */
  public String getLeftInputFormat() {
    return normalizeFormat(options.getString(
        SpatialJoinConstants.Operator.LEFT_INPUT_FORMAT,
        options.getString(SpatialJoinConstants.Operator.INPUT_FORMAT)));
  }

  /** Returns the normalized right input format. */
  public String getRightInputFormat() {
    return normalizeFormat(options.getString(
        SpatialJoinConstants.Operator.RIGHT_INPUT_FORMAT,
        options.getString(SpatialJoinConstants.Operator.INPUT_FORMAT)));
  }

  /** Returns the configured spatial predicate name. */
  public String getJoinPredicate() {
    return options.getString(
        SpatialJoinConstants.Operator.JOIN_PREDICATE,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_PREDICATE);
  }

  /** Returns the configured join algorithm label. */
  public String getJoinAlgorithm() {
    return options.getString(
        SpatialJoinConstants.Operator.JOIN_ALGORITHM,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_ALGORITHM);
  }

  /** Returns the simplification threshold used by the current preview join path. */
  public int getSimplificationThreshold() {
    return Math.max(1, options.getInt(
        SpatialJoinConstants.Operator.JOIN_SIMPLIFICATION_THRESHOLD,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_SIMPLIFICATION_THRESHOLD));
  }

  /** Returns the online input-sampling fraction used by scalability experiments. */
  public double getInputSampleFraction() {
    double fraction = options.getDouble(
        SpatialJoinConstants.Operator.INPUT_SAMPLE_FRACTION,
        SpatialJoinConstants.Operator.DEFAULT_INPUT_SAMPLE_FRACTION);
    if (!Double.isFinite(fraction))
      return SpatialJoinConstants.Operator.DEFAULT_INPUT_SAMPLE_FRACTION;
    return Math.max(0.0d, Math.min(1.0d, fraction));
  }

  /** Returns the deterministic seed used by online input sampling. */
  public long getInputSampleSeed() {
    return options.getLong(
        SpatialJoinConstants.Operator.INPUT_SAMPLE_SEED,
        SpatialJoinConstants.Operator.DEFAULT_INPUT_SAMPLE_SEED);
  }

  /** Returns whether complex geometries should be quad-split before local plane-sweep filtering. */
  public boolean isJoinSimplificationEnabled() {
    return options.getBoolean(
        SpatialJoinConstants.Operator.JOIN_SIMPLIFICATION_ENABLED,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_SIMPLIFICATION_ENABLED);
  }

  /** Returns whether complex geometries should be split before partition construction/materialization. */
  public boolean isPrePartitionSplitEnabled() {
    return options.getBoolean(
        SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_ENABLED,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_PRE_PARTITION_SPLIT_ENABLED);
  }

  /** Returns the maximum number of fragments produced by one pre-partition split source geometry. */
  public int getPrePartitionSplitMaxFragments() {
    return Math.max(1, options.getInt(
        SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_MAX_FRAGMENTS,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_PRE_PARTITION_SPLIT_MAX_FRAGMENTS));
  }

  /** Returns the maximum quad-split depth for one pre-partition split source geometry. */
  public int getPrePartitionSplitMaxDepth() {
    return Math.max(0, options.getInt(
        SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_MAX_DEPTH,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_PRE_PARTITION_SPLIT_MAX_DEPTH));
  }

  /** Returns the configured plane-sweep scan-axis mode used inside local partition joins. */
  public String getPlaneSweepAxis() {
    return options.getString(
        SpatialJoinConstants.Operator.JOIN_PLANE_SWEEP_AXIS,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_PLANE_SWEEP_AXIS);
  }

  /** Returns the configured plane-sweep implementation variant used inside local partition joins. */
  public String getPlaneSweepImplementation() {
    return options.getString(
        SpatialJoinConstants.Operator.JOIN_PLANE_SWEEP_IMPLEMENTATION,
        SpatialJoinConstants.Operator.DEFAULT_JOIN_PLANE_SWEEP_IMPLEMENTATION);
  }

  /**
   * Returns the maximum compact assignment-index size allowed for the broadcast-based
   * prepared working-set materialization path.
   *
   * <p>When the estimated lookup table exceeds this threshold, the operator falls back to the
   * older OID-join replay path instead of broadcasting a large short-lived structure.
   */
  public long getPreparedAssignmentBroadcastMaxBytes() {
    return options.getSizeAsBytes(
        SpatialJoinConstants.Execution.PREPARED_ASSIGNMENT_BROADCAST_MAX_BYTES,
        SpatialJoinConstants.Execution.DEFAULT_PREPARED_ASSIGNMENT_BROADCAST_MAX_BYTES_BYTES);
  }

  /** Returns the configured feature-complexity mode used during partition planning. */
  public String getComplexityMode() {
    return options.getString(
        SpatialJoinConstants.Execution.COMPLEXITY_MODE,
        SpatialJoinConstants.Execution.DEFAULT_COMPLEXITY_MODE);
  }

  /** Returns the configured partition-load estimate mode used by execution planning. */
  public String getPartitionLoadEstimateMode() {
    return options.getString(
        SpatialJoinConstants.Execution.PARTITION_LOAD_ESTIMATE_MODE,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  /** Returns the load estimate used only to construct and recursively refine partition boundaries. */
  public String getPartitionRefinementLoadEstimateMode() {
    return options.getString(
        SpatialJoinConstants.Execution.PARTITION_REFINEMENT_LOAD_ESTIMATE_MODE,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_REFINEMENT_LOAD_ESTIMATE_MODE);
  }

  /** Returns the configured scalar partition-intensity mode used by job construction. */
  public String getPartitionIntensityMode() {
    return normalizePartitionIntensityMode(options.getString(
        SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_INTENSITY_MODE));
  }

  /**
   * Returns the driver-side time spent deriving final partition intensities during the most recent
   * materialized preparation. Feature profiling performed while mapping and persisting partition
   * data remains part of the partition-preparation time.
   */
  public long getLastPreparationIntensityElapsedMillis() {
    return Math.max(0L, lastPreparationIntensityElapsedNanos / 1_000_000L);
  }

  /** Returns the candidate partition-layout construction time from the latest preparation. */
  public long getLastPartitionConstructElapsedMillis() {
    return Math.max(0L, lastPartitionConstructElapsedNanos / 1_000_000L);
  }

  /** Returns the partition mapping and persistent-cache materialization time from the latest preparation. */
  public long getLastPartitionMaterializationElapsedMillis() {
    return Math.max(0L, lastPartitionMaterializationElapsedNanos / 1_000_000L);
  }

  /** Returns the number of lightweight exact-statistics passes used to converge the latest layout. */
  public int getLastLightweightRefinementPassCount() {
    return Math.max(0, lastLightweightRefinementPassCount);
  }

  /** Returns how many full left/right input scans initialized the latest exact refinement. */
  public int getLastLightweightRefinementFullInputScanCount() {
    return Math.max(0, lastLightweightRefinementFullInputScanCount);
  }

  /** Returns how many split-frontier remapping rounds followed the initial full scan. */
  public int getLastLightweightRefinementFrontierRemapCount() {
    return Math.max(0, lastLightweightRefinementFrontierRemapCount);
  }

  /** Returns the number of full-feature partition write passes used by the latest preparation. */
  public int getLastFullMaterializationPassCount() {
    return Math.max(0, lastFullMaterializationPassCount);
  }

  private static String normalizePartitionIntensityMode(String mode) {
    String normalized = mode == null ? "" : mode.trim().toLowerCase();
    if (SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE_CANDIDATE_AVG_COMPLEXITY.equals(normalized))
      return SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE_CANDIDATE_AVG_COMPLEXITY;
    if (SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE_CI_HYBRID.equals(normalized)
        || "feature-count-candidate".equals(normalized)
        || "feature-count-candidate-avg-vertex".equals(normalized)
        || "feature-count-candidate-avg-vertex-intensity".equals(normalized)
        || "featurecountcandidateavgvertexintensity".equals(normalized)
        || "ci-driven".equals(normalized)
        || "ours-ci".equals(normalized))
      return SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE_CI_HYBRID;
    return SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE_SIZEFUNCTION;
  }

  private boolean usesCandidateAverageComplexityIntensity() {
    return SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE_CANDIDATE_AVG_COMPLEXITY
        .equals(getPartitionIntensityMode());
  }

  private boolean usesCiHybridPartitionIntensity() {
    return SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE_CI_HYBRID
        .equals(getPartitionIntensityMode());
  }

  private boolean usesDerivedPartitionIntensity() {
    return usesCandidateAverageComplexityIntensity() || usesCiHybridPartitionIntensity();
  }

  public double getPartitionIntensityCiAlpha() {
    return clamp01(options.getDouble(
        SpatialJoinConstants.Execution.PARTITION_INTENSITY_CI_ALPHA,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_INTENSITY_CI_ALPHA));
  }

  private static double clamp01(double value) {
    if (Double.isNaN(value))
      return 0.0d;
    if (value < 0.0d)
      return 0.0d;
    if (value > 1.0d)
      return 1.0d;
    return value;
  }

  /** Returns the object-pair de-duplication mode applied after partition-local duplicate filtering. */
  public String getObjectPairDedupMode() {
    return normalizeObjectPairDedupMode(options.getString(
        SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_MODE,
        SpatialJoinConstants.Execution.DEFAULT_OBJECT_PAIR_DEDUP_MODE));
  }

  /** Returns whether the execution should recover object-level unique pairs globally. */
  public boolean isGlobalObjectPairDedupEnabled() {
    return SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_MODE_GLOBAL.equals(getObjectPairDedupMode());
  }

  /** Returns whether object-level global recovery time should be counted in compute-stage timing. */
  public boolean isObjectPairDedupCountInComputeTime() {
    return options.getBoolean(
        SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_COUNT_IN_COMPUTE_TIME,
        SpatialJoinConstants.Execution.DEFAULT_OBJECT_PAIR_DEDUP_COUNT_IN_COMPUTE_TIME);
  }

  private static String normalizeObjectPairDedupMode(String mode) {
    String normalized = mode == null ? "" : mode.trim().toLowerCase();
    if (SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_MODE_GLOBAL.equals(normalized))
      return SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_MODE_GLOBAL;
    return SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_MODE_NONE;
  }

  /** Returns the normalized partition-assignment strategy used after resource-aware partition construction. */
  public String getPartitionAssignmentStrategy() {
    return PartitionAssignmentStrategyHelper.normalizeStrategy(options.getString(
        SpatialJoinConstants.Execution.PARTITION_ASSIGNMENT_STRATEGY,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY));
  }

  /**
   * Returns whether later partition-local execution should still apply the reference-point
   * duplicate-avoidance rule for the given assignment strategy.
   *
   * <p>The currently supported asymmetric strategy still keeps replicated anchor-side semantics,
   * so the existing partition-envelope based duplicate filter remains required for every
   * non-baseline strategy.
   */
  private static boolean usesReferencePointDuplicateAvoidance(String partitionAssignmentStrategy) {
    return true;
  }

  /** Returns whether execution should reuse the prepared working set in memory or materialize partitions to disk. */
  public String getWorkingSetMode() {
    String configuredMode = options.getString(
        SpatialJoinConstants.Execution.WORKING_SET_MODE,
        SpatialJoinConstants.Execution.DEFAULT_WORKING_SET_MODE);
    if (SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_MEMORY.equalsIgnoreCase(configuredMode))
      return SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID;
    return configuredMode;
  }

  /** Returns how planned jobs should be executed: concurrently or sequentially. */
  public String getJobExecutionMode() {
    return options.getString(
        SpatialJoinConstants.Execution.JOB_EXECUTION_MODE,
        SpatialJoinConstants.Execution.DEFAULT_JOB_EXECUTION_MODE);
  }

  /** Returns the optional base path where disk-hybrid execution materializes final partition files. */
  public String getDiskMaterializationBasePath() {
    return options.getString(
        SpatialJoinConstants.Execution.DISK_MATERIALIZATION_BASE_PATH,
        SpatialJoinConstants.Execution.DEFAULT_DISK_MATERIALIZATION_BASE_PATH);
  }

  /** Returns whether disk-hybrid preparation should reload materialized files and verify their contents. */
  public boolean isValidateMaterializedPartitions() {
    return options.getBoolean(
        SpatialJoinConstants.Execution.DISK_VALIDATE_MATERIALIZED,
        SpatialJoinConstants.Execution.DEFAULT_DISK_VALIDATE_MATERIALIZED);
  }

  /** Returns whether disk-hybrid execution should keep its materialized partition files after completion. */
  public boolean isKeepMaterializedPartitions() {
    return options.getBoolean(
        SpatialJoinConstants.Execution.DISK_KEEP_MATERIALIZED,
        SpatialJoinConstants.Execution.DEFAULT_DISK_KEEP_MATERIALIZED);
  }

  /** Returns whether sequential debug runs should print fine-grained per-job timing breakdowns. */
  public boolean isProfileSequentialJobs() {
    return options.getBoolean(
        SpatialJoinConstants.Execution.DEBUG_PROFILE_JOBS,
        SpatialJoinConstants.Execution.DEFAULT_DEBUG_PROFILE_JOBS);
  }

  /** Returns whether partition-local execution should print per-partition audit counters. */
  public boolean isAuditPartitionJoin() {
    return options.getBoolean(
        SpatialJoinConstants.Execution.DEBUG_AUDIT_PARTITION_JOIN,
        SpatialJoinConstants.Execution.DEFAULT_DEBUG_AUDIT_PARTITION_JOIN);
  }

  /** Returns whether invalid geometries should be dropped immediately after reading. */
  public boolean isFilterInvalidGeometriesEarly() {
    return options.getBoolean(
        SpatialJoinConstants.Execution.FILTER_INVALID_GEOMETRIES_EARLY,
        SpatialJoinConstants.Execution.DEFAULT_FILTER_INVALID_GEOMETRIES_EARLY);
  }

  /** Creates the read helper bound to the left input. */
  public ReadOperator leftReader() {
    return new ReadOperator(buildInputOptions(getLeftInputPath(), getLeftInputFormat()));
  }

  /**
   * Builds the reader wrapper for the right dataset.
   *
   * @return right-side read operator
   */
  public ReadOperator rightReader() {
    return new ReadOperator(buildInputOptions(getRightInputPath(), getRightInputFormat()));
  }

  /** Reads the left dataset as a distributed feature RDD. */
  public JavaRDD<IFeature> readLeft(JavaSparkContext sparkContext) {
    return sampleInputIfConfigured(
        filterInvalidGeometriesIfConfigured(leftReader().readJ(sparkContext)),
        true);
  }

  /**
   * Reads the right dataset as a distributed feature RDD.
   *
   * @param sparkContext active Spark context
   * @return right-side feature RDD
   */
  public JavaRDD<IFeature> readRight(JavaSparkContext sparkContext) {
    return sampleInputIfConfigured(
        filterInvalidGeometriesIfConfigured(rightReader().readJ(sparkContext)),
        false);
  }

  private JavaRDD<IFeature> sampleInputIfConfigured(JavaRDD<IFeature> features, boolean leftSide) {
    if (features == null)
      return null;
    final double fraction = getInputSampleFraction();
    if (fraction >= 1.0d)
      return features;
    if (fraction <= 0.0d)
      return features.filter(feature -> false);
    final long seed = getInputSampleSeed();
    final long salt = leftSide ? LEFT_SAMPLE_SALT : RIGHT_SAMPLE_SALT;
    return features
        .zipWithUniqueId()
        .filter(tuple -> tuple != null && shouldKeepSampledInput(tuple._2(), seed, salt, fraction))
        .keys();
  }

  private static boolean shouldKeepSampledInput(long rowIndex, long seed, long salt, double fraction) {
    long mixed = sampleMix64(rowIndex ^ sampleMix64(seed ^ salt));
    double score = (mixed >>> 11) / (double) (1L << 53);
    return score < fraction;
  }

  private static long sampleMix64(long value) {
    long z = value + SAMPLE_GOLDEN_GAMMA;
    z = (z ^ (z >>> 30)) * SAMPLE_MIX_MULTIPLIER_1;
    z = (z ^ (z >>> 27)) * SAMPLE_MIX_MULTIPLIER_2;
    return z ^ (z >>> 31);
  }

  /**
   * Drops invalid geometries immediately after reading when the current execution options request it.
   *
   * <p>This early filter keeps self-intersecting or otherwise invalid shapes out of summaries,
   * partition construction, materialization, and later job transport. The filter intentionally
   * treats empty or non-2D geometries as unusable as well because they cannot participate in the
   * current 2D join pipeline.
   */
  private JavaRDD<IFeature> filterInvalidGeometriesIfConfigured(JavaRDD<IFeature> features) {
    if (features == null || !isFilterInvalidGeometriesEarly()) {
      return features;
    }
    return features.filter(SpatialJoinOperator::isValidEarlyJoinFeature);
  }

  /** Returns true only for features whose geometry can safely enter the 2D join pipeline. */
  private static boolean isValidEarlyJoinFeature(IFeature feature) {
    if (feature == null) {
      return false;
    }
    final Geometry geometry;
    try {
      geometry = feature.getGeometry();
    } catch (RuntimeException e) {
      return false;
    }
    if (geometry == null || geometry.isEmpty() || GeometryHelper.getCoordinateDimension(geometry) != 2) {
      return false;
    }
    try {
      return geometry.isValid();
    } catch (RuntimeException e) {
      return false;
    }
  }

  /** Counts left-side features using Spark. */
  public long countLeft(JavaSparkContext sparkContext) {
    return readLeft(sparkContext).count();
  }

  /**
   * Counts right-side features using Spark.
   *
   * @param sparkContext active Spark context
   * @return right-side feature count
   */
  public long countRight(JavaSparkContext sparkContext) {
    return readRight(sparkContext).count();
  }

  /**
   * Computes an exact left-side dataset summary from the loaded features.
   *
   * <p>The returned size is accumulated from {@link IFeature#getHeapEstimateBytes()}
   * for every loaded feature rather than being caller-estimated.
   *
   * @param sparkContext active Spark context
   * @return left dataset summary
   */
  public Summary summarizeLeft(JavaSparkContext sparkContext) {
    return SummaryHelper.computeFor(readLeft(sparkContext), getPartitionLoadEstimateMode());
  }

  /**
   * Computes an exact left-side dataset summary using the materialized-geometry heap estimate.
   *
   * <p>Unlike {@link #summarizeLeft(JavaSparkContext)}, this summary does not add the retained
   * binary-geometry payload on top of the materialized runtime geometry size in the {@code size}
   * field. It is intended for experiment-side footprint reporting rather than partition planning.
   */
  public Summary summarizeLeftMaterializedGeometry(JavaSparkContext sparkContext) {
    return SummaryHelper.computeForMaterializedGeometry(readLeft(sparkContext));
  }

  /**
   * Computes an exact right-side dataset summary from the loaded features.
   *
   * @param sparkContext active Spark context
   * @return right dataset summary
   */
  public Summary summarizeRight(JavaSparkContext sparkContext) {
    return SummaryHelper.computeFor(readRight(sparkContext), getPartitionLoadEstimateMode());
  }

  /**
   * Computes an exact right-side dataset summary using the materialized-geometry heap estimate.
   *
   * <p>This is the right-side counterpart of
   * {@link #summarizeLeftMaterializedGeometry(JavaSparkContext)} and is meant for experiment-side
   * footprint reporting only.
   */
  public Summary summarizeRightMaterializedGeometry(JavaSparkContext sparkContext) {
    return SummaryHelper.computeForMaterializedGeometry(readRight(sparkContext));
  }

  /**
   * Builds a copy of the current options enriched with LAHilbert left/right
   * summary metadata derived from the actual loaded datasets.
   *
   * <p>This is the preferred path for load-aware partition construction because
   * the total dataset sizes come from real feature sizes collected during
   * reading, while only the overlap-relevant portion is derived later by the
   * partitioner.
   *
   * @param sparkContext active Spark context
   * @return options ready for LAHilbert partition construction
   */
  public SpatialJoinOptions buildLAHilbertPartitionOptions(JavaSparkContext sparkContext) {
    return buildLAHilbertPartitionOptions(summarizeLeft(sparkContext), summarizeRight(sparkContext));
  }

  /**
   * Builds a copy of the current options enriched with the provided left/right
   * dataset summaries for LAHilbert partition construction.
   *
   * @param leftSummary left dataset summary
   * @param rightSummary right dataset summary
   * @return options ready for LAHilbert partition construction
   */
  public SpatialJoinOptions buildLAHilbertPartitionOptions(Summary leftSummary, Summary rightSummary) {
    SpatialJoinOptions partitionOptions = new SpatialJoinOptions(options);
    SummaryHelper.applyLAHilbertDatasetSummaries(partitionOptions, leftSummary, rightSummary);
    return partitionOptions;
  }

  /**
   * Creates the future partition-job planner configured by the current options.
   *
   * <p>This planner is intended for the later execution stage where partition
   * loads have been collected and need to be turned into a queue of Spark jobs.
   *
   * @return configured planner
   */
  public JobPlanner createJobPlanner() {
    return JobPlanner.fromOptions(options);
  }

  /**
   * Creates the dispatcher scaffold that will later push planned partition jobs
   * according to the configured concurrency limits.
   *
   * @return configured dispatcher
   */
  public JobDispatcher createJobDispatcher() {
    return JobDispatcher.fromOptions(options);
  }

  /**
   * Driver-side prepared execution context that reuses one persisted OID-to-feature mapping per dataset.
   *
   * <p>This context avoids rebuilding the input RDD lineage for every later partition-local job.
   * The partition-aware execution path should prefer this wrapper whenever it needs to load
   * selected partitions repeatedly from the same left/right inputs.
   */
  public static final class PreparedPartitionExecution implements Closeable {
    private final JavaPairRDD<Long, IFeature> leftFeaturesByOid;
    private final JavaPairRDD<Long, IFeature> rightFeaturesByOid;
    private final JavaPairRDD<Integer, IFeature> leftPartitionedFeatures;
    private final JavaPairRDD<Integer, IFeature> rightPartitionedFeatures;
    private final Summary leftSummary;
    private final Summary rightSummary;
    private final long leftFeatureCount;
    private final long rightFeatureCount;
    private final SpatialPartitioner.EffectivePlan effectivePlan;
    private final Broadcast<SpatialPartitioner.AssignmentLookupTable> leftPreparedLookupBroadcast;
    private final Broadcast<SpatialPartitioner.AssignmentLookupTable> rightPreparedLookupBroadcast;
    private final String partitionAssignmentStrategy;
    private final String anchorSide;

    private PreparedPartitionExecution(JavaPairRDD<Long, IFeature> leftFeaturesByOid,
                                       JavaPairRDD<Long, IFeature> rightFeaturesByOid,
                                       JavaPairRDD<Integer, IFeature> leftPartitionedFeatures,
                                       JavaPairRDD<Integer, IFeature> rightPartitionedFeatures,
                                       Summary leftSummary,
                                       Summary rightSummary,
                                       long leftFeatureCount,
                                       long rightFeatureCount,
                                       SpatialPartitioner.EffectivePlan effectivePlan,
                                       Broadcast<SpatialPartitioner.AssignmentLookupTable> leftPreparedLookupBroadcast,
                                       Broadcast<SpatialPartitioner.AssignmentLookupTable> rightPreparedLookupBroadcast,
                                       String partitionAssignmentStrategy,
                                       String anchorSide) {
      this.leftFeaturesByOid = leftFeaturesByOid;
      this.rightFeaturesByOid = rightFeaturesByOid;
      this.leftPartitionedFeatures = leftPartitionedFeatures;
      this.rightPartitionedFeatures = rightPartitionedFeatures;
      this.leftSummary = leftSummary == null ? new Summary() : leftSummary;
      this.rightSummary = rightSummary == null ? new Summary() : rightSummary;
      this.leftFeatureCount = Math.max(0L, leftFeatureCount);
      this.rightFeatureCount = Math.max(0L, rightFeatureCount);
      this.effectivePlan = effectivePlan;
      this.leftPreparedLookupBroadcast = leftPreparedLookupBroadcast;
      this.rightPreparedLookupBroadcast = rightPreparedLookupBroadcast;
      this.partitionAssignmentStrategy = partitionAssignmentStrategy == null
          ? SpatialJoinConstants.Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY
          : partitionAssignmentStrategy;
      this.anchorSide = anchorSide == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : anchorSide;
    }

    /** Returns the persisted left-side {@code (oid, feature)} RDD reused across partition-local jobs. */
    public JavaPairRDD<Long, IFeature> getLeftFeaturesByOid() {
      return leftFeaturesByOid;
    }

    /** Returns the persisted right-side {@code (oid, feature)} RDD reused across partition-local jobs. */
    public JavaPairRDD<Long, IFeature> getRightFeaturesByOid() {
      return rightFeaturesByOid;
    }

    /** Returns the left-side job-ready `(partitionId, feature)` working set partitioned by the final effective partitioner. */
    public JavaPairRDD<Integer, IFeature> getLeftPartitionedFeatures() {
      return leftPartitionedFeatures;
    }

    /** Returns the right-side job-ready `(partitionId, feature)` working set partitioned by the final effective partitioner. */
    public JavaPairRDD<Integer, IFeature> getRightPartitionedFeatures() {
      return rightPartitionedFeatures;
    }

    /** Returns the exact left-side dataset summary computed from the prepared features. */
    public Summary getLeftSummary() {
      return leftSummary;
    }

    /** Returns the exact right-side dataset summary computed from the prepared features. */
    public Summary getRightSummary() {
      return rightSummary;
    }

    /** Returns the left-side feature count already materialized during preparation. */
    public long getLeftFeatureCount() {
      return leftFeatureCount;
    }

    /** Returns the right-side feature count already materialized during preparation. */
    public long getRightFeatureCount() {
      return rightFeatureCount;
    }

    /** Returns the effective partition plan built from the prepared left/right inputs. */
    public SpatialPartitioner.EffectivePlan getEffectivePlan() {
      return effectivePlan;
    }

    /** Returns which assignment strategy produced the current effective plan. */
    public String getPartitionAssignmentStrategy() {
      return partitionAssignmentStrategy;
    }

    /** Returns the chosen anchor side, or {@code none} when the strategy is symmetric. */
    public String getAnchorSide() {
      return anchorSide;
    }

    @Override
    public void close() throws IOException {
      closeBroadcastQuietly(leftPreparedLookupBroadcast);
      closeBroadcastQuietly(rightPreparedLookupBroadcast);
      if (leftPartitionedFeatures != null)
        leftPartitionedFeatures.unpersist(true);
      if (rightPartitionedFeatures != null)
        rightPartitionedFeatures.unpersist(true);
      if (leftFeaturesByOid != null)
        leftFeaturesByOid.unpersist(true);
      if (rightFeaturesByOid != null)
        rightFeaturesByOid.unpersist(true);
    }
  }

  /** Aggregated metrics returned by one compact partition-local job execution. */
  static final class PartitionTaskExecutionSummary implements Serializable {
    private final long matchCount;
    private final long localJoinNanos;
    private final long[] partitionLocalJoinNanos;
    private final long planeSweepXAxisCalls;
    private final long planeSweepYAxisCalls;
    private final long planeSweepDiagonalAxisCalls;

    PartitionTaskExecutionSummary(long matchCount,
                                  long planeSweepXAxisCalls,
                                  long planeSweepYAxisCalls,
                                  long planeSweepDiagonalAxisCalls) {
      this(matchCount, 0L, null, planeSweepXAxisCalls, planeSweepYAxisCalls, planeSweepDiagonalAxisCalls);
    }

    PartitionTaskExecutionSummary(long matchCount,
                                  long localJoinNanos,
                                  long planeSweepXAxisCalls,
                                  long planeSweepYAxisCalls,
                                  long planeSweepDiagonalAxisCalls) {
      this(matchCount, localJoinNanos, null, planeSweepXAxisCalls, planeSweepYAxisCalls, planeSweepDiagonalAxisCalls);
    }

    PartitionTaskExecutionSummary(long matchCount,
                                  long localJoinNanos,
                                  long[] partitionLocalJoinNanos,
                                  long planeSweepXAxisCalls,
                                  long planeSweepYAxisCalls,
                                  long planeSweepDiagonalAxisCalls) {
      this.matchCount = Math.max(0L, matchCount);
      this.localJoinNanos = Math.max(0L, localJoinNanos);
      this.partitionLocalJoinNanos =
          partitionLocalJoinNanos == null ? new long[0] : java.util.Arrays.copyOf(partitionLocalJoinNanos, partitionLocalJoinNanos.length);
      this.planeSweepXAxisCalls = Math.max(0L, planeSweepXAxisCalls);
      this.planeSweepYAxisCalls = Math.max(0L, planeSweepYAxisCalls);
      this.planeSweepDiagonalAxisCalls = Math.max(0L, planeSweepDiagonalAxisCalls);
    }

    long getMatchCount() {
      return matchCount;
    }

    long getLocalJoinNanos() {
      return localJoinNanos;
    }

    long[] getPartitionLocalJoinNanos() {
      return java.util.Arrays.copyOf(partitionLocalJoinNanos, partitionLocalJoinNanos.length);
    }

    long getPlaneSweepXAxisCalls() {
      return planeSweepXAxisCalls;
    }

    long getPlaneSweepYAxisCalls() {
      return planeSweepYAxisCalls;
    }

    long getPlaneSweepDiagonalAxisCalls() {
      return planeSweepDiagonalAxisCalls;
    }
  }

  /**
   * Iterator over a task-local object-pair spill file.
   *
   * <p>The optional object-level recovery mode can generate many pair records for dense partitions.
   * Keeping those records in a {@code HashSet} would move the pressure from JTS geometry processing
   * to pair buffering. This iterator reads back a task-local file and deletes it as soon as Spark has
   * consumed the partition output.
   */
  private static final class DeletingLineIterator implements Iterator<String>, Closeable {
    private final File file;
    private BufferedReader reader;
    private String nextLine;
    private boolean closed;

    private DeletingLineIterator(File file) {
      this.file = file;
      try {
        this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
        advance();
      } catch (IOException e) {
        closeQuietly();
        throw new RuntimeException("Failed to read task-local object-pair spill file " + file, e);
      }
    }

    @Override
    public boolean hasNext() {
      return nextLine != null;
    }

    @Override
    public String next() {
      if (nextLine == null)
        throw new NoSuchElementException("No more object-pair records");
      String current = nextLine;
      advance();
      return current;
    }

    private void advance() {
      try {
        nextLine = reader == null ? null : reader.readLine();
        if (nextLine == null)
          close();
      } catch (IOException e) {
        closeQuietly();
        throw new RuntimeException("Failed to advance task-local object-pair spill file " + file, e);
      }
    }

    @Override
    public void close() throws IOException {
      if (closed)
        return;
      closed = true;
      IOException closeFailure = null;
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          closeFailure = e;
        } finally {
          reader = null;
        }
      }
      deleteSpillFile();
      if (closeFailure != null)
        throw closeFailure;
    }

    private void closeQuietly() {
      try {
        close();
      } catch (IOException ignored) {
        // Best-effort cleanup during failed task output iteration.
      }
    }

    private void deleteSpillFile() {
      if (file != null && file.exists() && !file.delete())
        file.deleteOnExit();
    }
  }

  /** Lightweight audit payload describing the expected per-partition input counts for one local join. */
  private static final class PartitionJoinAuditContext implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String executionSource;
    private final int jobId;
    private final long expectedLeftCount;
    private final long expectedRightCount;

    private PartitionJoinAuditContext(String executionSource,
                                      int jobId,
                                      long expectedLeftCount,
                                      long expectedRightCount) {
      this.executionSource = executionSource == null ? "unknown" : executionSource;
      this.jobId = jobId;
      this.expectedLeftCount = Math.max(0L, expectedLeftCount);
      this.expectedRightCount = Math.max(0L, expectedRightCount);
    }
  }

  /** Validation summary for one side of the disk-hybrid partition materialization. */
  public static final class MaterializedPartitionSideValidation implements Serializable {
    private final String side;
    private final long expectedFeatureCount;
    private final long reloadedFeatureCount;
    private final boolean partitionCountsMatch;
    private final boolean contentMatch;
    private final List<Integer> mismatchedPartitionIds;

    private MaterializedPartitionSideValidation(String side,
                                                long expectedFeatureCount,
                                                long reloadedFeatureCount,
                                                boolean partitionCountsMatch,
                                                boolean contentMatch,
                                                List<Integer> mismatchedPartitionIds) {
      this.side = side;
      this.expectedFeatureCount = Math.max(0L, expectedFeatureCount);
      this.reloadedFeatureCount = Math.max(0L, reloadedFeatureCount);
      this.partitionCountsMatch = partitionCountsMatch;
      this.contentMatch = contentMatch;
      this.mismatchedPartitionIds = mismatchedPartitionIds == null ? Collections.emptyList() : mismatchedPartitionIds;
    }

    public String getSide() {
      return side;
    }

    public long getExpectedFeatureCount() {
      return expectedFeatureCount;
    }

    public long getReloadedFeatureCount() {
      return reloadedFeatureCount;
    }

    public boolean isPartitionCountsMatch() {
      return partitionCountsMatch;
    }

    public boolean isContentMatch() {
      return contentMatch;
    }

    public List<Integer> getMismatchedPartitionIds() {
      return mismatchedPartitionIds;
    }

    public boolean isPassed() {
      return partitionCountsMatch && contentMatch;
    }
  }

  /** Validation summary comparing all materialized partition files with the pre-materialization pair RDD content. */
  public static final class MaterializedPartitionValidation implements Serializable {
    private final MaterializedPartitionSideValidation leftValidation;
    private final MaterializedPartitionSideValidation rightValidation;

    private MaterializedPartitionValidation(MaterializedPartitionSideValidation leftValidation,
                                            MaterializedPartitionSideValidation rightValidation) {
      this.leftValidation = leftValidation;
      this.rightValidation = rightValidation;
    }

    public MaterializedPartitionSideValidation getLeftValidation() {
      return leftValidation;
    }

    public MaterializedPartitionSideValidation getRightValidation() {
      return rightValidation;
    }

    public boolean isPassed() {
      return (leftValidation == null || leftValidation.isPassed())
          && (rightValidation == null || rightValidation.isPassed());
    }
  }

  /**
   * Driver-side execution context for the disk-hybrid working-set mode.
   *
   * <p>The final effective partition metadata stays on the driver, while left/right partition-local
   * binary features are materialized to external storage under one root directory. Later jobs then
   * reload only the files belonging to their target partition ids.
   */
  public static final class MaterializedPartitionExecution implements Closeable {
    private final JavaSparkContext sparkContext;
    private final Summary leftSummary;
    private final Summary rightSummary;
    private final long leftFeatureCount;
    private final long rightFeatureCount;
    private final SpatialPartitioner.EffectivePlan effectivePlan;
    private final String materializationRoot;
    private final MaterializedPartitionValidation validation;
    private final boolean keepMaterialized;
    private final String partitionAssignmentStrategy;
    private final String anchorSide;
    private final long[] partitionRefinementLoadBytes;

    private MaterializedPartitionExecution(JavaSparkContext sparkContext,
                                           Summary leftSummary,
                                           Summary rightSummary,
                                           long leftFeatureCount,
                                           long rightFeatureCount,
                                           SpatialPartitioner.EffectivePlan effectivePlan,
                                           String materializationRoot,
                                           MaterializedPartitionValidation validation,
                                           boolean keepMaterialized,
                                           String partitionAssignmentStrategy,
                                           String anchorSide) {
      this(
          sparkContext,
          leftSummary,
          rightSummary,
          leftFeatureCount,
          rightFeatureCount,
          effectivePlan,
          materializationRoot,
          validation,
          keepMaterialized,
          partitionAssignmentStrategy,
          anchorSide,
          null);
    }

    private MaterializedPartitionExecution(JavaSparkContext sparkContext,
                                           Summary leftSummary,
                                           Summary rightSummary,
                                           long leftFeatureCount,
                                           long rightFeatureCount,
                                           SpatialPartitioner.EffectivePlan effectivePlan,
                                           String materializationRoot,
                                           MaterializedPartitionValidation validation,
                                           boolean keepMaterialized,
                                           String partitionAssignmentStrategy,
                                           String anchorSide,
                                           long[] partitionRefinementLoadBytes) {
      this.sparkContext = sparkContext;
      this.leftSummary = leftSummary == null ? new Summary() : leftSummary;
      this.rightSummary = rightSummary == null ? new Summary() : rightSummary;
      this.leftFeatureCount = Math.max(0L, leftFeatureCount);
      this.rightFeatureCount = Math.max(0L, rightFeatureCount);
      this.effectivePlan = effectivePlan;
      this.materializationRoot = materializationRoot;
      this.validation = validation;
      this.keepMaterialized = keepMaterialized;
      this.partitionAssignmentStrategy = partitionAssignmentStrategy == null
          ? SpatialJoinConstants.Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY
          : partitionAssignmentStrategy;
      this.anchorSide = anchorSide == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : anchorSide;
      this.partitionRefinementLoadBytes =
          partitionRefinementLoadBytes == null
              ? new long[0]
              : Arrays.copyOf(partitionRefinementLoadBytes, partitionRefinementLoadBytes.length);
    }

    /** Returns the exact left-side summary carried over from partition preparation. */
    public Summary getLeftSummary() {
      return leftSummary;
    }

    /** Returns the exact right-side summary carried over from partition preparation. */
    public Summary getRightSummary() {
      return rightSummary;
    }

    /** Returns the left-side feature count already materialized before the partition data was written to disk. */
    public long getLeftFeatureCount() {
      return leftFeatureCount;
    }

    /** Returns the right-side feature count already materialized before the partition data was written to disk. */
    public long getRightFeatureCount() {
      return rightFeatureCount;
    }

    /** Returns the final effective partition plan whose ids match the materialized partition files. */
    public SpatialPartitioner.EffectivePlan getEffectivePlan() {
      return effectivePlan;
    }

    /** Returns materialized-geometry loads aligned with the final dense effective partition ids. */
    public long[] getPartitionRefinementLoadBytes() {
      return Arrays.copyOf(partitionRefinementLoadBytes, partitionRefinementLoadBytes.length);
    }

    /** Returns the external-storage root directory containing metadata and left/right partition files. */
    public String getMaterializationRoot() {
      return materializationRoot;
    }

    /** Returns the optional materialization validation summary collected during preparation. */
    public MaterializedPartitionValidation getValidation() {
      return validation;
    }

    /** Returns whether the materialized partition files should be kept after execution finishes. */
    public boolean isKeepMaterialized() {
      return keepMaterialized;
    }

    /** Returns which assignment strategy produced the materialized effective plan. */
    public String getPartitionAssignmentStrategy() {
      return partitionAssignmentStrategy;
    }

    /** Returns the chosen anchor side, or {@code none} when the strategy is symmetric. */
    public String getAnchorSide() {
      return anchorSide;
    }

    @Override
    public void close() throws IOException {
      if (keepMaterialized || materializationRoot == null || materializationRoot.trim().isEmpty() || sparkContext == null)
        return;
      Path rootPath = new Path(materializationRoot);
      FileSystem fileSystem = rootPath.getFileSystem(sparkContext.hadoopConfiguration());
      if (fileSystem.exists(rootPath))
        fileSystem.delete(rootPath, true);
    }
  }

  /**
   * Builds a partition-level execution plan from partition load arrays.
   *
   * <p>This method is a framework hook only. It does not execute any Spark job
   * yet; it just estimates partition intensities and groups partitions into the
   * future job queue structure described in the detailed design.
   *
   * @param partitioner active spatial partitioner
   * @param leftFeatureCounts left-side feature counts per partition
   * @param rightFeatureCounts right-side feature counts per partition
   * @param leftBytes left-side bytes per partition
   * @param rightBytes right-side bytes per partition
   * @return partition execution plan
   */
  public JobQueueExecutionPlan planPartitionJobs(SpatialPartitioner partitioner,
                                                 long[] leftFeatureCounts,
                                                 long[] rightFeatureCounts,
                                                 long[] leftBytes,
                                                 long[] rightBytes) {
    List<PartProfile> inputs = PartProfile.fromPartitioner(
        partitioner,
        leftFeatureCounts,
        rightFeatureCounts,
        leftBytes,
        rightBytes);
    return createJobPlanner().buildJobPlan(inputs);
  }

  /**
   * Builds the effective partition set for later partition-aware join execution.
   *
   * <p>The partitioner itself is constructed from the union of left/right
   * lightweight descriptors, while the final retained partitions are pruned down
   * to those that actually receive features from both sides. After this method
   * returns, the temporary {@code AbstractFeature} arrays are no longer retained;
   * the returned plan keeps only the final partitioner and compact OID-to-partition
   * assignments used to reload binary features later.
   *
   * @param sparkContext active Spark context
   * @return effective partitioning plan
   */
  @Deprecated
  public SpatialPartitioner.EffectivePlan buildEffectivePartitioningPlan(JavaSparkContext sparkContext) {
    JavaRDD<IFeature> leftFeatures = readLeft(sparkContext);
    JavaRDD<IFeature> rightFeatures = readRight(sparkContext);
    Summary leftSummary = SummaryHelper.computeFor(leftFeatures, getPartitionLoadEstimateMode());
    Summary rightSummary = SummaryHelper.computeFor(rightFeatures, getPartitionLoadEstimateMode());
    String complexityMode = getComplexityMode();
    String loadEstimateMode = getPartitionLoadEstimateMode();
    AbstractFeature[] leftAbstractFeatures =
        SpatialPartitioner.collectAbstractFeatures(leftFeatures, complexityMode, loadEstimateMode);
    AbstractFeature[] rightAbstractFeatures =
        SpatialPartitioner.collectAbstractFeatures(rightFeatures, complexityMode, loadEstimateMode);
    Summary combinedSummary = new Summary(leftSummary);
    combinedSummary.expandToSummary(rightSummary);

    PartitionOperator partitionOperator = new PartitionOperator(buildLAHilbertPartitionOptions(leftSummary, rightSummary));
    SpatialPartitioner.EffectivePlan basePlan =
        partitionOperator.constructEffective(combinedSummary, leftAbstractFeatures, rightAbstractFeatures, null);
    return applyPartitionAssignmentStrategy(basePlan, leftAbstractFeatures, rightAbstractFeatures).getEffectivePlan();
  }

  /**
   * Applies the configured post-construction assignment strategy to a baseline dual-replication plan.
   *
   * <p>The surrounding partitioner still controls the resource-aware split decisions and final
   * partition geometry. This helper only rewrites the final per-partition left/right assignments
   * and the derived execution profiles.
   */
  private PartitionAssignmentStrategyHelper.StrategyPlanResult applyPartitionAssignmentStrategy(
      SpatialPartitioner.EffectivePlan basePlan,
      AbstractFeature[] leftAbstractFeatures,
      AbstractFeature[] rightAbstractFeatures) {
    return PartitionAssignmentStrategyHelper.applyStrategy(
        basePlan,
        leftAbstractFeatures,
        rightAbstractFeatures,
        getPartitionAssignmentStrategy());
  }

  /**
   * Builds the driver-side summaries and final effective partition plan shared by both memory mode
   * and disk-hybrid mode.
   *
   * <p>The current partition construction chain still relies on one collected
   * {@link AbstractFeature} array per side. This keeps the logical partitioning behavior identical
   * across the two working-set modes while allowing the disk-hybrid path to avoid building the
   * final in-memory `(partitionId, feature)` working set.
   */
  private PartitionPreparationContext buildPartitionPreparationContext(JavaPairRDD<Long, IFeature> leftFeaturesByOid,
                                                                      JavaPairRDD<Long, IFeature> rightFeaturesByOid) {
    String complexityMode = getComplexityMode();
    String loadEstimateMode = getPartitionLoadEstimateMode();
    SpatialPartitioner.CollectedAbstractFeatures leftCollected =
        SpatialPartitioner.collectAbstractFeaturesWithSummary(leftFeaturesByOid, complexityMode, loadEstimateMode);
    SpatialPartitioner.CollectedAbstractFeatures rightCollected =
        SpatialPartitioner.collectAbstractFeaturesWithSummary(rightFeaturesByOid, complexityMode, loadEstimateMode);
    return buildPartitionPreparationContext(leftCollected, rightCollected);
  }

  private PartitionPreparationContext buildPartitionPreparationContext(JavaRDD<IFeature> leftFeatures,
                                                                      JavaRDD<IFeature> rightFeatures) {
    String complexityMode = getComplexityMode();
    String loadEstimateMode = getPartitionLoadEstimateMode();
    SpatialPartitioner.CollectedAbstractFeatures leftCollected =
        SpatialPartitioner.collectAbstractFeaturesWithSummary(leftFeatures, complexityMode, loadEstimateMode);
    SpatialPartitioner.CollectedAbstractFeatures rightCollected =
        SpatialPartitioner.collectAbstractFeaturesWithSummary(rightFeatures, complexityMode, loadEstimateMode);
    return buildPartitionPreparationContext(leftCollected, rightCollected);
  }

  private PartitionPreparationContext buildPartitionPreparationContext(
      SpatialPartitioner.CollectedAbstractFeatures leftCollected,
      SpatialPartitioner.CollectedAbstractFeatures rightCollected) {
    long leftFeatureCount = leftCollected == null ? 0L : leftCollected.getFeatureCount();
    long rightFeatureCount = rightCollected == null ? 0L : rightCollected.getFeatureCount();
    Summary leftSummary = leftCollected == null ? new Summary() : leftCollected.getSummary();
    Summary rightSummary = rightCollected == null ? new Summary() : rightCollected.getSummary();
    AbstractFeature[] leftAbstractFeatures = leftCollected == null ? new AbstractFeature[0] : leftCollected.getAbstractFeatures();
    AbstractFeature[] rightAbstractFeatures = rightCollected == null ? new AbstractFeature[0] : rightCollected.getAbstractFeatures();
    Summary combinedSummary = new Summary(leftSummary);
    combinedSummary.expandToSummary(rightSummary);

    PartitionOperator partitionOperator = new PartitionOperator(buildLAHilbertPartitionOptions(leftSummary, rightSummary));
    SpatialPartitioner.EffectivePlan baseEffectivePlan =
        partitionOperator.constructEffective(combinedSummary, leftAbstractFeatures, rightAbstractFeatures, null);
    PartitionAssignmentStrategyHelper.StrategyPlanResult strategyPlan =
        applyPartitionAssignmentStrategy(baseEffectivePlan, leftAbstractFeatures, rightAbstractFeatures);
    return new PartitionPreparationContext(
        leftSummary,
        rightSummary,
        leftFeatureCount,
        rightFeatureCount,
        strategyPlan.getEffectivePlan(),
        strategyPlan.getStrategy(),
        strategyPlan.getAnchorSide());
  }

  /**
   * Builds the disk-hybrid partition-preparation context through a synopsis-first exact pipeline.
   *
   * <p>The path is intentionally specialized for the production dual-replication strategy:
   * <ol>
   *   <li>compute exact left/right summaries from binary metadata,</li>
   *   <li>collect one synopsis descriptor per occupied coarse grid cell,</li>
   *   <li>construct a candidate partitioner from those synopsis descriptors,</li>
   *   <li>iterate distributed exact partition statistics on disk-backed per-object descriptors
   *       until the partition layout converges, and</li>
   *   <li>derive the final effective plan from those exact statistics.</li>
   * </ol>
   *
   * <p>The resulting effective plan intentionally carries no explicit assignment lists. The later
   * disk-hybrid materialization path already expands raw features directly from the final
   * partitioner geometry in dual-replication mode.
   */
  private SynopsisCandidatePreparationContext buildSynopsisCandidatePreparationContextForDiskHybrid(
      JavaRDD<IFeature> leftFeatures,
      JavaRDD<IFeature> rightFeatures) {
    long stageStartNanos = System.nanoTime();
    String complexityMode = getComplexityMode();
    String loadEstimateMode = getPartitionRefinementLoadEstimateMode();
    String refinementStrategy = options.getString(
        SpatialJoinConstants.LAHilbert.REFINE_STRATEGY,
        SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_STRATEGY);
    boolean trackRepresentativeMoments =
        SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_OBJECT_CENTER_OFFSET_AXIS_LOAD.equals(
            refinementStrategy);
    JavaRDD<AbstractFeature> leftDescriptors = prepareDiskBackedRefinementFeatures(
        leftFeatures,
        complexityMode,
        loadEstimateMode,
        trackRepresentativeMoments);
    JavaRDD<AbstractFeature> rightDescriptors = prepareDiskBackedRefinementFeatures(
        rightFeatures,
        complexityMode,
        loadEstimateMode,
        trackRepresentativeMoments);
    try {
      long leftSummaryStartNanos = System.nanoTime();
      Summary leftSummary = SummaryHelper.computeForAbstractFeatures(leftDescriptors);
      long leftSummaryElapsedMillis = elapsedMillis(leftSummaryStartNanos);
      long rightSummaryStartNanos = System.nanoTime();
      Summary rightSummary = SummaryHelper.computeForAbstractFeatures(rightDescriptors);
      long rightSummaryElapsedMillis = elapsedMillis(rightSummaryStartNanos);
      long leftFeatureCount = leftSummary == null ? 0L : Math.max(0L, leftSummary.numFeatures());
      long rightFeatureCount = rightSummary == null ? 0L : Math.max(0L, rightSummary.numFeatures());
      Summary combinedSummary = new Summary(leftSummary);
      combinedSummary.expandToSummary(rightSummary);
      EnvelopeNDLite synopsisReferenceEnvelope = new EnvelopeNDLite(combinedSummary);
      int synopsisOrder = deriveDiskHybridSynopsisOrder(leftSummary, rightSummary);
      long leftSynopsisStartNanos = System.nanoTime();
      AbstractFeature[] leftSynopsis = SpatialPartitioner.collectSynopsisAbstractFeatures(
          leftDescriptors,
          synopsisReferenceEnvelope,
          synopsisOrder,
          trackRepresentativeMoments);
      long leftSynopsisElapsedMillis = elapsedMillis(leftSynopsisStartNanos);
      long rightSynopsisStartNanos = System.nanoTime();
      AbstractFeature[] rightSynopsis = SpatialPartitioner.collectSynopsisAbstractFeatures(
          rightDescriptors,
          synopsisReferenceEnvelope,
          synopsisOrder,
          trackRepresentativeMoments);
      long rightSynopsisElapsedMillis = elapsedMillis(rightSynopsisStartNanos);
      int leftSynopsisCellCount = leftSynopsis == null ? 0 : leftSynopsis.length;
      int rightSynopsisCellCount = rightSynopsis == null ? 0 : rightSynopsis.length;
      PartitionOperator partitionOperator =
          new PartitionOperator(buildLAHilbertPartitionOptions(leftSummary, rightSummary));
      long candidateConstructStartNanos = System.nanoTime();
      boolean aggregateMbrRefinementPruningEnabled = options.getBoolean(
          SpatialJoinConstants.LAHilbert.REFINE_AGGREGATE_MBR_PRUNING_ENABLED,
          SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_AGGREGATE_MBR_PRUNING_ENABLED);
      boolean effectiveRefinementPruningEnabled = options.getBoolean(
          SpatialJoinConstants.LAHilbert.REFINE_EFFECTIVE_PRUNING_ENABLED,
          SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_EFFECTIVE_PRUNING_ENABLED);
      SpatialPartitioner candidatePartitioner;
      if (aggregateMbrRefinementPruningEnabled && effectiveRefinementPruningEnabled) {
        candidatePartitioner = partitionOperator.constructWithSideAwareRefinement(
            combinedSummary,
            leftSynopsis,
            rightSynopsis,
            null);
      } else {
        candidatePartitioner = partitionOperator.construct(
            combinedSummary,
            concatAbstractFeatures(leftSynopsis, rightSynopsis),
            null);
      }
      long candidateConstructElapsedMillis = elapsedMillis(candidateConstructStartNanos);
      long exactRefinementStartNanos = System.nanoTime();
      int exactRefinementRounds = refineSynopsisCandidatePartitionsFromDescriptors(
          candidatePartitioner,
          leftDescriptors,
          rightDescriptors);
      long exactRefinementElapsedMillis = elapsedMillis(exactRefinementStartNanos);
      lastPartitionConstructElapsedNanos =
          Math.max(0L, System.nanoTime() - candidateConstructStartNanos);
      leftSynopsis = null;
      rightSynopsis = null;
      combinedSummary = null;
      synopsisReferenceEnvelope = null;
      int candidatePartitionCount = candidatePartitioner == null ? 0 : candidatePartitioner.numPartitions();
      System.out.println(String.format(
          "[Prepare detail] disk-hybrid synopsis candidate prepare: leftSummary=%d ms, rightSummary=%d ms, " +
              "leftSynopsis=%d ms (%d cells / %d features), rightSynopsis=%d ms (%d cells / %d features), candidateConstruct=%d ms, " +
              "exactRefinement=%d ms (lightweightPasses=%d, candidatePartitions=%d), descriptorCache=DISK_ONLY, total=%d ms",
          leftSummaryElapsedMillis,
          rightSummaryElapsedMillis,
          leftSynopsisElapsedMillis,
          leftSynopsisCellCount,
          leftFeatureCount,
          rightSynopsisElapsedMillis,
          rightSynopsisCellCount,
          rightFeatureCount,
          candidateConstructElapsedMillis,
          exactRefinementElapsedMillis,
          exactRefinementRounds,
          candidatePartitionCount,
          elapsedMillis(stageStartNanos)));
      return new SynopsisCandidatePreparationContext(
          leftSummary,
          rightSummary,
          leftFeatureCount,
          rightFeatureCount,
          candidatePartitioner,
          SpatialJoinConstants.Execution.PARTITION_ASSIGNMENT_STRATEGY_DUAL_REPLICATION,
          PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE);
    } finally {
      unpersistQuietly(leftDescriptors);
      unpersistQuietly(rightDescriptors);
    }
  }

  private JavaRDD<AbstractFeature> prepareDiskBackedRefinementFeatures(
      JavaRDD<IFeature> features,
      String complexityMode,
      String loadEstimateMode,
      boolean trackRepresentativeMoments) {
    return features
        .mapPartitions(iterator -> new RefinementDescriptorIterator(
            iterator,
            complexityMode,
            loadEstimateMode,
            trackRepresentativeMoments))
        .persist(DISK_HYBRID_PREPARED_FEATURE_STORAGE_LEVEL);
  }

  private int refineSynopsisCandidatePartitionsFromDescriptors(
      SpatialPartitioner candidatePartitioner,
      JavaRDD<AbstractFeature> leftFeatures,
      JavaRDD<AbstractFeature> rightFeatures) {
    if (!(candidatePartitioner instanceof LAHilbertPartitioner)
        || leftFeatures == null
        || rightFeatures == null)
      return 0;
    LAHilbertPartitioner lahilbert = (LAHilbertPartitioner) candidatePartitioner;
    lastLightweightRefinementFullInputScanCount = 0;
    lastLightweightRefinementFrontierRemapCount = 0;
    RefinementFrontier frontier = null;
    try {
      LAHilbertPartitioner.PartitionInfo[] currentLayout = lahilbert.getPartitionInfos();
      frontier = RefinementFrontier.initialize(
          leftFeatures,
          rightFeatures,
          lahilbert,
          currentLayout,
          DISK_HYBRID_EXACT_REFINEMENT_HISTOGRAM_BINS);
      // The initial generation is fully materialized by its collected statistics. Later rounds
      // depend only on that generation, so release the source descriptor cache immediately.
      unpersistQuietly(leftFeatures);
      unpersistQuietly(rightFeatures);
      lastLightweightRefinementFullInputScanCount = 1;
      Set<String> observedGenerationCodes = new HashSet<>();
      for (LAHilbertPartitioner.PartitionInfo info : currentLayout) {
        if (info != null)
          observedGenerationCodes.add(info.getCode());
      }
      boolean initialGeneration = true;

      for (int pass = 1; pass <= DISK_HYBRID_EXACT_REFINEMENT_MAX_ROUNDS; pass++) {
        int partitionsBefore = currentLayout.length;
        SpatialPartitioner.PartitionRefinementStats leftStats =
            frontier.toStats(currentLayout, FRONTIER_SIDE_LEFT);
        SpatialPartitioner.PartitionRefinementStats rightStats =
            frontier.toStats(currentLayout, FRONTIER_SIDE_RIGHT);
        boolean changed = lahilbert.refineFromExactPartitionStats(
            leftStats,
            rightStats,
            observedGenerationCodes);
        LAHilbertPartitioner.ExactRefinementDiagnostics diagnostics =
            lahilbert.getLastExactRefinementDiagnostics();
        LAHilbertPartitioner.PartitionInfo[] nextLayout = lahilbert.getPartitionInfos();
        RefinementLayoutDelta delta = RefinementLayoutDelta.between(currentLayout, nextLayout);
        System.out.println(String.format(
            "[Prepare detail] lightweight exact refinement: pass=%d, source=%s, " +
                "partitionsBefore=%d, partitionsAfter=%d, splitParents=%d, prunedParents=%d, changed=%s",
            pass,
            pass == 1 ? "full-input" : "frontier-cache",
            partitionsBefore,
            nextLayout.length,
            delta.splitParents.size(),
            delta.prunedParentCount,
            changed));
        System.out.println(String.format(
            "[Prepare detail] lightweight exact refinement diagnostics: pass=%d, " +
                "candidatePartitions=%d, emptyPartitions=%d, nonEmptyPartitions=%d, " +
                "leftOnlyPartitions=%d, rightOnlyPartitions=%d, bothSidePartitions=%d, " +
                "bothSideButAggregateMbrDisjointPartitions=%d, effectiveBeforeRefinementPartitions=%d, " +
                "effectivePrunedPartitions=%d, splitParentPartitions=%d, outputPartitions=%d, " +
                "nonEmptyPartitionLoadBytes=%d, leftOnlyPartitionLoadBytes=%d, " +
                "rightOnlyPartitionLoadBytes=%d, bothSideButAggregateMbrDisjointPartitionLoadBytes=%d, " +
                "effectiveBeforeRefinementPartitionLoadBytes=%d, prunablePartitionLoadBytes=%d, " +
                "initialGeneration=%s, observedGenerationPartitions=%d, " +
                "observedGenerationNonEmptyPartitions=%d, observedGenerationLeftOnlyPartitions=%d, " +
                "observedGenerationRightOnlyPartitions=%d, " +
                "observedGenerationBothSideButAggregateMbrDisjointPartitions=%d, " +
                "observedGenerationPrunablePartitions=%d, " +
                "observedGenerationPrunablePartitionLoadBytes=%d, " +
                "effectivePruningEnabled=%s, aggregateMbrPruningEnabled=%s",
            pass,
            diagnostics.getCandidatePartitionCount(),
            diagnostics.getEmptyPartitionCount(),
            diagnostics.getNonEmptyPartitionCount(),
            diagnostics.getLeftOnlyPartitionCount(),
            diagnostics.getRightOnlyPartitionCount(),
            diagnostics.getBothSidePartitionCount(),
            diagnostics.getBothSideAggregateMbrDisjointPartitionCount(),
            diagnostics.getEffectiveBeforeRefinementPartitionCount(),
            diagnostics.getEffectivePrunedPartitionCount(),
            diagnostics.getSplitParentCount(),
            diagnostics.getOutputPartitionCount(),
            diagnostics.getNonEmptyPartitionLoadBytes(),
            diagnostics.getLeftOnlyPartitionLoadBytes(),
            diagnostics.getRightOnlyPartitionLoadBytes(),
            diagnostics.getBothSideAggregateMbrDisjointPartitionLoadBytes(),
            diagnostics.getEffectiveBeforeRefinementPartitionLoadBytes(),
            diagnostics.getPrunablePartitionLoadBytes(),
            initialGeneration,
            diagnostics.getObservedGenerationPartitionCount(),
            diagnostics.getObservedGenerationNonEmptyPartitionCount(),
            diagnostics.getObservedGenerationLeftOnlyPartitionCount(),
            diagnostics.getObservedGenerationRightOnlyPartitionCount(),
            diagnostics.getObservedGenerationBothSideAggregateMbrDisjointPartitionCount(),
            diagnostics.getObservedGenerationPrunablePartitionCount(),
            diagnostics.getObservedGenerationPrunablePartitionLoadBytes(),
            diagnostics.isEffectiveRefinementPruningEnabled(),
            diagnostics.isAggregateMbrRefinementPruningEnabled()));
        if (!changed) {
          lastLightweightRefinementPassCount = pass;
          return pass;
        }
        frontier.apply(delta);
        if (!delta.splitParents.isEmpty())
          lastLightweightRefinementFrontierRemapCount++;
        observedGenerationCodes = delta.splitChildCodes();
        initialGeneration = false;
        currentLayout = nextLayout;
      }
      lastLightweightRefinementPassCount = DISK_HYBRID_EXACT_REFINEMENT_MAX_ROUNDS;
      throw new IllegalStateException(
          "Lightweight exact disk-hybrid refinement did not converge after "
              + DISK_HYBRID_EXACT_REFINEMENT_MAX_ROUNDS
              + " passes; full feature materialization was not started");
    } finally {
      closeQuietly(frontier);
    }
  }

  /**
   * Builds only the lightweight partition-analysis context without preparing the final execution
   * working set. This path is intended for partition-layout and job-planning experiments.
   */
  public PartitionPreparationContext preparePartitionAnalysisContext(JavaSparkContext sparkContext) {
    JavaRDD<IFeature> leftFeatures = readLeft(sparkContext);
    JavaRDD<IFeature> rightFeatures = readRight(sparkContext);
    return runWithSparkJobGroup(
        sparkContext,
        "sj-stage-partition-analysis",
        "SpatialJoin prepare lightweight partition analysis context",
        () -> buildPartitionPreparationContext(leftFeatures, rightFeatures));
  }

  /**
   * Builds only the lightweight partition-analysis context from already collected abstract-feature
   * summaries. This avoids re-reading the raw datasets when multiple partitioners are compared on
   * the same inputs.
   */
  public PartitionPreparationContext preparePartitionAnalysisContext(
      SpatialPartitioner.CollectedAbstractFeatures leftCollected,
      SpatialPartitioner.CollectedAbstractFeatures rightCollected) {
    return buildPartitionPreparationContext(leftCollected, rightCollected);
  }

  /**
   * Builds a disk-hybrid-style effective partition-load analysis context without writing
   * materialized partition files or executing local joins.
   *
   * <p>This method is intended for throughput-denominator experiments. It follows the same
   * synopsis-driven candidate partition construction and effective-partition pruning used by the
   * production disk-hybrid dual-replication path, but stops after collecting exact per-partition
   * counts/load/complexity. Callers can select the load-estimate mode through
   * {@link SpatialJoinConstants.Execution#PARTITION_LOAD_ESTIMATE_MODE}; for example
   * {@code materialized-geometry} counts the estimated materialized JTS geometry heap plus
   * non-geometry attributes, excluding retained binary geometry bytes.
   *
   * @param sparkContext active Spark context
   * @return effective partition analysis context aligned with the disk-hybrid materialization path
   */
  public PartitionPreparationContext prepareDiskHybridPartitionLoadAnalysisContext(JavaSparkContext sparkContext) {
    String normalizedStrategy = PartitionAssignmentStrategyHelper.normalizeStrategy(getPartitionAssignmentStrategy());
    if (!shouldUseSynopsisDiskHybridPreparation(normalizedStrategy)) {
      return preparePartitionAnalysisContext(sparkContext);
    }

    JavaRDD<IFeature> leftRawFeatures =
        isPrePartitionSplitEnabled() ? readLeft(sparkContext) : prepareDiskHybridStagingFeatures(readLeft(sparkContext));
    JavaRDD<IFeature> rightRawFeatures =
        isPrePartitionSplitEnabled() ? readRight(sparkContext) : prepareDiskHybridStagingFeatures(readRight(sparkContext));
    JavaRDD<IFeature> leftFeatures = preparePrePartitionSplitFeaturesIfConfigured(leftRawFeatures);
    JavaRDD<IFeature> rightFeatures = preparePrePartitionSplitFeaturesIfConfigured(rightRawFeatures);
    try {
      SynopsisCandidatePreparationContext candidateContext = runWithSparkJobGroup(
          sparkContext,
          "sj-stage-partition-load-analysis-prepare",
          "SpatialJoin prepare synopsis candidate context for partition-load analysis",
          () -> buildSynopsisCandidatePreparationContextForDiskHybrid(leftFeatures, rightFeatures));
      return runWithSparkJobGroup(
          sparkContext,
          "sj-stage-partition-load-analysis-stats",
          "SpatialJoin collect effective materialized-geometry partition-load statistics",
          () -> buildPartitionLoadAnalysisContextFromStats(candidateContext, leftFeatures, rightFeatures));
    } finally {
      unpersistQuietly(leftFeatures);
      unpersistQuietly(rightFeatures);
      if (leftFeatures != leftRawFeatures && !isPrePartitionSplitEnabled())
        unpersistQuietly(leftRawFeatures);
      if (rightFeatures != rightRawFeatures && !isPrePartitionSplitEnabled())
        unpersistQuietly(rightRawFeatures);
    }
  }

  private PartitionPreparationContext buildPartitionLoadAnalysisContextFromStats(
      SynopsisCandidatePreparationContext candidateContext,
      JavaRDD<IFeature> leftFeatures,
      JavaRDD<IFeature> rightFeatures) {
    if (candidateContext == null || candidateContext.candidatePartitioner == null) {
      return new PartitionPreparationContext(
          candidateContext == null ? null : candidateContext.leftSummary,
          candidateContext == null ? null : candidateContext.rightSummary,
          candidateContext == null ? 0L : candidateContext.leftFeatureCount,
          candidateContext == null ? 0L : candidateContext.rightFeatureCount,
          null,
          candidateContext == null ? getPartitionAssignmentStrategy() : candidateContext.partitionAssignmentStrategy,
          candidateContext == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : candidateContext.anchorSide);
    }
    SpatialPartitioner.PartitionLoadStats leftStats =
        SpatialPartitioner.collectPartitionLoadStats(
            leftFeatures,
            candidateContext.candidatePartitioner,
            getComplexityMode(),
            getPartitionLoadEstimateMode());
    SpatialPartitioner.PartitionLoadStats rightStats =
        SpatialPartitioner.collectPartitionLoadStats(
            rightFeatures,
            candidateContext.candidatePartitioner,
            getComplexityMode(),
            getPartitionLoadEstimateMode());
    SpatialPartitioner.EffectivePlan effectivePlan =
        candidateContext.candidatePartitioner.buildEffectivePlanFromPartitionStats(leftStats, rightStats);
    effectivePlan = applyPartitionIntensityMode(effectivePlan);
    return new PartitionPreparationContext(
        candidateContext.leftSummary,
        candidateContext.rightSummary,
        candidateContext.leftFeatureCount,
        candidateContext.rightFeatureCount,
        effectivePlan,
        candidateContext.partitionAssignmentStrategy,
        candidateContext.anchorSide);
  }

  /**
   * Materializes the disk-hybrid execution context directly from cached `(oid, feature)` inputs.
   *
   * <p>Unlike the memory-oriented path, this method does not build and hold the final
   * `(partitionId, feature)` working set in serialized memory. It replays assignments once,
   * streams the partitioned records to external files, and releases temporary replay state
   * immediately afterwards.
   */
  private MaterializedPartitionExecution materializePartitionPreparationContext(
      JavaSparkContext sparkContext,
      SynopsisCandidatePreparationContext context,
      JavaRDD<IFeature> leftFeatures,
      JavaRDD<IFeature> rightFeatures) {
    long materializeStartNanos = System.nanoTime();
    lastFullMaterializationPassCount = 0;
    if (context == null || context.candidatePartitioner == null) {
      return new MaterializedPartitionExecution(
          sparkContext,
          context == null ? null : context.leftSummary,
          context == null ? null : context.rightSummary,
          context == null ? 0L : context.leftFeatureCount,
          context == null ? 0L : context.rightFeatureCount,
          null,
          null,
          null,
          isKeepMaterializedPartitions(),
          context == null ? getPartitionAssignmentStrategy() : context.partitionAssignmentStrategy,
          context == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : context.anchorSide);
    }
    String materializationRoot = resolveMaterializationRoot(sparkContext);
    deletePathIfExists(sparkContext, materializationRoot);
    long leftStatsStartNanos = System.nanoTime();
    CandidatePartitionMaterializationStats leftMaterializationStats =
        materializeCandidatePartitionSideAndCollectStats(
            leftFeatures,
            context.candidatePartitioner,
            materializationRoot,
            "left",
            getComplexityMode(),
            getPartitionLoadEstimateMode(),
            getPartitionRefinementLoadEstimateMode());
    long leftStatsElapsedMillis = elapsedMillis(leftStatsStartNanos);
    long rightStatsStartNanos = System.nanoTime();
    CandidatePartitionMaterializationStats rightMaterializationStats =
        materializeCandidatePartitionSideAndCollectStats(
            rightFeatures,
            context.candidatePartitioner,
            materializationRoot,
            "right",
            getComplexityMode(),
            getPartitionLoadEstimateMode(),
            getPartitionRefinementLoadEstimateMode());
    lastFullMaterializationPassCount = 1;
    long rightStatsElapsedMillis = elapsedMillis(rightStatsStartNanos);
    SpatialPartitioner.PartitionLoadStats leftStats = leftMaterializationStats.loadStats;
    SpatialPartitioner.PartitionLoadStats rightStats = rightMaterializationStats.loadStats;

    boolean layoutChanged = false;
    if (context.candidatePartitioner instanceof LAHilbertPartitioner) {
      LAHilbertPartitioner lahilbert = (LAHilbertPartitioner) context.candidatePartitioner;
      layoutChanged = lahilbert.refineFromExactPartitionStats(
          leftMaterializationStats.refinementStats,
          rightMaterializationStats.refinementStats);
      LAHilbertPartitioner.ExactRefinementDiagnostics diagnostics =
          lahilbert.getLastExactRefinementDiagnostics();
      System.out.println(String.format(
          "[Prepare detail] final materialization layout diagnostics: " +
              "candidatePartitions=%d, emptyPartitions=%d, nonEmptyPartitions=%d, " +
              "leftOnlyPartitions=%d, rightOnlyPartitions=%d, bothSidePartitions=%d, " +
              "bothSideButAggregateMbrDisjointPartitions=%d, effectiveBeforeRefinementPartitions=%d, " +
              "effectivePrunedPartitions=%d, splitParentPartitions=%d, outputPartitions=%d, " +
              "nonEmptyPartitionLoadBytes=%d, leftOnlyPartitionLoadBytes=%d, " +
              "rightOnlyPartitionLoadBytes=%d, bothSideButAggregateMbrDisjointPartitionLoadBytes=%d, " +
              "effectiveBeforeRefinementPartitionLoadBytes=%d, prunablePartitionLoadBytes=%d, " +
              "effectivePruningEnabled=%s, aggregateMbrPruningEnabled=%s",
          diagnostics.getCandidatePartitionCount(),
          diagnostics.getEmptyPartitionCount(),
          diagnostics.getNonEmptyPartitionCount(),
          diagnostics.getLeftOnlyPartitionCount(),
          diagnostics.getRightOnlyPartitionCount(),
          diagnostics.getBothSidePartitionCount(),
          diagnostics.getBothSideAggregateMbrDisjointPartitionCount(),
          diagnostics.getEffectiveBeforeRefinementPartitionCount(),
          diagnostics.getEffectivePrunedPartitionCount(),
          diagnostics.getSplitParentCount(),
          diagnostics.getOutputPartitionCount(),
          diagnostics.getNonEmptyPartitionLoadBytes(),
          diagnostics.getLeftOnlyPartitionLoadBytes(),
          diagnostics.getRightOnlyPartitionLoadBytes(),
          diagnostics.getBothSideAggregateMbrDisjointPartitionLoadBytes(),
          diagnostics.getEffectiveBeforeRefinementPartitionLoadBytes(),
          diagnostics.getPrunablePartitionLoadBytes(),
          diagnostics.isEffectiveRefinementPruningEnabled(),
          diagnostics.isAggregateMbrRefinementPruningEnabled()));
    }
    System.out.println(String.format(
        "[Prepare detail] final materialization layout verification: partitions=%d, changed=%s",
        context.candidatePartitioner.numPartitions(),
        layoutChanged));
    if (layoutChanged) {
      deletePathIfExists(
          sparkContext,
          new Path(
              new Path(materializationRoot),
              candidateMaterializationSide("left")).toString());
      deletePathIfExists(
          sparkContext,
          new Path(
              new Path(materializationRoot),
              candidateMaterializationSide("right")).toString());
      throw new IllegalStateException(
          "Partition layout changed during final materialization verification after lightweight "
              + "refinement had converged; refusing to rewrite full feature partition files");
    }
    long effectivePlanStartNanos = System.nanoTime();
    // LAHilbertPartitioner.retainPartitions(...) mutates the candidate partitioner in place,
    // so we must capture the original candidate id space before building the effective plan.
    int candidatePartitionCountBeforeRetain =
        context.candidatePartitioner == null ? 0 : context.candidatePartitioner.numPartitions();
    int[] retainedCandidatePartitionIds = SpatialPartitioner.collectEffectivePartitionIdsFromStats(
        leftStats,
        rightStats,
        candidatePartitionCountBeforeRetain);
    SpatialPartitioner.EffectivePlan effectivePlan =
        context.candidatePartitioner.buildEffectivePlanFromPartitionStats(leftStats, rightStats);
    int effectiveProfileCount =
        effectivePlan == null || effectivePlan.getPartitionProfiles() == null
            ? 0
            : effectivePlan.getPartitionProfiles().size();
    int retainedCandidateCount = retainedCandidatePartitionIds == null ? 0 : retainedCandidatePartitionIds.length;
    if (effectiveProfileCount != retainedCandidateCount) {
      throw new IllegalStateException(
          "Mismatched effective partition counts during disk-hybrid materialization: effectivePlanProfiles="
              + effectiveProfileCount
              + ", retainedCandidatePartitionIds="
              + retainedCandidateCount);
    }
    long[] effectiveRefinementLoadBytes = combineRetainedPartitionLoads(
        leftMaterializationStats.refinementStats,
        rightMaterializationStats.refinementStats,
        retainedCandidatePartitionIds);
    promoteCandidateMaterializedPartitions(
        sparkContext,
        materializationRoot,
        "left",
        retainedCandidatePartitionIds);
    promoteCandidateMaterializedPartitions(
        sparkContext,
        materializationRoot,
        "right",
        retainedCandidatePartitionIds);
    ensurePromotedMaterializedPartitionCount(
        sparkContext,
        materializationRoot,
        "left",
        effectiveProfileCount);
    ensurePromotedMaterializedPartitionCount(
        sparkContext,
        materializationRoot,
        "right",
        effectiveProfileCount);
    effectivePlan = applyPartitionIntensityMode(effectivePlan);
    writeMaterializationMetadata(
        sparkContext,
        new PartitionPreparationContext(
            context.leftSummary,
            context.rightSummary,
            context.leftFeatureCount,
            context.rightFeatureCount,
            effectivePlan,
            context.partitionAssignmentStrategy,
            context.anchorSide),
        materializationRoot);
    long effectivePlanElapsedMillis = elapsedMillis(effectivePlanStartNanos);
    SpatialPartitioner.EffectivePlan strippedEffectivePlan = stripAssignments(effectivePlan);
    long materializationElapsedNanos = Math.max(0L, System.nanoTime() - materializeStartNanos);
    lastPartitionMaterializationElapsedNanos = materializationElapsedNanos;
    System.out.println(String.format(
        "[Prepare detail] disk-hybrid fused stats+materialize: left=%d ms, right=%d ms, " +
            "effectivePlan+promote=%d ms, fullMaterializationPasses=1, total=%d ms",
        leftStatsElapsedMillis,
        rightStatsElapsedMillis,
        effectivePlanElapsedMillis,
        materializationElapsedNanos / 1_000_000L));
    return new MaterializedPartitionExecution(
        sparkContext,
        context.leftSummary,
        context.rightSummary,
        context.leftFeatureCount,
        context.rightFeatureCount,
        strippedEffectivePlan,
        materializationRoot,
        null,
        isKeepMaterializedPartitions(),
        context.partitionAssignmentStrategy,
        context.anchorSide,
        effectiveRefinementLoadBytes);
  }

  private MaterializedPartitionExecution materializePartitionPreparationContext(
      JavaSparkContext sparkContext,
      PartitionPreparationContext context,
      JavaRDD<IFeature> leftFeatures,
      JavaRDD<IFeature> rightFeatures) {
    long materializeStartNanos = System.nanoTime();
    if (context == null || context.getEffectivePlan() == null) {
      return new MaterializedPartitionExecution(
          sparkContext,
          context == null ? null : context.getLeftSummary(),
          context == null ? null : context.getRightSummary(),
          context == null ? 0L : context.getLeftFeatureCount(),
          context == null ? 0L : context.getRightFeatureCount(),
          context == null ? null : context.getEffectivePlan(),
          null,
          null,
          isKeepMaterializedPartitions(),
          context == null ? getPartitionAssignmentStrategy() : context.getPartitionAssignmentStrategy(),
          context == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : context.getAnchorSide());
    }
    if (!canDirectlyMaterializeFromPartitioner(context.getPartitionAssignmentStrategy()))
      throw new IllegalStateException("JavaRDD-based disk-hybrid materialization only supports direct dual-replication");
    SpatialPartitioner.EffectivePlan effectivePlan = context.getEffectivePlan();
    int[] effectivePartitionIds = effectivePlan.getPartitionProfiles().stream()
        .mapToInt(PartProfile::getPartitionId)
        .toArray();
    String materializationRoot = resolveMaterializationRoot(sparkContext);
    long metadataStartNanos = System.nanoTime();
    deletePathIfExists(sparkContext, materializationRoot);
    writeMaterializationMetadata(sparkContext, context, materializationRoot);
    long metadataElapsedMillis = elapsedMillis(metadataStartNanos);
    long leftMaterializeStartNanos = System.nanoTime();
    MaterializedPartitionSideValidation leftValidation = materializeDirectPartitionSide(
        sparkContext,
        leftFeatures,
        effectivePlan.getPartitioner(),
        effectivePartitionIds,
        materializationRoot,
        "left");
    long leftMaterializeElapsedMillis = elapsedMillis(leftMaterializeStartNanos);
    long rightMaterializeStartNanos = System.nanoTime();
    MaterializedPartitionSideValidation rightValidation = materializeDirectPartitionSide(
        sparkContext,
        rightFeatures,
        effectivePlan.getPartitioner(),
        effectivePartitionIds,
        materializationRoot,
        "right");
    long rightMaterializeElapsedMillis = elapsedMillis(rightMaterializeStartNanos);
    MaterializedPartitionValidation validation =
        !isValidateMaterializedPartitions()
            ? null
            : new MaterializedPartitionValidation(leftValidation, rightValidation);
    effectivePlan = applyPartitionIntensityMode(effectivePlan);
    if (usesDerivedPartitionIntensity()) {
      writeMaterializationMetadata(
          sparkContext,
          new PartitionPreparationContext(
              context.getLeftSummary(),
              context.getRightSummary(),
              context.getLeftFeatureCount(),
              context.getRightFeatureCount(),
              effectivePlan,
              context.getPartitionAssignmentStrategy(),
              context.getAnchorSide()),
          materializationRoot);
    }
    SpatialPartitioner.EffectivePlan strippedEffectivePlan = stripAssignments(effectivePlan);
    long materializationElapsedNanos = Math.max(0L, System.nanoTime() - materializeStartNanos);
    lastPartitionMaterializationElapsedNanos = materializationElapsedNanos;
    System.out.println(String.format(
        "[Prepare detail] disk-hybrid materialization: metadata=%d ms, leftMaterialize=%d ms, " +
            "rightMaterialize=%d ms, total=%d ms",
        metadataElapsedMillis,
        leftMaterializeElapsedMillis,
        rightMaterializeElapsedMillis,
        materializationElapsedNanos / 1_000_000L));
    return new MaterializedPartitionExecution(
        sparkContext,
        context.getLeftSummary(),
        context.getRightSummary(),
        context.getLeftFeatureCount(),
        context.getRightFeatureCount(),
        strippedEffectivePlan,
        materializationRoot,
        validation,
        isKeepMaterializedPartitions(),
        context.getPartitionAssignmentStrategy(),
        context.getAnchorSide());
  }

  private MaterializedPartitionExecution materializePartitionPreparationContext(
      JavaSparkContext sparkContext,
      PartitionPreparationContext context,
      JavaPairRDD<Long, IFeature> leftFeaturesByOid,
      JavaPairRDD<Long, IFeature> rightFeaturesByOid) {
    long materializeStartNanos = System.nanoTime();
    if (context == null || context.getEffectivePlan() == null) {
      return new MaterializedPartitionExecution(
          sparkContext,
          context == null ? null : context.getLeftSummary(),
          context == null ? null : context.getRightSummary(),
          context == null ? 0L : context.getLeftFeatureCount(),
          context == null ? 0L : context.getRightFeatureCount(),
          context == null ? null : context.getEffectivePlan(),
          null,
          null,
          isKeepMaterializedPartitions(),
          context == null ? getPartitionAssignmentStrategy() : context.getPartitionAssignmentStrategy(),
          context == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : context.getAnchorSide());
    }
    SpatialPartitioner.EffectivePlan effectivePlan = context.getEffectivePlan();
    int[] effectivePartitionIds = effectivePlan.getPartitionProfiles().stream()
        .mapToInt(PartProfile::getPartitionId)
        .toArray();
    String materializationRoot = resolveMaterializationRoot(sparkContext);
    long metadataStartNanos = System.nanoTime();
    deletePathIfExists(sparkContext, materializationRoot);
    writeMaterializationMetadata(sparkContext, context, materializationRoot);
    long metadataElapsedMillis = elapsedMillis(metadataStartNanos);
    long leftMaterializeStartNanos = System.nanoTime();
    MaterializedPartitionSideValidation leftValidation = materializeAssignedPartitionSide(
        sparkContext,
        leftFeaturesByOid,
        effectivePlan.getLeftAssignments(),
        effectivePartitionIds,
        effectivePlan.getPartitioner(),
        materializationRoot,
        "left",
        context.getPartitionAssignmentStrategy());
    long leftMaterializeElapsedMillis = elapsedMillis(leftMaterializeStartNanos);
    unpersistQuietly(leftFeaturesByOid);
    long rightMaterializeStartNanos = System.nanoTime();
    MaterializedPartitionSideValidation rightValidation = materializeAssignedPartitionSide(
        sparkContext,
        rightFeaturesByOid,
        effectivePlan.getRightAssignments(),
        effectivePartitionIds,
        effectivePlan.getPartitioner(),
        materializationRoot,
        "right",
        context.getPartitionAssignmentStrategy());
    long rightMaterializeElapsedMillis = elapsedMillis(rightMaterializeStartNanos);
    unpersistQuietly(rightFeaturesByOid);
    MaterializedPartitionValidation validation =
        !isValidateMaterializedPartitions()
            ? null
            : new MaterializedPartitionValidation(leftValidation, rightValidation);
    effectivePlan = applyPartitionIntensityMode(effectivePlan);
    if (usesDerivedPartitionIntensity()) {
      writeMaterializationMetadata(
          sparkContext,
          new PartitionPreparationContext(
              context.getLeftSummary(),
              context.getRightSummary(),
              context.getLeftFeatureCount(),
              context.getRightFeatureCount(),
              effectivePlan,
              context.getPartitionAssignmentStrategy(),
              context.getAnchorSide()),
          materializationRoot);
    }
    SpatialPartitioner.EffectivePlan strippedEffectivePlan = stripAssignments(effectivePlan);
    long materializationElapsedNanos = Math.max(0L, System.nanoTime() - materializeStartNanos);
    lastPartitionMaterializationElapsedNanos = materializationElapsedNanos;
    System.out.println(String.format(
        "[Prepare detail] disk-hybrid materialization: metadata=%d ms, leftMaterialize=%d ms, " +
            "rightMaterialize=%d ms, total=%d ms",
        metadataElapsedMillis,
        leftMaterializeElapsedMillis,
        rightMaterializeElapsedMillis,
        materializationElapsedNanos / 1_000_000L));
    return new MaterializedPartitionExecution(
        sparkContext,
        context.getLeftSummary(),
        context.getRightSummary(),
        context.getLeftFeatureCount(),
        context.getRightFeatureCount(),
        strippedEffectivePlan,
        materializationRoot,
        validation,
        isKeepMaterializedPartitions(),
        context.getPartitionAssignmentStrategy(),
        context.getAnchorSide());
  }

  private static long elapsedMillis(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000L;
  }

  private static AbstractFeature[] concatAbstractFeatures(AbstractFeature[] leftFeatures,
                                                          AbstractFeature[] rightFeatures) {
    int leftLength = leftFeatures == null ? 0 : leftFeatures.length;
    int rightLength = rightFeatures == null ? 0 : rightFeatures.length;
    AbstractFeature[] combined = new AbstractFeature[leftLength + rightLength];
    if (leftLength > 0)
      System.arraycopy(leftFeatures, 0, combined, 0, leftLength);
    if (rightLength > 0)
      System.arraycopy(rightFeatures, 0, combined, leftLength, rightLength);
    return combined;
  }

  private static Summary copySummary(Summary summary) {
    return summary == null ? new Summary() : new Summary(summary);
  }

  private MaterializedPartitionSideValidation materializeAssignedPartitionSide(
      JavaSparkContext sparkContext,
      JavaPairRDD<Long, IFeature> featuresByOid,
      List<SpatialPartitioner.FeatureRef> assignments,
      int[] effectivePartitionIds,
      SpatialPartitioner partitioner,
      String materializationRoot,
      String side,
      String partitionAssignmentStrategy) {
    if (featuresByOid == null || partitioner == null)
      return isValidateMaterializedPartitions()
          ? new MaterializedPartitionSideValidation(side, 0L, 0L, true, true, Collections.emptyList())
          : null;
    PreparedPartitionBuildResult assigned;
    if (canDirectlyMaterializeFromPartitioner(partitionAssignmentStrategy)) {
      assigned = buildDirectPartitionFeaturesForMaterialization(
          featuresByOid,
          partitioner,
          isValidateMaterializedPartitions() ? StorageLevel.DISK_ONLY() : null,
          isValidateMaterializedPartitions());
    } else {
      if (assignments == null || assignments.isEmpty()) {
        return isValidateMaterializedPartitions()
            ? new MaterializedPartitionSideValidation(side, 0L, 0L, true, true, Collections.emptyList())
            : null;
      }
      assigned = buildAssignedPartitionFeatures(
          featuresByOid,
          assignments,
          effectivePartitionIds,
          partitioner,
          isValidateMaterializedPartitions() ? StorageLevel.DISK_ONLY() : null,
          isValidateMaterializedPartitions());
    }
    try {
      JavaPairRDD<Integer, IFeature> partitionFeatures = assigned.getPartitionedFeatures();
      materializePartitionSide(partitionFeatures, partitioner, materializationRoot, side, effectivePartitionIds);
      if (!isValidateMaterializedPartitions())
        return null;
      return validateMaterializedPartitionSide(
          sparkContext,
          materializationRoot,
          side,
          effectivePartitionIds,
          partitionFeatures);
    } finally {
      assigned.closeQuietly();
    }
  }

  private MaterializedPartitionSideValidation materializeDirectPartitionSide(
      JavaSparkContext sparkContext,
      JavaRDD<IFeature> features,
      SpatialPartitioner partitioner,
      int[] effectivePartitionIds,
      String materializationRoot,
      String side) {
    if (features == null || partitioner == null) {
      return isValidateMaterializedPartitions()
          ? new MaterializedPartitionSideValidation(side, 0L, 0L, true, true, Collections.emptyList())
          : null;
    }
    PreparedPartitionBuildResult assigned = buildDirectPartitionFeaturesForMaterialization(
        features,
        partitioner,
        isValidateMaterializedPartitions() ? StorageLevel.DISK_ONLY() : null,
        isValidateMaterializedPartitions());
    try {
      JavaPairRDD<Integer, IFeature> partitionFeatures = assigned.getPartitionedFeatures();
      materializePartitionSide(partitionFeatures, partitioner, materializationRoot, side, effectivePartitionIds);
      if (!isValidateMaterializedPartitions())
        return null;
      return validateMaterializedPartitionSide(
          sparkContext,
          materializationRoot,
          side,
          effectivePartitionIds,
          partitionFeatures);
    } finally {
      assigned.closeQuietly();
    }
  }

  /**
   * Prepares the partition-aware execution inputs once and reuses them across all later partition-local jobs.
   *
   * <p>The method materializes one persisted {@code (oid, feature)} RDD per dataset so subsequent
   * reload-by-partition operations do not need to rebuild the source read lineage or recompute
   * {@code zipWithUniqueId()} for every job.
   *
   * @param sparkContext active Spark context
   * @return prepared execution context that should be closed after use
   */
  public PreparedPartitionExecution preparePartitionAwareExecution(JavaSparkContext sparkContext) {
    JavaPairRDD<Long, IFeature> leftFeaturesByOid = prepareFeaturesByOid(readLeft(sparkContext));
    JavaPairRDD<Long, IFeature> rightFeaturesByOid = prepareFeaturesByOid(readRight(sparkContext));
    PreparedPartitionBuildResult leftPreparedResult = null;
    PreparedPartitionBuildResult rightPreparedResult = null;
    try {
      PartitionPreparationContext context = runWithSparkJobGroup(
          sparkContext,
          "sj-stage-partition-prepare",
          "SpatialJoin prepare partition construction context",
          () -> buildPartitionPreparationContext(leftFeaturesByOid, rightFeaturesByOid));
      SpatialPartitioner.EffectivePlan effectivePlan = context.getEffectivePlan();
      final SpatialPartitioner.EffectivePlan baseEffectivePlan = effectivePlan;
      int[] effectivePartitionIds = baseEffectivePlan.getPartitionProfiles().stream()
          .mapToInt(PartProfile::getPartitionId)
          .toArray();
      leftPreparedResult = runWithSparkJobGroup(
          sparkContext,
          "sj-stage-prepare-left-working-set",
          "SpatialJoin build left prepared partition working set",
          () -> buildPreparedPartitionFeatures(
              leftFeaturesByOid,
              baseEffectivePlan.getLeftAssignments(),
              effectivePartitionIds,
              baseEffectivePlan.getPartitioner()));
      rightPreparedResult = runWithSparkJobGroup(
          sparkContext,
          "sj-stage-prepare-right-working-set",
          "SpatialJoin build right prepared partition working set",
          () -> buildPreparedPartitionFeatures(
              rightFeaturesByOid,
              baseEffectivePlan.getRightAssignments(),
              effectivePartitionIds,
              baseEffectivePlan.getPartitioner()));
      effectivePlan = applyPartitionIntensityMode(baseEffectivePlan);
      unpersistQuietly(leftFeaturesByOid);
      unpersistQuietly(rightFeaturesByOid);
      return new PreparedPartitionExecution(
          null,
          null,
          leftPreparedResult.getPartitionedFeatures(),
          rightPreparedResult.getPartitionedFeatures(),
          context.getLeftSummary(),
          context.getRightSummary(),
          context.getLeftFeatureCount(),
          context.getRightFeatureCount(),
          effectivePlan,
          leftPreparedResult.getLookupBroadcast(),
          rightPreparedResult.getLookupBroadcast(),
          context.getPartitionAssignmentStrategy(),
          context.getAnchorSide());
    } catch (RuntimeException e) {
      if (leftPreparedResult != null)
        leftPreparedResult.closeQuietly();
      if (rightPreparedResult != null)
        rightPreparedResult.closeQuietly();
      try {
        leftFeaturesByOid.unpersist(true);
      } catch (Exception ignored) {
      }
      try {
        rightFeaturesByOid.unpersist(true);
      } catch (Exception ignored) {
      }
      throw e;
    }
  }

  /**
   * Prepares the disk-hybrid working-set mode by materializing final partition data to external storage.
   *
   * <p>The method keeps the raw `(oid, feature)` inputs in spillable form, constructs the effective
   * partition plan from lightweight abstract features, and then writes the final partitioned working
   * set directly to external files. Concretely it:
   * <ol>
   *   <li>reads and caches the original {@code (oid, feature)} working sets,</li>
   *   <li>constructs the final effective partitions and their assignments,</li>
   *   <li>materializes partition metadata plus left/right partition data under the configured
   *       materialization root, and</li>
   *   <li>returns a new execution context that points at those partition files instead of the
   *       original cached feature RDDs.</li>
   * </ol>
   *
   * <p>After materialization succeeds, the raw prepared feature caches can be released because later jobs
   * reload only the partition files needed for their target partition ids.
   *
   * @param sparkContext active Spark context
   * @return materialized execution context that should be closed after use
   */
  public MaterializedPartitionExecution prepareMaterializedPartitionExecution(JavaSparkContext sparkContext) {
    lastPreparationIntensityElapsedNanos = 0L;
    lastPartitionConstructElapsedNanos = 0L;
    lastPartitionMaterializationElapsedNanos = 0L;
    lastLightweightRefinementPassCount = 0;
    lastLightweightRefinementFullInputScanCount = 0;
    lastLightweightRefinementFrontierRemapCount = 0;
    lastFullMaterializationPassCount = 0;
    String normalizedStrategy = PartitionAssignmentStrategyHelper.normalizeStrategy(getPartitionAssignmentStrategy());
    if (shouldUseSynopsisDiskHybridPreparation(normalizedStrategy)) {
      JavaRDD<IFeature> leftRawFeatures =
          isPrePartitionSplitEnabled() ? readLeft(sparkContext) : prepareDiskHybridStagingFeatures(readLeft(sparkContext));
      JavaRDD<IFeature> rightRawFeatures =
          isPrePartitionSplitEnabled() ? readRight(sparkContext) : prepareDiskHybridStagingFeatures(readRight(sparkContext));
      JavaRDD<IFeature> leftFeatures = preparePrePartitionSplitFeaturesIfConfigured(leftRawFeatures);
      JavaRDD<IFeature> rightFeatures = preparePrePartitionSplitFeaturesIfConfigured(rightRawFeatures);
      try {
        if (isValidateMaterializedPartitions()) {
          PartitionPreparationContext context = runWithSparkJobGroup(
              sparkContext,
              "sj-stage-partition-prepare",
              "SpatialJoin prepare partition construction context",
              () -> {
                SynopsisCandidatePreparationContext candidateContext =
                    buildSynopsisCandidatePreparationContextForDiskHybrid(leftFeatures, rightFeatures);
                SpatialPartitioner.PartitionLoadStats leftStats =
                    SpatialPartitioner.collectPartitionLoadStats(
                        leftFeatures,
                        candidateContext.candidatePartitioner,
                        getComplexityMode(),
                        getPartitionLoadEstimateMode());
                SpatialPartitioner.PartitionLoadStats rightStats =
                    SpatialPartitioner.collectPartitionLoadStats(
                        rightFeatures,
                        candidateContext.candidatePartitioner,
                        getComplexityMode(),
                        getPartitionLoadEstimateMode());
                SpatialPartitioner.EffectivePlan effectivePlan =
                    candidateContext.candidatePartitioner.buildEffectivePlanFromPartitionStats(leftStats, rightStats);
                return new PartitionPreparationContext(
                    candidateContext.leftSummary,
                    candidateContext.rightSummary,
                    candidateContext.leftFeatureCount,
                    candidateContext.rightFeatureCount,
                    effectivePlan,
                    candidateContext.partitionAssignmentStrategy,
                    candidateContext.anchorSide);
              });
          return runWithSparkJobGroup(
              sparkContext,
              "sj-stage-materialize-working-set",
              "SpatialJoin materialize disk-hybrid partition working set",
              () -> materializePartitionPreparationContext(sparkContext, context, leftFeatures, rightFeatures));
        }
        SynopsisCandidatePreparationContext context = runWithSparkJobGroup(
            sparkContext,
            "sj-stage-partition-prepare",
            "SpatialJoin prepare synopsis candidate partition context",
            () -> buildSynopsisCandidatePreparationContextForDiskHybrid(leftFeatures, rightFeatures));
        return runWithSparkJobGroup(
            sparkContext,
            "sj-stage-materialize-working-set",
            "SpatialJoin materialize disk-hybrid partition working set",
            () -> materializePartitionPreparationContext(sparkContext, context, leftFeatures, rightFeatures));
      } finally {
        unpersistQuietly(leftFeatures);
        unpersistQuietly(rightFeatures);
        if (leftFeatures != leftRawFeatures && !isPrePartitionSplitEnabled())
          unpersistQuietly(leftRawFeatures);
        if (rightFeatures != rightRawFeatures && !isPrePartitionSplitEnabled())
          unpersistQuietly(rightRawFeatures);
      }
    }

    JavaPairRDD<Long, IFeature> leftFeaturesByOid = prepareFeaturesByOid(readLeft(sparkContext));
    JavaPairRDD<Long, IFeature> rightFeaturesByOid = prepareFeaturesByOid(readRight(sparkContext));
    try {
      PartitionPreparationContext context = runWithSparkJobGroup(
          sparkContext,
          "sj-stage-partition-prepare",
          "SpatialJoin prepare partition construction context",
          () -> buildPartitionPreparationContext(leftFeaturesByOid, rightFeaturesByOid));
      return runWithSparkJobGroup(
          sparkContext,
          "sj-stage-materialize-working-set",
          "SpatialJoin materialize disk-hybrid partition working set",
          () -> materializePartitionPreparationContext(sparkContext, context, leftFeaturesByOid, rightFeaturesByOid));
    } finally {
      if (leftFeaturesByOid != null)
        leftFeaturesByOid.unpersist(true);
      if (rightFeaturesByOid != null)
        rightFeaturesByOid.unpersist(true);
    }
  }

  private boolean shouldUseSynopsisDiskHybridPreparation(String normalizedPartitionAssignmentStrategy) {
    return options.getBoolean(
            SpatialJoinConstants.Execution.DISK_SYNOPSIS_PREPARATION_ENABLED,
            SpatialJoinConstants.Execution.DEFAULT_DISK_SYNOPSIS_PREPARATION_ENABLED)
        && SpatialJoinConstants.Execution.PARTITION_ASSIGNMENT_STRATEGY_DUAL_REPLICATION
            .equals(PartitionAssignmentStrategyHelper.normalizeStrategy(normalizedPartitionAssignmentStrategy));
  }

  private int deriveDiskHybridSynopsisOrder(Summary leftSummary, Summary rightSummary) {
    long partitionThresholdBytes = options.getSizeAsBytes(
        SpatialJoinConstants.LAHilbert.PARTITION_BYTES,
        SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES);
    long totalBytes = Math.max(1L,
        (leftSummary == null ? 0L : Math.max(0L, leftSummary.size()))
            + (rightSummary == null ? 0L : Math.max(0L, rightSummary.size())));
    int numDimensions = Math.max(
        1,
        Math.max(
            leftSummary == null ? 0 : leftSummary.getCoordinateDimension(),
            rightSummary == null ? 0 : rightSummary.getCoordinateDimension()));
    if (partitionThresholdBytes <= 0L)
      return Math.max(0, Math.min(SpatialJoinConstants.LAHilbert.MAX_ORDER, DISK_HYBRID_SYNOPSIS_EXTRA_ORDERS));
    double desiredCells = Math.max(1.0d, Math.ceil(totalBytes / (double) partitionThresholdBytes));
    int baseOrder = (int) Math.ceil(
        Math.log(desiredCells) / (Math.log(2.0d) * Math.max(1, numDimensions)));
    return Math.max(
        0,
        Math.min(
            SpatialJoinConstants.LAHilbert.MAX_ORDER,
            baseOrder + DISK_HYBRID_SYNOPSIS_EXTRA_ORDERS));
  }

  /**
   * Reloads only the left-side binary features needed by one planned job.
   *
   * <p>The returned features preserve the project's lazy binary geometry storage
   * until later join code explicitly accesses {@code getGeometry()}.
   *
   * @param sparkContext active Spark context
   * @param plan effective partitioning plan
   * @param partitionIds partitions requested by the current job
   * @return pair RDD of (partitionId, feature)
   */
  @Deprecated
  public JavaPairRDD<Integer, IFeature> loadLeftBinaryFeatures(JavaSparkContext sparkContext,
                                                               SpatialPartitioner.EffectivePlan plan,
                                                               int[] partitionIds) {
    return loadBinaryFeatures(readLeft(sparkContext), plan == null ? null : plan.getLeftAssignments(), partitionIds);
  }

  /**
   * Reloads only the left-side binary features needed by one planned job from a prepared execution context.
   *
   * Using this overload avoids rebuilding the source read lineage after partition construction.
   */
  public JavaPairRDD<Integer, IFeature> loadLeftBinaryFeatures(PreparedPartitionExecution prepared,
                                                               int[] partitionIds) {
    if (prepared == null)
      return loadBinaryFeatures((JavaPairRDD<Long, IFeature>) null, null, partitionIds);
    if (prepared.getLeftPartitionedFeatures() != null)
      return IndexHelper.selectAssignedPartitions(prepared.getLeftPartitionedFeatures(), partitionIds);
    return loadBinaryFeatures(
        prepared.getLeftFeaturesByOid(),
        prepared.getEffectivePlan() == null ? null : prepared.getEffectivePlan().getLeftAssignments(),
        partitionIds);
  }

  /**
   * Reloads only the right-side binary features needed by one planned job.
   *
   * @param sparkContext active Spark context
   * @param plan effective partitioning plan
   * @param partitionIds partitions requested by the current job
   * @return pair RDD of (partitionId, feature)
   */
  @Deprecated
  public JavaPairRDD<Integer, IFeature> loadRightBinaryFeatures(JavaSparkContext sparkContext,
                                                                SpatialPartitioner.EffectivePlan plan,
                                                                int[] partitionIds) {
    return loadBinaryFeatures(readRight(sparkContext), plan == null ? null : plan.getRightAssignments(), partitionIds);
  }

  /**
   * Reloads only the right-side binary features needed by one planned job from a prepared execution context.
   *
   * Using this overload avoids rebuilding the source read lineage after partition construction.
   */
  public JavaPairRDD<Integer, IFeature> loadRightBinaryFeatures(PreparedPartitionExecution prepared,
                                                                int[] partitionIds) {
    if (prepared == null)
      return loadBinaryFeatures((JavaPairRDD<Long, IFeature>) null, null, partitionIds);
    if (prepared.getRightPartitionedFeatures() != null)
      return IndexHelper.selectAssignedPartitions(prepared.getRightPartitionedFeatures(), partitionIds);
    return loadBinaryFeatures(
        prepared.getRightFeaturesByOid(),
        prepared.getEffectivePlan() == null ? null : prepared.getEffectivePlan().getRightAssignments(),
        partitionIds);
  }

  /**
   * Reloads only the left-side binary features needed by one planned job from disk-hybrid partition files.
   */
  public JavaPairRDD<Integer, IFeature> loadLeftBinaryFeatures(JavaSparkContext sparkContext,
                                                               MaterializedPartitionExecution materialized,
                                                               int[] partitionIds) {
    return loadMaterializedBinaryFeatures(
        sparkContext,
        materialized == null ? null : materialized.getMaterializationRoot(),
        "left",
        partitionIds);
  }

  /**
   * Reloads only the right-side binary features needed by one planned job from disk-hybrid partition files.
   */
  public JavaPairRDD<Integer, IFeature> loadRightBinaryFeatures(JavaSparkContext sparkContext,
                                                                MaterializedPartitionExecution materialized,
                                                                int[] partitionIds) {
    return loadMaterializedBinaryFeatures(
        sparkContext,
        materialized == null ? null : materialized.getMaterializationRoot(),
        "right",
        partitionIds);
  }

  /**
   * Builds the partition-aware execution plan from the effective partition set.
   *
   * @param effectivePlan effective partitioning result
   * @return partition job plan
   */
  @Deprecated
  public JobQueueExecutionPlan planEffectivePartitionJobs(SpatialPartitioner.EffectivePlan effectivePlan) {
    if (effectivePlan == null) {
      return createJobPlanner().buildJobPlan(Collections.emptyList());
    }
    return createJobPlanner().buildJobPlan(effectivePlan.getPartitionProfiles());
  }

  /**
   * Builds the partition-aware job plan directly from a prepared execution context.
   *
   * This is the preferred planning entry after partition construction because the prepared
   * context already bundles the final effective partitions and the cached feature inputs
   * that later job execution will reuse by partition id.
   *
   * @param prepared prepared partition-aware execution context
   * @return partition job plan
   */
  public JobQueueExecutionPlan planPreparedPartitionJobs(PreparedPartitionExecution prepared) {
    return planEffectivePartitionJobs(prepared == null ? null : prepared.getEffectivePlan());
  }

  /**
   * Executes the partition-aware spatial join path.
   *
   * <p>This method follows the intended later pipeline:
   * 1. build the effective partitions
   * 2. group effective partitions into future Spark jobs
   * 3. for each job, reload only the binary features mapped to its partitions
   * 4. deserialize geometries only when the join algorithm touches them
   *
   * @param sparkContext active Spark context
   * @return partition-aware execution summary
   */
  public SpatialJoinResult executePartitionAware(JavaSparkContext sparkContext) {
    ensureSupportedJoinConfiguration();
    if (SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID.equalsIgnoreCase(getWorkingSetMode())) {
      return executePartitionAwareDiskHybrid(sparkContext);
    }
    try (PreparedPartitionExecution prepared = preparePartitionAwareExecution(sparkContext)) {
      return executePreparedPartitionAware(prepared);
    } catch (IOException e) {
      throw new RuntimeException("Failed to close prepared partition-aware execution context", e);
    }
  }

  /**
   * Executes the partition-aware join from an already prepared execution context.
   *
   * The effective partition set in the prepared context participates directly in job planning,
   * and each planned job later reloads only the features assigned to its partition ids.
   *
   * @param prepared prepared partition-aware execution context
   * @return partition-aware execution summary
   */
  public SpatialJoinResult executePreparedPartitionAware(PreparedPartitionExecution prepared) {
    if (SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID.equalsIgnoreCase(getWorkingSetMode())) {
      throw new UnsupportedOperationException(
          "Disk-hybrid working-set mode requires executePartitionAware(JavaSparkContext) so partition files can be materialized first");
    }
    if (SpatialJoinConstants.Execution.JOB_EXECUTION_MODE_SEQUENTIAL.equalsIgnoreCase(getJobExecutionMode())) {
      return executePreparedPartitionAwareSequential(prepared);
    }
    return executePreparedPartitionAwareConcurrent(prepared);
  }

  /**
   * Executes the partition-aware join using the default resource-aware concurrent dispatcher.
   *
   * This is the production-oriented execution path that pushes multiple planned jobs according
   * to the configured thread-pool size and concurrency cap.
   *
   * @param prepared prepared partition-aware execution context
   * @return partition-aware execution summary
   */
  public SpatialJoinResult executePreparedPartitionAwareConcurrent(PreparedPartitionExecution prepared) {
    if (prepared == null) {
      return SpatialJoinResult.fromPlan(
          getLeftInputPath(),
          getRightInputPath(),
          getLeftInputFormat(),
          getRightInputFormat(),
          getJoinPredicate(),
          "partition-aware-" + getJoinAlgorithm(),
          0L,
          0L,
          0L,
          null,
          Collections.emptyList());
    }
    JobQueueExecutionPlan executionPlan = planPreparedPartitionJobs(prepared);
    if (isGlobalObjectPairDedupEnabled()) {
      List<JobExecutionResult> jobResults = executeObjectPairFilePlan(prepared, executionPlan);
      long uniqueMatches = countUniqueObjectPairsFromJobResults(sparkContextFromPrepared(prepared), jobResults);
      return buildPartitionAwareResult(prepared, executionPlan, jobResults,
          "partition-aware-" + getJoinAlgorithm() + "-object-global", uniqueMatches);
    }
    List<JobExecutionResult> jobResults = executePartitionPlan(prepared, executionPlan);
    return buildPartitionAwareResult(prepared, executionPlan, jobResults);
  }

  /**
   * Executes the partition-aware join by running planned jobs strictly one by one.
   *
   * This mode is intended for deterministic full-pipeline verification and debugging:
   * the same partitions and job plan are used, but no dispatcher-level concurrency is introduced.
   *
   * @param prepared prepared partition-aware execution context
   * @return partition-aware execution summary
   */
  public SpatialJoinResult executePreparedPartitionAwareSequential(PreparedPartitionExecution prepared) {
    if (prepared == null) {
      return SpatialJoinResult.fromPlan(
          getLeftInputPath(),
          getRightInputPath(),
          getLeftInputFormat(),
          getRightInputFormat(),
          getJoinPredicate(),
          "partition-aware-" + getJoinAlgorithm() + "-sequential",
          0L,
          0L,
          0L,
          null,
          Collections.emptyList());
    }
    JobQueueExecutionPlan executionPlan = planPreparedPartitionJobs(prepared);
    if (isGlobalObjectPairDedupEnabled()) {
      List<JobExecutionResult> jobResults = executeObjectPairFilePlanSequential(prepared, executionPlan);
      long uniqueMatches = countUniqueObjectPairsFromJobResults(sparkContextFromPrepared(prepared), jobResults);
      return buildPartitionAwareResult(prepared, executionPlan, jobResults,
          "partition-aware-" + getJoinAlgorithm() + "-sequential-object-global", uniqueMatches);
    }
    List<JobExecutionResult> jobResults = executePartitionPlanSequential(prepared, executionPlan);
    return buildPartitionAwareResult(prepared, executionPlan, jobResults, "partition-aware-" + getJoinAlgorithm() + "-sequential");
  }

  /**
   * Convenience entry that prepares the execution context and then runs the plan sequentially.
   *
   * @param sparkContext active Spark context
   * @return sequential partition-aware execution summary
   */
  public SpatialJoinResult executePartitionAwareSequential(JavaSparkContext sparkContext) {
    ensureSupportedJoinConfiguration();
    if (SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID.equalsIgnoreCase(getWorkingSetMode())) {
      try (MaterializedPartitionExecution materialized = prepareMaterializedPartitionExecution(sparkContext)) {
        return executeMaterializedPartitionAwareSequential(sparkContext, materialized);
      } catch (IOException e) {
        throw new RuntimeException("Failed to close prepared sequential disk-hybrid execution context", e);
      }
    }
    try (PreparedPartitionExecution prepared = preparePartitionAwareExecution(sparkContext)) {
      return executePreparedPartitionAwareSequential(prepared);
    } catch (IOException e) {
      throw new RuntimeException("Failed to close prepared sequential partition-aware execution context", e);
    }
  }

  /**
   * Convenience entry that prepares the execution context and then runs the plan through the dispatcher.
   *
   * @param sparkContext active Spark context
   * @return concurrent partition-aware execution summary
   */
  public SpatialJoinResult executePartitionAwareConcurrent(JavaSparkContext sparkContext) {
    ensureSupportedJoinConfiguration();
    if (SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID.equalsIgnoreCase(getWorkingSetMode())) {
      try (MaterializedPartitionExecution materialized = prepareMaterializedPartitionExecution(sparkContext)) {
        return executeMaterializedPartitionAwareConcurrent(sparkContext, materialized);
      } catch (IOException e) {
        throw new RuntimeException("Failed to close prepared concurrent disk-hybrid execution context", e);
      }
    }
    try (PreparedPartitionExecution prepared = preparePartitionAwareExecution(sparkContext)) {
      return executePreparedPartitionAwareConcurrent(prepared);
    } catch (IOException e) {
      throw new RuntimeException("Failed to close prepared concurrent partition-aware execution context", e);
    }
  }

  /**
   * Executes the partition-aware join in disk-hybrid working-set mode.
   *
   * <p>The final effective partition data is first materialized to external storage and then jobs reload only
   * the left/right partition files matching their {@code partitionIds}. This mode is intended for scenarios where
   * the prepared raw feature working set should be released before job execution starts.
   */
  public SpatialJoinResult executePartitionAwareDiskHybrid(JavaSparkContext sparkContext) {
    ensureSupportedJoinConfiguration();
    try (MaterializedPartitionExecution materialized = prepareMaterializedPartitionExecution(sparkContext)) {
      return executeMaterializedPartitionAware(sparkContext, materialized);
    } catch (IOException e) {
      throw new RuntimeException("Failed to close disk-hybrid partition-aware execution context", e);
    }
  }

  /**
   * Executes the partition-aware join from an already materialized disk-hybrid working set.
   *
   * <p>This method is the dispatch switch for disk-backed execution. It does not rebuild partitions or
   * rewrite files; it only chooses whether the already materialized jobs will be executed sequentially
   * or through the concurrent dispatcher.
   *
   * @param sparkContext active Spark context used later to reload partition files.
   * @param materialized disk-backed working set produced by {@link #prepareMaterializedPartitionExecution(JavaSparkContext)}.
   * @return partition-aware execution summary over the materialized working set.
   */
  public SpatialJoinResult executeMaterializedPartitionAware(JavaSparkContext sparkContext,
                                                            MaterializedPartitionExecution materialized) {
    if (SpatialJoinConstants.Execution.JOB_EXECUTION_MODE_SEQUENTIAL.equalsIgnoreCase(getJobExecutionMode())) {
      return executeMaterializedPartitionAwareSequential(sparkContext, materialized);
    }
    return executeMaterializedPartitionAwareConcurrent(sparkContext, materialized);
  }

  /**
   * Executes the disk-hybrid plan through the configured concurrent dispatcher.
   *
   * <p>The method rebuilds the job plan from the materialized partition profiles, submits those jobs to
   * the same resource-aware dispatcher used by the in-memory path, and finally aggregates one
   * {@link JobExecutionResult} per job into one {@link SpatialJoinResult}.
   *
   * @param sparkContext active Spark context used to reload partition files on demand.
   * @param materialized disk-backed working set whose partition files will be consumed by jobs.
   * @return aggregated execution summary for the concurrent disk-hybrid run.
   */
  public SpatialJoinResult executeMaterializedPartitionAwareConcurrent(JavaSparkContext sparkContext,
                                                                      MaterializedPartitionExecution materialized) {
    if (materialized == null) {
      return SpatialJoinResult.fromPlan(
          getLeftInputPath(),
          getRightInputPath(),
          getLeftInputFormat(),
          getRightInputFormat(),
          getJoinPredicate(),
          "partition-aware-" + getJoinAlgorithm() + "-disk-hybrid",
          0L,
          0L,
          0L,
          null,
          Collections.emptyList());
    }
    JobQueueExecutionPlan executionPlan = createJobPlanner().buildJobPlan(
        materialized.getEffectivePlan() == null ? Collections.emptyList() : materialized.getEffectivePlan().getPartitionProfiles());
    if (isGlobalObjectPairDedupEnabled()) {
      List<JobExecutionResult> jobResults = executeObjectPairFilePlan(sparkContext, materialized, executionPlan);
      long uniqueMatches = countUniqueObjectPairsFromJobResults(sparkContext, jobResults);
      return buildPartitionAwareResult(materialized, executionPlan, jobResults,
          "partition-aware-" + getJoinAlgorithm() + "-disk-hybrid-object-global", uniqueMatches);
    }
    List<JobExecutionResult> jobResults = executePartitionPlan(sparkContext, materialized, executionPlan);
    return buildPartitionAwareResult(materialized, executionPlan, jobResults,
        "partition-aware-" + getJoinAlgorithm() + "-disk-hybrid");
  }

  /**
   * Executes the disk-hybrid plan one job at a time for deterministic verification.
   *
   * <p>This follows the same plan and the same partition files as the concurrent path, but removes dispatcher
   * concurrency so callers can validate the full pipeline in a predictable order.
   *
   * @param sparkContext active Spark context used to reload partition files on demand.
   * @param materialized disk-backed working set whose partition files will be consumed by jobs.
   * @return aggregated execution summary for the sequential disk-hybrid run.
   */
  public SpatialJoinResult executeMaterializedPartitionAwareSequential(JavaSparkContext sparkContext,
                                                                      MaterializedPartitionExecution materialized) {
    if (materialized == null) {
      return SpatialJoinResult.fromPlan(
          getLeftInputPath(),
          getRightInputPath(),
          getLeftInputFormat(),
          getRightInputFormat(),
          getJoinPredicate(),
          "partition-aware-" + getJoinAlgorithm() + "-disk-hybrid-sequential",
          0L,
          0L,
          0L,
          null,
          Collections.emptyList());
    }
    JobQueueExecutionPlan executionPlan = createJobPlanner().buildJobPlan(
        materialized.getEffectivePlan() == null ? Collections.emptyList() : materialized.getEffectivePlan().getPartitionProfiles());
    if (isGlobalObjectPairDedupEnabled()) {
      List<JobExecutionResult> jobResults = executeObjectPairFilePlanSequential(sparkContext, materialized, executionPlan);
      long uniqueMatches = countUniqueObjectPairsFromJobResults(sparkContext, jobResults);
      return buildPartitionAwareResult(materialized, executionPlan, jobResults,
          "partition-aware-" + getJoinAlgorithm() + "-disk-hybrid-sequential-object-global", uniqueMatches);
    }
    List<JobExecutionResult> jobResults = executePartitionPlanSequential(sparkContext, materialized, executionPlan);
    return buildPartitionAwareResult(materialized, executionPlan, jobResults,
        "partition-aware-" + getJoinAlgorithm() + "-disk-hybrid-sequential");
  }

  private SpatialJoinResult buildPartitionAwareResult(PreparedPartitionExecution prepared,
                                                      JobQueueExecutionPlan executionPlan,
                                                      List<JobExecutionResult> jobResults) {
    return buildPartitionAwareResult(prepared, executionPlan, jobResults, "partition-aware-" + getJoinAlgorithm());
  }

  private SpatialJoinResult buildPartitionAwareResult(PreparedPartitionExecution prepared,
                                                      JobQueueExecutionPlan executionPlan,
                                                      List<JobExecutionResult> jobResults,
                                                      String algorithmLabel) {
    return buildPartitionAwareResult(prepared, executionPlan, jobResults, algorithmLabel, null);
  }

  private SpatialJoinResult buildPartitionAwareResult(PreparedPartitionExecution prepared,
                                                      JobQueueExecutionPlan executionPlan,
                                                      List<JobExecutionResult> jobResults,
                                                      String algorithmLabel,
                                                      Long overrideMatchCount) {
    long totalMatches = 0L;
    if (overrideMatchCount != null) {
      totalMatches = Math.max(0L, overrideMatchCount);
    } else {
      for (JobExecutionResult jobResult : jobResults) {
        totalMatches += jobResult.getMatchCount();
      }
    }
    return SpatialJoinResult.fromPlan(
        getLeftInputPath(),
        getRightInputPath(),
        getLeftInputFormat(),
        getRightInputFormat(),
        getJoinPredicate(),
        algorithmLabel,
        prepared.getLeftFeatureCount(),
        prepared.getRightFeatureCount(),
        totalMatches,
        executionPlan,
        jobResults);
  }

  private SpatialJoinResult buildPartitionAwareResult(MaterializedPartitionExecution materialized,
                                                      JobQueueExecutionPlan executionPlan,
                                                      List<JobExecutionResult> jobResults,
                                                      String algorithmLabel) {
    return buildPartitionAwareResult(materialized, executionPlan, jobResults, algorithmLabel, null);
  }

  private SpatialJoinResult buildPartitionAwareResult(MaterializedPartitionExecution materialized,
                                                      JobQueueExecutionPlan executionPlan,
                                                      List<JobExecutionResult> jobResults,
                                                      String algorithmLabel,
                                                      Long overrideMatchCount) {
    long totalMatches = 0L;
    if (overrideMatchCount != null) {
      totalMatches = Math.max(0L, overrideMatchCount);
    } else {
      for (JobExecutionResult jobResult : jobResults) {
        totalMatches += jobResult.getMatchCount();
      }
    }
    return SpatialJoinResult.fromPlan(
        getLeftInputPath(),
        getRightInputPath(),
        getLeftInputFormat(),
        getRightInputFormat(),
        getJoinPredicate(),
        algorithmLabel,
        materialized == null ? 0L : materialized.getLeftFeatureCount(),
        materialized == null ? 0L : materialized.getRightFeatureCount(),
        totalMatches,
        executionPlan,
        jobResults);
  }

  private static int[] buildLocalPhysicalPartitionIds(int partitionCount) {
    if (partitionCount <= 0)
      return new int[0];
    int[] partitionIds = new int[partitionCount];
    for (int i = 0; i < partitionCount; i++)
      partitionIds[i] = i;
    return partitionIds;
  }

  private static JavaSparkContext sparkContextFromPrepared(PreparedPartitionExecution prepared) {
    if (prepared == null)
      return JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate());
    if (prepared.getLeftPartitionedFeatures() != null)
      return JavaSparkContext.fromSparkContext(prepared.getLeftPartitionedFeatures().context());
    if (prepared.getLeftFeaturesByOid() != null)
      return JavaSparkContext.fromSparkContext(prepared.getLeftFeaturesByOid().context());
    if (prepared.getRightPartitionedFeatures() != null)
      return JavaSparkContext.fromSparkContext(prepared.getRightPartitionedFeatures().context());
    if (prepared.getRightFeaturesByOid() != null)
      return JavaSparkContext.fromSparkContext(prepared.getRightFeaturesByOid().context());
    return JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate());
  }

  private static boolean hasValidPhysicalPartitionSelection(JavaPairRDD<Integer, IFeature> leftPartitionFeatures,
                                                            JavaPairRDD<Integer, IFeature> rightPartitionFeatures,
                                                            int[] selectedPhysicalPartitionIds,
                                                            int[] localPhysicalToGlobalPartitionIds) {
    if (leftPartitionFeatures == null || rightPartitionFeatures == null
        || selectedPhysicalPartitionIds == null || selectedPhysicalPartitionIds.length == 0)
      return false;
    int leftPartitionCount = leftPartitionFeatures.rdd().partitions().length;
    int rightPartitionCount = rightPartitionFeatures.rdd().partitions().length;
    if (leftPartitionCount <= 0 || leftPartitionCount != rightPartitionCount)
      return false;
    if (localPhysicalToGlobalPartitionIds != null && localPhysicalToGlobalPartitionIds.length != leftPartitionCount)
      return false;
    for (int selectedPhysicalPartitionId : selectedPhysicalPartitionIds) {
      if (selectedPhysicalPartitionId < 0 || selectedPhysicalPartitionId >= leftPartitionCount)
        return false;
    }
    return true;
  }

  private static JobExecutionResult buildJobExecutionResult(JobGroup jobGroup,
                                                            PartitionTaskExecutionSummary summary) {
    return buildJobExecutionResult(jobGroup, summary, null);
  }

  private static JobExecutionResult buildJobExecutionResult(JobGroup jobGroup,
                                                            PartitionTaskExecutionSummary summary,
                                                            String objectPairOutputPath) {
    if (jobGroup == null || summary == null)
      return new JobExecutionResult(-1, null, new int[0], 0L, 0L, 0L, 0L);
    long leftFeatureCount = 0L;
    long rightFeatureCount = 0L;
    for (PartCost partition : jobGroup.getPartitions()) {
      leftFeatureCount += partition.getProfile().getLeftFeatureCount();
      rightFeatureCount += partition.getProfile().getRightFeatureCount();
    }
    return new JobExecutionResult(
        jobGroup.getJobId(),
        jobGroup.getComplexityType(),
        jobGroup.getPartitionIds(),
        leftFeatureCount,
        rightFeatureCount,
        jobGroup.getTotalMemoryLoadBytes(),
        summary.getMatchCount(),
        summary.getLocalJoinNanos(),
        summary.getPartitionLocalJoinNanos(),
        summary.getPlaneSweepXAxisCalls(),
        summary.getPlaneSweepYAxisCalls(),
        summary.getPlaneSweepDiagonalAxisCalls(),
        objectPairOutputPath);
  }

  /**
   * Executes the prepared working set in object-level global de-duplication mode.
   *
   * <p>This path is intentionally opt-in. The default {@code none} mode keeps the existing
   * resource-aware per-job counting path unchanged, while {@code global} writes original OID
   * pairs from all effective partitions to external files for downstream object-level recovery.
   */
  public JobExecutionResult executeGlobalObjectPairDedupJob(PreparedPartitionExecution prepared,
                                                            JobQueueExecutionPlan executionPlan) {
    if (prepared == null || prepared.getEffectivePlan() == null)
      return new JobExecutionResult(-100, PartitionComplexityType.NORMAL, new int[0], 0L, 0L, 0L, 0L);
    List<JobExecutionResult> jobResults = executeObjectPairFilePlanSequential(prepared, executionPlan);
    return aggregateObjectPairFileResults(executionPlan, prepared.getEffectivePlan(), jobResults);
  }

  /** Executes the disk-hybrid working set in object-level global de-duplication mode. */
  public JobExecutionResult executeGlobalObjectPairDedupJob(JavaSparkContext sparkContext,
                                                            MaterializedPartitionExecution materialized,
                                                            JobQueueExecutionPlan executionPlan) {
    if (sparkContext == null || materialized == null || materialized.getEffectivePlan() == null)
      return new JobExecutionResult(-100, PartitionComplexityType.NORMAL, new int[0], 0L, 0L, 0L, 0L);
    List<JobExecutionResult> jobResults = executeObjectPairFilePlanSequential(sparkContext, materialized, executionPlan);
    return aggregateObjectPairFileResults(executionPlan, materialized.getEffectivePlan(), jobResults);
  }

  /** Writes original object-id pairs for one prepared task unit without changing the normal count-only path. */
  public JobExecutionResult executeObjectPairFileJob(PreparedPartitionExecution prepared,
                                                     JobGroup jobGroup) {
    if (prepared == null || prepared.getEffectivePlan() == null || jobGroup == null)
      return new JobExecutionResult(-1, PartitionComplexityType.NORMAL, new int[0], 0L, 0L, 0L, 0L);
    int[] partitionIds = jobGroup.getPartitionIds();
    JavaPairRDD<Integer, IFeature> leftPartitionFeatures = loadLeftBinaryFeatures(prepared, partitionIds);
    JavaPairRDD<Integer, IFeature> rightPartitionFeatures = loadRightBinaryFeatures(prepared, partitionIds);
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(leftPartitionFeatures.context());
    return executeObjectPairFileJob(
        sparkContext,
        prepared.getEffectivePlan(),
        leftPartitionFeatures,
        rightPartitionFeatures,
        prepared.getPartitionAssignmentStrategy(),
        jobGroup,
        "prepared-object-pairs");
  }

  /** Writes original object-id pairs for one disk-hybrid task unit without retaining pair sets in memory. */
  public JobExecutionResult executeObjectPairFileJob(JavaSparkContext sparkContext,
                                                     MaterializedPartitionExecution materialized,
                                                     JobGroup jobGroup) {
    if (sparkContext == null || materialized == null || materialized.getEffectivePlan() == null || jobGroup == null)
      return new JobExecutionResult(-1, PartitionComplexityType.NORMAL, new int[0], 0L, 0L, 0L, 0L);
    int[] partitionIds = jobGroup.getPartitionIds();
    JavaPairRDD<Integer, IFeature> leftPartitionFeatures = loadLeftBinaryFeatures(sparkContext, materialized, partitionIds);
    JavaPairRDD<Integer, IFeature> rightPartitionFeatures = loadRightBinaryFeatures(sparkContext, materialized, partitionIds);
    return executeObjectPairFileJob(
        sparkContext,
        materialized.getEffectivePlan(),
        leftPartitionFeatures,
        rightPartitionFeatures,
        materialized.getPartitionAssignmentStrategy(),
        jobGroup,
        "materialized-object-pairs");
  }

  private JobExecutionResult executeObjectPairFileJob(JavaSparkContext sparkContext,
                                                      SpatialPartitioner.EffectivePlan effectivePlan,
                                                      JavaPairRDD<Integer, IFeature> leftPartitionFeatures,
                                                      JavaPairRDD<Integer, IFeature> rightPartitionFeatures,
                                                      String partitionAssignmentStrategy,
                                                      JobGroup jobGroup,
                                                      String executionSource) {
    if (sparkContext == null || effectivePlan == null || effectivePlan.getPartitioner() == null
        || leftPartitionFeatures == null || rightPartitionFeatures == null || jobGroup == null) {
      return new JobExecutionResult(-1, PartitionComplexityType.NORMAL, new int[0], 0L, 0L, 0L, 0L);
    }
    SpatialPartitioner partitioner = effectivePlan.getPartitioner();
    boolean useReferencePointDuplicateAvoidance =
        usesReferencePointDuplicateAvoidance(partitionAssignmentStrategy);
    final long[] expectedLeftCountsByPartitionId = buildExpectedPartitionFeatureCounts(effectivePlan, true);
    final long[] expectedRightCountsByPartitionId = buildExpectedPartitionFeatureCounts(effectivePlan, false);
    int[] partitionIds = jobGroup.getPartitionIds();
    int[] localPhysicalPartitionIds = buildLocalPhysicalPartitionIds(partitionIds.length);
    if (hasValidPhysicalPartitionSelection(
        leftPartitionFeatures,
        rightPartitionFeatures,
        localPhysicalPartitionIds,
        partitionIds)) {
      String outputPath = buildObjectPairOutputPath(executionSource + "-job-" + jobGroup.getJobId());
      System.out.println("Object pair output path       : " + outputPath);
      System.out.println("Object pair write path        : compact zipPartitions/runJob");
      deletePathIfExists(sparkContext, outputPath);
      PartitionTaskExecutionSummary compactSummary =
          CompactPartitionJobExecution$.MODULE$.executeSelectedPartitionsToObjectPairFiles(
              this,
              partitioner,
              useReferencePointDuplicateAvoidance,
              jobGroup.getJobId(),
              leftPartitionFeatures,
              rightPartitionFeatures,
              localPhysicalPartitionIds,
              partitionIds,
              expectedLeftCountsByPartitionId,
              expectedRightCountsByPartitionId,
              executionSource,
              outputPath);
      return buildJobExecutionResult(jobGroup, compactSummary, outputPath);
    }
    LongAccumulator planeSweepXAxisCalls =
        createJobAccumulator(sparkContext, "sj.planeSweepAxisX.objectPairs", jobGroup.getJobId());
    LongAccumulator planeSweepYAxisCalls =
        createJobAccumulator(sparkContext, "sj.planeSweepAxisY.objectPairs", jobGroup.getJobId());
    LongAccumulator planeSweepDiagonalAxisCalls =
        createJobAccumulator(sparkContext, "sj.planeSweepAxisDiagonal.objectPairs", jobGroup.getJobId());
    JavaPairRDD<Integer, Tuple2<Iterable<IFeature>, Iterable<IFeature>>> grouped =
        leftPartitionFeatures.cogroup(rightPartitionFeatures, partitioner);
    LongAccumulator emittedPairRecords = sparkContext.sc().longAccumulator(
        "sj.objectPairRecords." + jobGroup.getJobId() + "." + System.nanoTime());
    String outputPath = buildObjectPairOutputPath(executionSource + "-job-" + jobGroup.getJobId());
    System.out.println("Object pair write path        : fallback cogroup/saveAsTextFile");
    JavaRDD<String> pairLines = grouped.flatMap(group -> executePartitionJoinObjectPairLines(
            partitioner,
            group._1,
            group._2._1,
            group._2._2,
            useReferencePointDuplicateAvoidance,
            planeSweepXAxisCalls,
            planeSweepYAxisCalls,
            planeSweepDiagonalAxisCalls,
            executionSource,
            jobGroup.getJobId(),
            expectedLeftCountsByPartitionId,
            expectedRightCountsByPartitionId,
            emittedPairRecords));
    System.out.println("Object pair output path       : " + outputPath);
    pairLines.saveAsTextFile(outputPath);
    return buildJobExecutionResult(
        jobGroup,
        new PartitionTaskExecutionSummary(
            emittedPairRecords.value(),
            planeSweepXAxisCalls.value(),
            planeSweepYAxisCalls.value(),
            planeSweepDiagonalAxisCalls.value()),
        outputPath);
  }

  private String buildObjectPairOutputPath(String executionSource) {
    String configuredBase = options.getString(
        SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_OUTPUT_BASE_PATH,
        SpatialJoinConstants.Execution.DEFAULT_OBJECT_PAIR_DEDUP_OUTPUT_BASE_PATH);
    String base = configuredBase == null || configuredBase.trim().isEmpty()
        ? options.getString(
            SpatialJoinConstants.Execution.DISK_MATERIALIZATION_BASE_PATH,
            SpatialJoinConstants.Execution.DEFAULT_DISK_MATERIALIZATION_BASE_PATH)
        : configuredBase.trim();
    String normalizedBase = normalizeOutputPath(base);
    String suffix = (executionSource == null || executionSource.trim().isEmpty())
        ? "object-pairs"
        : executionSource.trim().replaceAll("[^A-Za-z0-9._-]+", "-");
    String separator = normalizedBase.endsWith("/") ? "" : "/";
    return normalizedBase + separator + "object_pairs/" + suffix + "-" + UUID.randomUUID();
  }

  private static String normalizeOutputPath(String path) {
    if (path == null || path.trim().isEmpty())
      return "object_pairs/" + UUID.randomUUID();
    String normalized = path.trim().replace('\\', '/');
    if (normalized.matches("^[A-Za-z]:/.*"))
      return "file:/" + normalized;
    return normalized;
  }

  private static File createTaskLocalObjectPairSpillFile() throws IOException {
    File directory = resolveSparkLocalDirectory();
    if (directory != null) {
      if (!directory.exists() && !directory.mkdirs())
        directory = null;
      if (directory != null)
        return File.createTempFile("sj-object-pairs-", ".tsv", directory);
    }
    return File.createTempFile("sj-object-pairs-", ".tsv");
  }

  private static File resolveSparkLocalDirectory() {
    String configured = null;
    try {
      SparkEnv env = SparkEnv.get();
      if (env != null && env.conf() != null)
        configured = env.conf().get("spark.local.dir", "");
    } catch (RuntimeException ignored) {
      // Fall back to java.io.tmpdir when SparkEnv is not available on the current thread.
    }
    if (configured == null || configured.trim().isEmpty())
      configured = System.getProperty("java.io.tmpdir", "");
    if (configured == null || configured.trim().isEmpty())
      return null;
    String first = configured.split(",")[0].trim().replace('\\', '/');
    if (first.startsWith("file:/"))
      first = first.substring("file:/".length());
    if (first.isEmpty())
      return null;
    return new File(first);
  }

  private static void closeQuietly(Closeable closeable) {
    if (closeable == null)
      return;
    try {
      closeable.close();
    } catch (IOException ignored) {
      // Best-effort cleanup.
    }
  }

  private static void deleteFileQuietly(File file) {
    if (file != null && file.exists() && !file.delete())
      file.deleteOnExit();
  }

  public long countUniqueObjectPairsFromJobResults(JavaSparkContext sparkContext,
                                                   List<JobExecutionResult> jobResults) {
    if (jobResults == null || jobResults.isEmpty())
      return 0L;
    List<String> outputPaths = new ArrayList<>();
    long emittedRecords = 0L;
    for (JobExecutionResult result : jobResults) {
      if (result == null)
        continue;
      emittedRecords += result.getMatchCount();
      String outputPath = result.getObjectPairOutputPath();
      if (outputPath != null && !outputPath.trim().isEmpty())
        outputPaths.add(outputPath.trim());
    }
    if (outputPaths.isEmpty())
      return emittedRecords;
    File bucketDirectory = createObjectPairDedupBucketDirectory();
    int bucketCount = 512;
    BufferedWriter[] bucketWriters = new BufferedWriter[bucketCount];
    Configuration configuration =
        sparkContext == null ? new Configuration() : sparkContext.hadoopConfiguration();
    try {
      long bucketedRecords = bucketObjectPairFiles(configuration, outputPaths, bucketDirectory, bucketWriters);
      long uniqueRecords = countUniqueBucketedObjectPairs(bucketDirectory, bucketCount);
      System.out.println("Object pair emitted records    : " + emittedRecords);
      System.out.println("Object pair bucketed records   : " + bucketedRecords);
      System.out.println("Object pair global unique count: " + uniqueRecords);
      return uniqueRecords;
    } finally {
      closeBucketWriters(bucketWriters);
      deleteDirectoryQuietly(bucketDirectory);
      cleanupObjectPairOutputPaths(configuration, outputPaths);
    }
  }

  private static void cleanupObjectPairOutputPaths(Configuration configuration,
                                                   List<String> outputPaths) {
    if (outputPaths == null || outputPaths.isEmpty())
      return;
    Set<String> uniquePaths = new LinkedHashSet<>();
    for (String outputPath : outputPaths) {
      if (outputPath != null && !outputPath.trim().isEmpty())
        uniquePaths.add(outputPath.trim());
    }
    if (uniquePaths.isEmpty())
      return;
    Configuration effectiveConfiguration = configuration == null ? new Configuration() : configuration;
    int deleted = 0;
    for (String outputPath : uniquePaths) {
      try {
        Path rootPath = new Path(outputPath);
        FileSystem fileSystem = rootPath.getFileSystem(effectiveConfiguration);
        if (fileSystem.exists(rootPath)) {
          if (fileSystem.delete(rootPath, true))
            deleted++;
          else
            System.err.println("Warning: failed to delete object-pair output path " + outputPath);
        }
      } catch (IOException | RuntimeException e) {
        System.err.println("Warning: failed to clean object-pair output path "
            + outputPath + ": " + e.getMessage());
      }
    }
    System.out.println("Object pair output paths cleaned: " + deleted + "/" + uniquePaths.size());
  }

  private static File createObjectPairDedupBucketDirectory() {
    File root = resolveSparkLocalDirectory();
    if (root == null)
      root = new File(System.getProperty("java.io.tmpdir", "."));
    File directory = new File(root, "sj-object-pair-dedup-" + UUID.randomUUID());
    if (!directory.exists() && !directory.mkdirs())
      throw new RuntimeException("Failed to create object-pair dedup bucket directory " + directory);
    return directory;
  }

  private static long bucketObjectPairFiles(Configuration configuration,
                                            List<String> outputPaths,
                                            File bucketDirectory,
                                            BufferedWriter[] bucketWriters) {
    long recordCount = 0L;
    for (String outputPath : outputPaths) {
      try {
        Path rootPath = new Path(outputPath);
        FileSystem fileSystem = rootPath.getFileSystem(configuration);
        if (!fileSystem.exists(rootPath))
          continue;
        List<Path> files = new ArrayList<>();
        collectObjectPairFiles(fileSystem, rootPath, files);
        for (Path file : files) {
          try (BufferedReader reader =
                   new BufferedReader(new InputStreamReader(fileSystem.open(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
              String pair = line.trim();
              if (pair.isEmpty())
                continue;
              int bucket = Math.floorMod(pair.hashCode(), bucketWriters.length);
              BufferedWriter writer = bucketWriters[bucket];
              if (writer == null) {
                writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(new File(bucketDirectory, "bucket-" + bucket + ".tsv"), true),
                    StandardCharsets.UTF_8));
                bucketWriters[bucket] = writer;
              }
              writer.write(pair);
              writer.newLine();
              recordCount++;
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to bucket object-pair output " + outputPath, e);
      }
    }
    closeBucketWriters(bucketWriters);
    return recordCount;
  }

  private static void collectObjectPairFiles(FileSystem fileSystem,
                                             Path path,
                                             List<Path> files) throws IOException {
    FileStatus status = fileSystem.getFileStatus(path);
    if (status.isDirectory()) {
      for (FileStatus child : fileSystem.listStatus(path)) {
        String name = child.getPath().getName();
        if (name.startsWith("_") || name.startsWith("."))
          continue;
        collectObjectPairFiles(fileSystem, child.getPath(), files);
      }
    } else {
      String name = path.getName();
      if (!name.startsWith("_") && !name.startsWith("."))
        files.add(path);
    }
  }

  private static long countUniqueBucketedObjectPairs(File bucketDirectory,
                                                     int bucketCount) {
    long uniqueRecords = 0L;
    for (int bucket = 0; bucket < bucketCount; bucket++) {
      File bucketFile = new File(bucketDirectory, "bucket-" + bucket + ".tsv");
      if (!bucketFile.exists() || bucketFile.length() <= 0L)
        continue;
      Set<String> pairs = new HashSet<>();
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(new FileInputStream(bucketFile), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String pair = line.trim();
          if (!pair.isEmpty())
            pairs.add(pair);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to count unique object pairs in " + bucketFile, e);
      }
      uniqueRecords += pairs.size();
      pairs.clear();
    }
    return uniqueRecords;
  }

  private static void closeBucketWriters(BufferedWriter[] writers) {
    if (writers == null)
      return;
    for (int i = 0; i < writers.length; i++) {
      closeQuietly(writers[i]);
      writers[i] = null;
    }
  }

  private static void deleteDirectoryQuietly(File directory) {
    if (directory == null || !directory.exists())
      return;
    File[] children = directory.listFiles();
    if (children != null) {
      for (File child : children) {
        if (child.isDirectory())
          deleteDirectoryQuietly(child);
        else
          deleteFileQuietly(child);
      }
    }
    if (!directory.delete())
      directory.deleteOnExit();
  }

  private static JobExecutionResult aggregateObjectPairFileResults(JobQueueExecutionPlan executionPlan,
                                                                   SpatialPartitioner.EffectivePlan effectivePlan,
                                                                   List<JobExecutionResult> jobResults) {
    long matchCount = 0L;
    long planeSweepXAxisCalls = 0L;
    long planeSweepYAxisCalls = 0L;
    long planeSweepDiagonalAxisCalls = 0L;
    if (jobResults != null) {
      for (JobExecutionResult result : jobResults) {
        if (result == null)
          continue;
        matchCount += result.getMatchCount();
        planeSweepXAxisCalls += result.getPlaneSweepXAxisCount();
        planeSweepYAxisCalls += result.getPlaneSweepYAxisCount();
        planeSweepDiagonalAxisCalls += result.getPlaneSweepDiagonalAxisCount();
      }
    }
    return buildGlobalObjectPairDedupResult(
        executionPlan,
        effectivePlan,
        collectExecutionPartitionIds(executionPlan, effectivePlan),
        matchCount,
        planeSweepXAxisCalls,
        planeSweepYAxisCalls,
        planeSweepDiagonalAxisCalls);
  }

  private static JobExecutionResult buildGlobalObjectPairDedupResult(JobQueueExecutionPlan executionPlan,
                                                                     SpatialPartitioner.EffectivePlan effectivePlan,
                                                                     int[] partitionIds,
                                                                     long matchCount,
                                                                     long planeSweepXAxisCalls,
                                                                     long planeSweepYAxisCalls,
                                                                     long planeSweepDiagonalAxisCalls) {
    Map<Integer, PartProfile> profilesById = new HashMap<>();
    if (executionPlan != null && executionPlan.getJobGroups() != null) {
      for (JobGroup jobGroup : executionPlan.getJobGroups()) {
        if (jobGroup == null || jobGroup.getPartitions() == null)
          continue;
        for (PartCost partCost : jobGroup.getPartitions()) {
          if (partCost != null && partCost.getProfile() != null)
            profilesById.put(partCost.getProfile().getPartitionId(), partCost.getProfile());
        }
      }
    }
    if (profilesById.isEmpty() && effectivePlan != null && effectivePlan.getPartitionProfiles() != null) {
      for (PartProfile profile : effectivePlan.getPartitionProfiles()) {
        if (profile != null)
          profilesById.put(profile.getPartitionId(), profile);
      }
    }
    long leftFeatureCount = 0L;
    long rightFeatureCount = 0L;
    long totalMemoryLoadBytes = 0L;
    if (partitionIds != null) {
      for (int partitionId : partitionIds) {
        PartProfile profile = profilesById.get(partitionId);
        if (profile == null)
          continue;
        leftFeatureCount += profile.getLeftFeatureCount();
        rightFeatureCount += profile.getRightFeatureCount();
        totalMemoryLoadBytes += profile.getTotalMemoryLoadBytes();
      }
    }
    return new JobExecutionResult(
        -100,
        PartitionComplexityType.NORMAL,
        partitionIds,
        leftFeatureCount,
        rightFeatureCount,
        totalMemoryLoadBytes,
        matchCount,
        planeSweepXAxisCalls,
        planeSweepYAxisCalls,
        planeSweepDiagonalAxisCalls);
  }

  private static int[] collectExecutionPartitionIds(JobQueueExecutionPlan executionPlan,
                                                    SpatialPartitioner.EffectivePlan effectivePlan) {
    LinkedHashSet<Integer> ids = new LinkedHashSet<>();
    if (executionPlan != null && executionPlan.getJobGroups() != null) {
      for (JobGroup jobGroup : executionPlan.getJobGroups()) {
        if (jobGroup == null)
          continue;
        int[] partitionIds = jobGroup.getPartitionIds();
        if (partitionIds == null)
          continue;
        for (int partitionId : partitionIds)
          ids.add(partitionId);
      }
    }
    if (ids.isEmpty() && effectivePlan != null && effectivePlan.getPartitionProfiles() != null) {
      for (PartProfile profile : effectivePlan.getPartitionProfiles()) {
        if (profile != null)
          ids.add(profile.getPartitionId());
      }
    }
    int[] partitionIds = new int[ids.size()];
    int index = 0;
    for (Integer partitionId : ids)
      partitionIds[index++] = partitionId == null ? -1 : partitionId;
    return partitionIds;
  }

  private static long[] buildExpectedPartitionFeatureCounts(SpatialPartitioner.EffectivePlan effectivePlan,
                                                            boolean leftSide) {
    int partitionCount =
        effectivePlan == null || effectivePlan.getPartitioner() == null ? 0 : effectivePlan.getPartitioner().numPartitions();
    long[] expectedCounts = new long[partitionCount];
    if (effectivePlan == null || effectivePlan.getPartitionProfiles() == null)
      return expectedCounts;
    for (PartProfile profile : effectivePlan.getPartitionProfiles()) {
      if (profile == null)
        continue;
      int partitionId = profile.getPartitionId();
      if (partitionId < 0 || partitionId >= expectedCounts.length)
        continue;
      expectedCounts[partitionId] = leftSide ? profile.getLeftFeatureCount() : profile.getRightFeatureCount();
    }
    return expectedCounts;
  }

  private static long[] buildExpectedPartitionFeatureCounts(JobGroup jobGroup,
                                                            int partitionCount,
                                                            boolean leftSide) {
    long[] expectedCounts = new long[Math.max(0, partitionCount)];
    if (jobGroup == null || jobGroup.getPartitions() == null)
      return expectedCounts;
    for (PartCost partition : jobGroup.getPartitions()) {
      if (partition == null || partition.getProfile() == null)
        continue;
      PartProfile profile = partition.getProfile();
      int partitionId = profile.getPartitionId();
      if (partitionId < 0 || partitionId >= expectedCounts.length)
        continue;
      expectedCounts[partitionId] = leftSide ? profile.getLeftFeatureCount() : profile.getRightFeatureCount();
    }
    return expectedCounts;
  }

  private PartitionJoinAuditContext createPartitionJoinAuditContext(String executionSource,
                                                                   int jobId,
                                                                   int partitionId,
                                                                   long[] expectedLeftCountsByPartitionId,
                                                                   long[] expectedRightCountsByPartitionId) {
    if (!isAuditPartitionJoin())
      return null;
    return new PartitionJoinAuditContext(
        executionSource,
        jobId,
        valueAt(expectedLeftCountsByPartitionId, partitionId),
        valueAt(expectedRightCountsByPartitionId, partitionId));
  }

  private static long valueAt(long[] values, int index) {
    return values == null || index < 0 || index >= values.length ? 0L : values[index];
  }

  private static void printPartitionJoinAudit(PartitionJoinAuditContext auditContext,
                                              int partitionId,
                                              int loadedLeftCount,
                                              int loadedRightCount,
                                              long rawMatchCount,
                                              long keptMatchCount,
                                              long rejectedByReferencePoint) {
    if (auditContext == null)
      return;
    System.out.println(String.format(
        "[Partition audit] source=%s, jobId=%d, partitionId=%d, loadedLeft=%d, loadedRight=%d, " +
            "profileLeft=%d, profileRight=%d, rawCandidateCount=%d, keptCount=%d, rejectedByReferencePoint=%d",
        auditContext.executionSource,
        auditContext.jobId,
        partitionId,
        loadedLeftCount,
        loadedRightCount,
        auditContext.expectedLeftCount,
        auditContext.expectedRightCount,
        rawMatchCount,
        keptMatchCount,
        rejectedByReferencePoint));
  }

  /**
   * Executes one planned partition job by reloading only the binary features mapped
   * to the partitions in the given job group.
   *
   * @param sparkContext active Spark context
   * @param effectivePlan effective partitioning result
   * @param jobGroup one planned job group
   * @return execution summary for that job
   */
  @Deprecated
  public JobExecutionResult executePartitionJob(JavaSparkContext sparkContext,
                                                SpatialPartitioner.EffectivePlan effectivePlan,
                                                JobGroup jobGroup) {
    if (jobGroup == null || effectivePlan == null) {
      return new JobExecutionResult(-1, null, new int[0], 0L, 0L, 0L, 0L);
    }
    int[] partitionIds = jobGroup.getPartitionIds();
    JavaPairRDD<Integer, IFeature> leftPartitionFeatures = loadLeftBinaryFeatures(sparkContext, effectivePlan, partitionIds);
    JavaPairRDD<Integer, IFeature> rightPartitionFeatures = loadRightBinaryFeatures(sparkContext, effectivePlan, partitionIds);
    boolean useReferencePointDuplicateAvoidance = usesReferencePointDuplicateAvoidance(getPartitionAssignmentStrategy());
    final long[] expectedLeftCountsByPartitionId = buildExpectedPartitionFeatureCounts(effectivePlan, true);
    final long[] expectedRightCountsByPartitionId = buildExpectedPartitionFeatureCounts(effectivePlan, false);
    LongAccumulator planeSweepXAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisX.job", jobGroup.getJobId());
    LongAccumulator planeSweepYAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisY.job", jobGroup.getJobId());
    LongAccumulator planeSweepDiagonalAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisDiagonal.job", jobGroup.getJobId());
    JavaPairRDD<Integer, Tuple2<Iterable<IFeature>, Iterable<IFeature>>> grouped =
        leftPartitionFeatures.cogroup(rightPartitionFeatures, effectivePlan.getPartitioner());
    long matchCount = grouped
        .map(group -> executePartitionJoin(
            effectivePlan.getPartitioner(),
            group._1,
            group._2._1,
            group._2._2,
            useReferencePointDuplicateAvoidance,
            null,
            null,
            planeSweepXAxisCalls,
            planeSweepYAxisCalls,
            planeSweepDiagonalAxisCalls))
        .fold(0L, Long::sum);

    long leftFeatureCount = 0L;
    long rightFeatureCount = 0L;
    for (PartCost partition : jobGroup.getPartitions()) {
      leftFeatureCount += partition.getProfile().getLeftFeatureCount();
      rightFeatureCount += partition.getProfile().getRightFeatureCount();
    }
    return new JobExecutionResult(
        jobGroup.getJobId(),
        jobGroup.getComplexityType(),
        partitionIds,
        leftFeatureCount,
        rightFeatureCount,
        jobGroup.getTotalMemoryLoadBytes(),
        matchCount,
        planeSweepXAxisCalls.value(),
        planeSweepYAxisCalls.value(),
        planeSweepDiagonalAxisCalls.value());
  }

  /**
   * Executes one planned partition job against a prepared execution context.
   *
   * This overload is preferred for partition-aware execution because it reuses the
   * cached OID-to-feature RDDs created during preparation instead of rereading the inputs.
   *
   * <p>The method performs one full job in four steps:
   * <ol>
   *   <li>reload the left/right binary features whose assignments target {@code partitionIds},</li>
   *   <li>{@code cogroup} both sides by the final effective partitioner,</li>
   *   <li>run one partition-local join per grouped partition id, and</li>
   *   <li>aggregate partition-level match counts into one per-job result summary.</li>
   * </ol>
   *
   * @param prepared prepared execution context holding cached {@code (oid, feature)} RDDs and the final effective plan.
   * @param jobGroup one planned job containing the target partition ids plus planning metadata.
   * @return execution summary for this one job.
   */
  public JobExecutionResult executePartitionJob(PreparedPartitionExecution prepared,
                                                JobGroup jobGroup) {
    if (jobGroup == null || prepared == null || prepared.getEffectivePlan() == null) {
      return new JobExecutionResult(-1, null, new int[0], 0L, 0L, 0L, 0L);
    }
    int[] partitionIds = jobGroup.getPartitionIds();
    SpatialPartitioner partitioner = prepared.getEffectivePlan().getPartitioner();
    boolean useReferencePointDuplicateAvoidance =
        usesReferencePointDuplicateAvoidance(prepared.getPartitionAssignmentStrategy());
    final long[] expectedLeftCountsByPartitionId = buildExpectedPartitionFeatureCounts(prepared.getEffectivePlan(), true);
    final long[] expectedRightCountsByPartitionId = buildExpectedPartitionFeatureCounts(prepared.getEffectivePlan(), false);
    if (!isProfileSequentialJobs()
        && prepared.getLeftPartitionedFeatures() != null
        && prepared.getRightPartitionedFeatures() != null
        && hasValidPhysicalPartitionSelection(
            prepared.getLeftPartitionedFeatures(),
            prepared.getRightPartitionedFeatures(),
            partitionIds,
            null)) {
      PartitionTaskExecutionSummary compactSummary = CompactPartitionJobExecution$.MODULE$.executeSelectedPartitions(
          this,
          partitioner,
          useReferencePointDuplicateAvoidance,
          jobGroup.getJobId(),
          prepared.getLeftPartitionedFeatures(),
          prepared.getRightPartitionedFeatures(),
          partitionIds,
          null,
          expectedLeftCountsByPartitionId,
          expectedRightCountsByPartitionId,
          "prepared");
      return buildJobExecutionResult(jobGroup, compactSummary);
    }
    JavaPairRDD<Integer, IFeature> leftPartitionFeatures = loadLeftBinaryFeatures(prepared, partitionIds);
    JavaPairRDD<Integer, IFeature> rightPartitionFeatures = loadRightBinaryFeatures(prepared, partitionIds);
    if (isProfileSequentialJobs()) {
      return executePartitionJobWithProfiling(
          JavaSparkContext.fromSparkContext(leftPartitionFeatures.context()),
          partitioner,
          useReferencePointDuplicateAvoidance,
          jobGroup,
          leftPartitionFeatures,
          rightPartitionFeatures);
    }
    if (hasValidPhysicalPartitionSelection(leftPartitionFeatures, rightPartitionFeatures, partitionIds, null)) {
      PartitionTaskExecutionSummary compactSummary = CompactPartitionJobExecution$.MODULE$.executeSelectedPartitions(
          this,
          partitioner,
          useReferencePointDuplicateAvoidance,
          jobGroup.getJobId(),
          leftPartitionFeatures,
          rightPartitionFeatures,
          partitionIds,
          null,
          expectedLeftCountsByPartitionId,
          expectedRightCountsByPartitionId,
          "prepared");
      return buildJobExecutionResult(jobGroup, compactSummary);
    }
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(leftPartitionFeatures.context());
    LongAccumulator planeSweepXAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisX.job", jobGroup.getJobId());
    LongAccumulator planeSweepYAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisY.job", jobGroup.getJobId());
    LongAccumulator planeSweepDiagonalAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisDiagonal.job", jobGroup.getJobId());
    JavaPairRDD<Integer, Tuple2<Iterable<IFeature>, Iterable<IFeature>>> grouped =
        leftPartitionFeatures.cogroup(rightPartitionFeatures, partitioner);
    long matchCount = grouped
        .map(group -> executePartitionJoin(
            partitioner,
            group._1,
            group._2._1,
            group._2._2,
            useReferencePointDuplicateAvoidance,
            null,
            null,
            planeSweepXAxisCalls,
            planeSweepYAxisCalls,
            planeSweepDiagonalAxisCalls,
            "prepared",
            jobGroup.getJobId(),
            expectedLeftCountsByPartitionId,
            expectedRightCountsByPartitionId))
        .fold(0L, Long::sum);
    return buildJobExecutionResult(
        jobGroup,
        new PartitionTaskExecutionSummary(
            matchCount,
            planeSweepXAxisCalls.value(),
            planeSweepYAxisCalls.value(),
            planeSweepDiagonalAxisCalls.value()));
  }

  /**
   * Executes one prepared partition job for correlation diagnostics and records only the
   * accumulated partition-local join kernel time. Normal experiments should keep using
   * {@link #executePartitionJob(PreparedPartitionExecution, JobGroup)}.
   */
  public JobExecutionResult executePartitionJobForLocalJoinTiming(PreparedPartitionExecution prepared,
                                                                  JobGroup jobGroup) {
    if (jobGroup == null || prepared == null || prepared.getEffectivePlan() == null) {
      return new JobExecutionResult(-1, null, new int[0], 0L, 0L, 0L, 0L);
    }
    int[] partitionIds = jobGroup.getPartitionIds();
    SpatialPartitioner partitioner = prepared.getEffectivePlan().getPartitioner();
    boolean useReferencePointDuplicateAvoidance =
        usesReferencePointDuplicateAvoidance(prepared.getPartitionAssignmentStrategy());
    final long[] expectedLeftCountsByPartitionId = buildExpectedPartitionFeatureCounts(prepared.getEffectivePlan(), true);
    final long[] expectedRightCountsByPartitionId = buildExpectedPartitionFeatureCounts(prepared.getEffectivePlan(), false);
    if (prepared.getLeftPartitionedFeatures() != null
        && prepared.getRightPartitionedFeatures() != null
        && hasValidPhysicalPartitionSelection(
            prepared.getLeftPartitionedFeatures(),
            prepared.getRightPartitionedFeatures(),
            partitionIds,
            null)) {
      PartitionTaskExecutionSummary compactSummary =
          CompactPartitionJobExecution$.MODULE$.executeSelectedPartitionsForLocalJoinTiming(
              this,
              partitioner,
              useReferencePointDuplicateAvoidance,
              jobGroup.getJobId(),
              prepared.getLeftPartitionedFeatures(),
              prepared.getRightPartitionedFeatures(),
              partitionIds,
              null,
              expectedLeftCountsByPartitionId,
              expectedRightCountsByPartitionId,
              "correlation-prepared");
      return buildJobExecutionResult(jobGroup, compactSummary);
    }
    JavaPairRDD<Integer, IFeature> leftPartitionFeatures = loadLeftBinaryFeatures(prepared, partitionIds);
    JavaPairRDD<Integer, IFeature> rightPartitionFeatures = loadRightBinaryFeatures(prepared, partitionIds);
    if (hasValidPhysicalPartitionSelection(leftPartitionFeatures, rightPartitionFeatures, partitionIds, null)) {
      PartitionTaskExecutionSummary compactSummary =
          CompactPartitionJobExecution$.MODULE$.executeSelectedPartitionsForLocalJoinTiming(
              this,
              partitioner,
              useReferencePointDuplicateAvoidance,
              jobGroup.getJobId(),
              leftPartitionFeatures,
              rightPartitionFeatures,
              partitionIds,
              null,
              expectedLeftCountsByPartitionId,
              expectedRightCountsByPartitionId,
              "correlation-prepared");
      return buildJobExecutionResult(jobGroup, compactSummary);
    }
    return executePartitionJobWithLocalJoinTiming(
        JavaSparkContext.fromSparkContext(leftPartitionFeatures.context()),
        partitioner,
        useReferencePointDuplicateAvoidance,
        jobGroup,
        leftPartitionFeatures,
        rightPartitionFeatures,
        "correlation-prepared",
        expectedLeftCountsByPartitionId,
        expectedRightCountsByPartitionId);
  }

  /**
   * Executes one planned partition job against a disk-hybrid working set.
   *
   * <p>The job reloads only the left/right partition files whose ids belong to the current job group,
   * then runs the same partition-local join logic used by the in-memory path. The difference from the
   * prepared overload is only where the binary features come from: partition files on disk instead of
   * cached {@code (oid, feature)} RDDs.
   *
   * @param sparkContext active Spark context used to read partition files.
   * @param materialized materialized working set containing partition metadata and partition files.
   * @param jobGroup one planned job containing the partition ids to reload.
   * @return execution summary for this one job.
   */
  public JobExecutionResult executePartitionJob(JavaSparkContext sparkContext,
                                                MaterializedPartitionExecution materialized,
                                                JobGroup jobGroup) {
    if (jobGroup == null || materialized == null || materialized.getEffectivePlan() == null) {
      return new JobExecutionResult(-1, null, new int[0], 0L, 0L, 0L, 0L);
    }
    int[] partitionIds = jobGroup.getPartitionIds();
    SpatialPartitioner partitioner = materialized.getEffectivePlan().getPartitioner();
    boolean useReferencePointDuplicateAvoidance =
        usesReferencePointDuplicateAvoidance(materialized.getPartitionAssignmentStrategy());
    final long[] expectedLeftCountsByPartitionId = buildExpectedPartitionFeatureCounts(materialized.getEffectivePlan(), true);
    final long[] expectedRightCountsByPartitionId = buildExpectedPartitionFeatureCounts(materialized.getEffectivePlan(), false);
    JavaPairRDD<Integer, IFeature> leftPartitionFeatures = loadLeftBinaryFeatures(sparkContext, materialized, partitionIds);
    JavaPairRDD<Integer, IFeature> rightPartitionFeatures = loadRightBinaryFeatures(sparkContext, materialized, partitionIds);
    if (isProfileSequentialJobs()) {
      return executePartitionJobWithProfiling(
          sparkContext,
          partitioner,
          useReferencePointDuplicateAvoidance,
          jobGroup,
          leftPartitionFeatures,
          rightPartitionFeatures);
    }
    int[] localPhysicalPartitionIds = buildLocalPhysicalPartitionIds(partitionIds.length);
    if (hasValidPhysicalPartitionSelection(
        leftPartitionFeatures,
        rightPartitionFeatures,
        localPhysicalPartitionIds,
        partitionIds)) {
      PartitionTaskExecutionSummary compactSummary = CompactPartitionJobExecution$.MODULE$.executeSelectedPartitions(
          this,
          partitioner,
          useReferencePointDuplicateAvoidance,
          jobGroup.getJobId(),
          leftPartitionFeatures,
          rightPartitionFeatures,
          localPhysicalPartitionIds,
          partitionIds,
          expectedLeftCountsByPartitionId,
          expectedRightCountsByPartitionId,
          "materialized");
      return buildJobExecutionResult(jobGroup, compactSummary);
    }
    LongAccumulator planeSweepXAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisX.job", jobGroup.getJobId());
    LongAccumulator planeSweepYAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisY.job", jobGroup.getJobId());
    LongAccumulator planeSweepDiagonalAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisDiagonal.job", jobGroup.getJobId());
    JavaPairRDD<Integer, Tuple2<Iterable<IFeature>, Iterable<IFeature>>> grouped =
        leftPartitionFeatures.cogroup(rightPartitionFeatures, partitioner);
    long matchCount = grouped
        .map(group -> executePartitionJoin(
            partitioner,
            group._1,
            group._2._1,
            group._2._2,
            useReferencePointDuplicateAvoidance,
            null,
            null,
            planeSweepXAxisCalls,
            planeSweepYAxisCalls,
            planeSweepDiagonalAxisCalls,
            "materialized",
            jobGroup.getJobId(),
            expectedLeftCountsByPartitionId,
            expectedRightCountsByPartitionId))
        .fold(0L, Long::sum);
    return buildJobExecutionResult(
        jobGroup,
        new PartitionTaskExecutionSummary(
            matchCount,
            planeSweepXAxisCalls.value(),
            planeSweepYAxisCalls.value(),
            planeSweepDiagonalAxisCalls.value()));
  }

  /**
   * Executes one disk-hybrid partition job for correlation diagnostics and records only the
   * accumulated partition-local join kernel time. The partition-file reload and Spark scheduling
   * time are intentionally excluded from {@link JobExecutionResult#getLocalJoinNanos()}.
   */
  public JobExecutionResult executePartitionJobForLocalJoinTiming(JavaSparkContext sparkContext,
                                                                  MaterializedPartitionExecution materialized,
                                                                  JobGroup jobGroup) {
    if (jobGroup == null || materialized == null || materialized.getEffectivePlan() == null) {
      return new JobExecutionResult(-1, null, new int[0], 0L, 0L, 0L, 0L);
    }
    int[] partitionIds = jobGroup.getPartitionIds();
    SpatialPartitioner partitioner = materialized.getEffectivePlan().getPartitioner();
    boolean useReferencePointDuplicateAvoidance =
        usesReferencePointDuplicateAvoidance(materialized.getPartitionAssignmentStrategy());
    final long[] expectedLeftCountsByPartitionId = buildExpectedPartitionFeatureCounts(materialized.getEffectivePlan(), true);
    final long[] expectedRightCountsByPartitionId = buildExpectedPartitionFeatureCounts(materialized.getEffectivePlan(), false);
    JavaPairRDD<Integer, IFeature> leftPartitionFeatures = loadLeftBinaryFeatures(sparkContext, materialized, partitionIds);
    JavaPairRDD<Integer, IFeature> rightPartitionFeatures = loadRightBinaryFeatures(sparkContext, materialized, partitionIds);
    int[] localPhysicalPartitionIds = buildLocalPhysicalPartitionIds(partitionIds.length);
    if (hasValidPhysicalPartitionSelection(
        leftPartitionFeatures,
        rightPartitionFeatures,
        localPhysicalPartitionIds,
        partitionIds)) {
      PartitionTaskExecutionSummary compactSummary =
          CompactPartitionJobExecution$.MODULE$.executeSelectedPartitionsForLocalJoinTiming(
              this,
              partitioner,
              useReferencePointDuplicateAvoidance,
              jobGroup.getJobId(),
              leftPartitionFeatures,
              rightPartitionFeatures,
              localPhysicalPartitionIds,
              partitionIds,
              expectedLeftCountsByPartitionId,
              expectedRightCountsByPartitionId,
              "correlation-materialized");
      return buildJobExecutionResult(jobGroup, compactSummary);
    }
    return executePartitionJobWithLocalJoinTiming(
        sparkContext,
        partitioner,
        useReferencePointDuplicateAvoidance,
        jobGroup,
        leftPartitionFeatures,
        rightPartitionFeatures,
        "correlation-materialized",
        expectedLeftCountsByPartitionId,
        expectedRightCountsByPartitionId);
  }

  private JobExecutionResult executePartitionJobWithLocalJoinTiming(JavaSparkContext sparkContext,
                                                                    SpatialPartitioner partitioner,
                                                                    boolean useReferencePointDuplicateAvoidance,
                                                                    JobGroup jobGroup,
                                                                    JavaPairRDD<Integer, IFeature> leftPartitionFeatures,
                                                                    JavaPairRDD<Integer, IFeature> rightPartitionFeatures,
                                                                    String executionSource,
                                                                    long[] expectedLeftCountsByPartitionId,
                                                                    long[] expectedRightCountsByPartitionId) {
    if (sparkContext == null || partitioner == null || jobGroup == null
        || leftPartitionFeatures == null || rightPartitionFeatures == null) {
      return new JobExecutionResult(-1, null, new int[0], 0L, 0L, 0L, 0L);
    }
    LongAccumulator localJoinNanos =
        sparkContext.sc().longAccumulator("sj.localJoinNanos.correlation." + jobGroup.getJobId());
    LongAccumulator planeSweepXAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepX.correlation", jobGroup.getJobId());
    LongAccumulator planeSweepYAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepY.correlation", jobGroup.getJobId());
    LongAccumulator planeSweepDiagonalAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepDiagonal.correlation", jobGroup.getJobId());
    JavaPairRDD<Integer, Tuple2<Iterable<IFeature>, Iterable<IFeature>>> grouped =
        leftPartitionFeatures.cogroup(rightPartitionFeatures, partitioner);
    long matchCount = grouped
        .map(group -> executePartitionJoin(
            partitioner,
            group._1,
            group._2._1,
            group._2._2,
            useReferencePointDuplicateAvoidance,
            localJoinNanos,
            null,
            planeSweepXAxisCalls,
            planeSweepYAxisCalls,
            planeSweepDiagonalAxisCalls,
            executionSource,
            jobGroup.getJobId(),
            expectedLeftCountsByPartitionId,
            expectedRightCountsByPartitionId))
        .fold(0L, Long::sum);
    return buildJobExecutionResult(
        jobGroup,
        new PartitionTaskExecutionSummary(
            matchCount,
            localJoinNanos.value(),
            planeSweepXAxisCalls.value(),
            planeSweepYAxisCalls.value(),
            planeSweepDiagonalAxisCalls.value()));
  }

  /**
   * Executes one planned job from already loaded in-memory {@code (partitionId, feature)} inputs.
   *
   * <p>This overload is intended for compute-only debug experiments that want to exclude
   * partition-file reload time from the measured query phase while still using the same
   * partition-local join kernel as the main execution path.
   *
   * <p>It is intentionally kept out of the normal concurrent production/debug dispatch path.
   * Preloading multiple jobs at once was found to stack resident job inputs on top of
   * shuffle/Kryo buffers and cause executor OOMs, so the concurrent baseline continues to
   * use the operator's regular on-demand reload path.
   */
  public JobExecutionResult executeLoadedPartitionJob(SpatialPartitioner partitioner,
                                                      String partitionAssignmentStrategy,
                                                      JobGroup jobGroup,
                                                      JavaPairRDD<Integer, IFeature> leftPartitionFeatures,
                                                      JavaPairRDD<Integer, IFeature> rightPartitionFeatures) {
    if (jobGroup == null || partitioner == null || leftPartitionFeatures == null || rightPartitionFeatures == null) {
      return new JobExecutionResult(-1, null, new int[0], 0L, 0L, 0L, 0L);
    }
    boolean useReferencePointDuplicateAvoidance =
        usesReferencePointDuplicateAvoidance(partitionAssignmentStrategy);
    int[] partitionIds = jobGroup.getPartitionIds();
    int partitionCount = leftPartitionFeatures.rdd().partitions().length;
    final long[] expectedLeftCountsByPartitionId = buildExpectedPartitionFeatureCounts(jobGroup, partitioner.numPartitions(), true);
    final long[] expectedRightCountsByPartitionId = buildExpectedPartitionFeatureCounts(jobGroup, partitioner.numPartitions(), false);
    if (partitionCount == rightPartitionFeatures.rdd().partitions().length) {
      PartitionTaskExecutionSummary compactSummary = null;
      if (partitionCount == partitioner.numPartitions()
          && hasValidPhysicalPartitionSelection(leftPartitionFeatures, rightPartitionFeatures, partitionIds, null)) {
        compactSummary = CompactPartitionJobExecution$.MODULE$.executeSelectedPartitions(
            this,
            partitioner,
            useReferencePointDuplicateAvoidance,
            jobGroup.getJobId(),
            leftPartitionFeatures,
            rightPartitionFeatures,
            partitionIds,
            null,
            expectedLeftCountsByPartitionId,
            expectedRightCountsByPartitionId,
            "loaded");
      } else if (partitionCount == partitionIds.length) {
        int[] localPhysicalPartitionIds = buildLocalPhysicalPartitionIds(partitionIds.length);
        if (hasValidPhysicalPartitionSelection(
            leftPartitionFeatures,
            rightPartitionFeatures,
            localPhysicalPartitionIds,
            partitionIds)) {
          compactSummary = CompactPartitionJobExecution$.MODULE$.executeSelectedPartitions(
              this,
              partitioner,
              useReferencePointDuplicateAvoidance,
              jobGroup.getJobId(),
              leftPartitionFeatures,
              rightPartitionFeatures,
              localPhysicalPartitionIds,
              partitionIds,
              expectedLeftCountsByPartitionId,
              expectedRightCountsByPartitionId,
              "loaded");
        }
      }
      if (compactSummary != null)
        return buildJobExecutionResult(jobGroup, compactSummary);
    }
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(leftPartitionFeatures.context());
    LongAccumulator planeSweepXAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisX.job", jobGroup.getJobId());
    LongAccumulator planeSweepYAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisY.job", jobGroup.getJobId());
    LongAccumulator planeSweepDiagonalAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisDiagonal.job", jobGroup.getJobId());
    JavaPairRDD<Integer, Tuple2<Iterable<IFeature>, Iterable<IFeature>>> grouped =
        leftPartitionFeatures.cogroup(rightPartitionFeatures, partitioner);
    long matchCount = grouped
        .map(group -> executePartitionJoin(
            partitioner,
            group._1,
            group._2._1,
            group._2._2,
            useReferencePointDuplicateAvoidance,
            null,
            null,
            planeSweepXAxisCalls,
            planeSweepYAxisCalls,
            planeSweepDiagonalAxisCalls,
            "loaded",
            jobGroup.getJobId(),
            expectedLeftCountsByPartitionId,
            expectedRightCountsByPartitionId))
        .fold(0L, Long::sum);
    return buildJobExecutionResult(
        jobGroup,
        new PartitionTaskExecutionSummary(
            matchCount,
            planeSweepXAxisCalls.value(),
            planeSweepYAxisCalls.value(),
            planeSweepDiagonalAxisCalls.value()));
  }

  /**
   * Debug-only execution path that materializes the current job inputs and reports a coarse timing breakdown.
   *
   * <p>The breakdown separates:
   * - left working-set reload/materialization
   * - right working-set reload/materialization
   * - grouped join stage
   * - aggregated partition-local join time inside executors
   *
   * <p>This method intentionally adds extra actions ({@code count}) and caching to expose where time is spent.
   * It should therefore only be enabled in local or diagnostic runs.
   */
  private JobExecutionResult executePartitionJobWithProfiling(JavaSparkContext sparkContext,
                                                              SpatialPartitioner partitioner,
                                                              boolean useReferencePointDuplicateAvoidance,
                                                              JobGroup jobGroup,
                                                              JavaPairRDD<Integer, IFeature> leftPartitionFeatures,
                                                              JavaPairRDD<Integer, IFeature> rightPartitionFeatures) {
    JavaPairRDD<Integer, IFeature> cachedLeft = leftPartitionFeatures.persist(StorageLevel.MEMORY_ONLY_SER());
    JavaPairRDD<Integer, IFeature> cachedRight = rightPartitionFeatures.persist(StorageLevel.MEMORY_ONLY_SER());
    final long[] expectedLeftCountsByPartitionId = buildExpectedPartitionFeatureCounts(jobGroup, partitioner.numPartitions(), true);
    final long[] expectedRightCountsByPartitionId = buildExpectedPartitionFeatureCounts(jobGroup, partitioner.numPartitions(), false);
    try {
      long leftStartNanos = System.nanoTime();
      long leftAssignedPairCount = cachedLeft.count();
      long leftReloadMillis = (System.nanoTime() - leftStartNanos) / 1_000_000L;

      long rightStartNanos = System.nanoTime();
      long rightAssignedPairCount = cachedRight.count();
      long rightReloadMillis = (System.nanoTime() - rightStartNanos) / 1_000_000L;

      LongAccumulator localJoinNanos = sparkContext.sc().longAccumulator("sj.localJoinNanos.job." + jobGroup.getJobId());
      LongAccumulator partitionJoinCalls = sparkContext.sc().longAccumulator("sj.partitionJoinCalls.job." + jobGroup.getJobId());
      LongAccumulator planeSweepXAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisX.job", jobGroup.getJobId());
      LongAccumulator planeSweepYAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisY.job", jobGroup.getJobId());
      LongAccumulator planeSweepDiagonalAxisCalls = createJobAccumulator(sparkContext, "sj.planeSweepAxisDiagonal.job", jobGroup.getJobId());

      long joinStageStartNanos = System.nanoTime();
      JavaPairRDD<Integer, Tuple2<Iterable<IFeature>, Iterable<IFeature>>> grouped =
          cachedLeft.cogroup(cachedRight, partitioner);
      long matchCount = grouped
          .map(group -> executePartitionJoin(
              partitioner,
              group._1,
              group._2._1,
              group._2._2,
              useReferencePointDuplicateAvoidance,
              localJoinNanos,
              partitionJoinCalls,
              planeSweepXAxisCalls,
              planeSweepYAxisCalls,
              planeSweepDiagonalAxisCalls,
              "profiled",
              jobGroup.getJobId(),
              expectedLeftCountsByPartitionId,
              expectedRightCountsByPartitionId))
          .fold(0L, Long::sum);
      long joinStageMillis = (System.nanoTime() - joinStageStartNanos) / 1_000_000L;
      long localJoinMillis = localJoinNanos.value() / 1_000_000L;
      long shuffleAndGroupApproxMillis = Math.max(0L, joinStageMillis - localJoinMillis);

      System.out.println(String.format(
          "[Sequential job detail] jobId=%d, leftReload=%d ms (%d pairs), rightReload=%d ms (%d pairs), joinStage=%d ms, localJoinSum=%d ms, shuffleAndGroupApprox=%d ms, partitionJoinCalls=%d",
          jobGroup.getJobId(),
          leftReloadMillis,
          leftAssignedPairCount,
          rightReloadMillis,
          rightAssignedPairCount,
          joinStageMillis,
          localJoinMillis,
          shuffleAndGroupApproxMillis,
          partitionJoinCalls.value()));

      long leftFeatureCount = 0L;
      long rightFeatureCount = 0L;
      for (PartCost partition : jobGroup.getPartitions()) {
        leftFeatureCount += partition.getProfile().getLeftFeatureCount();
        rightFeatureCount += partition.getProfile().getRightFeatureCount();
      }
      return new JobExecutionResult(
          jobGroup.getJobId(),
          jobGroup.getComplexityType(),
          jobGroup.getPartitionIds(),
          leftFeatureCount,
          rightFeatureCount,
          jobGroup.getTotalMemoryLoadBytes(),
          matchCount,
          localJoinNanos.value(),
          planeSweepXAxisCalls.value(),
          planeSweepYAxisCalls.value(),
          planeSweepDiagonalAxisCalls.value());
    } finally {
      cachedLeft.unpersist(true);
      cachedRight.unpersist(true);
    }
  }

  public SpatialJoinResult execute(JavaSparkContext sparkContext) {
    long leftCount = countLeft(sparkContext);
    long rightCount = countRight(sparkContext);
    long matchCount = executeLocal(new Configuration()).getMatchCount();
    return SpatialJoinResult.basic(
        getLeftInputPath(),
        getRightInputPath(),
        getLeftInputFormat(),
        getRightInputFormat(),
        getJoinPredicate(),
        getJoinAlgorithm(),
        leftCount,
        rightCount,
        matchCount);
  }

  /**
   * Executes the current local preview implementation of spatial join.
   *
   * <p>Both inputs are read locally, projected to lightweight join-time features that
   * retain only serialized geometry and an OID attribute, and then joined with the existing
   * {@link SpatialJoinAlgorithms} helper methods. When
   * {@code sj.join.simplification.enabled=true}, the preview path quad-splits
   * complex geometries before filtering; otherwise it keeps the binary MBR
   * filter-refine path.
   *
   * @param conf optional Hadoop configuration for local file access
   * @return join execution summary
   */
  public SpatialJoinResult executeLocal(Configuration conf) {
    ensureSupportedJoinConfiguration();
    Configuration effectiveConf = conf == null ? new Configuration() : conf;
    List<IFeature> leftFeatures = readLeftJoinReady(effectiveConf);
    List<IFeature> rightFeatures = readRightJoinReady(effectiveConf);
    long matchCount =
        isJoinSimplificationEnabled()
            ? SpatialJoinAlgorithms.spatialJoinIntersectsWithSimplification2(
                leftFeatures,
                rightFeatures,
                getSimplificationThreshold(),
                null,
                null,
                getPlaneSweepAxis(),
                getPlaneSweepImplementation())
            : SpatialJoinAlgorithms.spatialJoinIntersectsBinaryFilterRefine(
                leftFeatures,
                rightFeatures,
                null,
                null,
                getPlaneSweepAxis(),
                getPlaneSweepImplementation());
    return SpatialJoinResult.basic(
        getLeftInputPath(),
        getRightInputPath(),
        getLeftInputFormat(),
        getRightInputFormat(),
        getJoinPredicate(),
        getJoinAlgorithm(),
        leftFeatures.size(),
        rightFeatures.size(),
        matchCount);
  }

  /**
   * Reads and projects the left dataset to the compact join-time representation.
   *
   * <p>The resulting features keep only the serialized geometry field and a lightweight OID.
   * Geometries therefore stay in binary form until the join algorithm explicitly accesses them.
   *
   * @param conf optional Hadoop configuration
   * @return left-side join-ready features
   */
  List<IFeature> readLeftJoinReady(Configuration conf) {
    return materializeJoinReady(leftReader().readLocalJ(conf));
  }

  /**
   * Reads and projects the right dataset to the compact join-time representation.
   *
   * @param conf optional Hadoop configuration
   * @return right-side join-ready features
   */
  List<IFeature> readRightJoinReady(Configuration conf) {
    return materializeJoinReady(rightReader().readLocalJ(conf));
  }

  /**
   * Validates that the requested predicate/algorithm combination is supported by the
   * current preview implementation.
   */
  private void ensureSupportedJoinConfiguration() {
    if (!"intersects".equalsIgnoreCase(getJoinPredicate())) {
      throw new UnsupportedOperationException("Only intersects predicate is implemented in the current operator");
    }
    if (!"local-plane-sweep".equalsIgnoreCase(getJoinAlgorithm())) {
      throw new UnsupportedOperationException("Only local-plane-sweep algorithm is implemented in the current operator");
    }
  }

  private JavaPairRDD<Integer, IFeature> loadBinaryFeatures(JavaRDD<IFeature> features,
                                                            List<SpatialPartitioner.FeatureRef> assignments,
                                                            int[] partitionIds) {
    return SpatialPartitioner.loadAssignedBinaryFeatures(features, assignments, partitionIds);
  }

  /**
   * Reloads partition-local binary features from a precomputed OID-to-feature mapping.
   *
   * This helper keeps later partition-local jobs on top of cached RDDs instead of rebuilding
   * the original input lineage and reassigning fresh unique ids on every load.
   */
  private JavaPairRDD<Integer, IFeature> loadBinaryFeatures(JavaPairRDD<Long, IFeature> featuresByOid,
                                                            List<SpatialPartitioner.FeatureRef> assignments,
                                                            int[] partitionIds) {
    return SpatialPartitioner.loadAssignedBinaryFeatures(featuresByOid, assignments, partitionIds);
  }

  /**
   * Reloads binary features from disk-hybrid partition files.
   *
   * <p>Each effective partition is materialized under its own path, so a later job can load only the paths
   * matching its {@code partitionIds} instead of scanning the full source input again.
   *
   * @param sparkContext active Spark context used to parallelize the target partition ids and read files.
   * @param materializationRoot root directory that stores one partition file tree per side.
   * @param side either {@code left} or {@code right}; selects which subtree to read from.
   * @param partitionIds the effective partition ids required by the current job.
   * @return one pair RDD of {@code (partitionId, feature)} containing only the requested partitions.
   */
  @SuppressWarnings("unchecked")
  private JavaPairRDD<Integer, IFeature> loadMaterializedBinaryFeatures(JavaSparkContext sparkContext,
                                                                        String materializationRoot,
                                                                        String side,
                                                                        int[] partitionIds) {
    if (sparkContext == null || materializationRoot == null || materializationRoot.trim().isEmpty()
        || partitionIds == null || partitionIds.length == 0) {
      return sparkContext == null ? JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate())
          .parallelizePairs(new ArrayList<>()) : sparkContext.parallelizePairs(new ArrayList<>());
    }
    List<Integer> targetPartitionIds = new ArrayList<>(partitionIds.length);
    for (int partitionId : partitionIds)
      targetPartitionIds.add(partitionId);
    final int[] requestedPartitionIds = Arrays.copyOf(partitionIds, partitionIds.length);
    JavaRDD<Tuple2<Integer, IFeature>> loadedPartitions = sparkContext
        .parallelize(targetPartitionIds, Math.max(1, targetPartitionIds.size()))
        .mapPartitionsWithIndex(
            (Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, IFeature>>>) (physicalPartitionId, ignored) -> {
              if (physicalPartitionId == null || physicalPartitionId < 0 || physicalPartitionId >= requestedPartitionIds.length) {
                return Collections.emptyIterator();
              }
              int partitionId = requestedPartitionIds[physicalPartitionId];
              try {
                return streamMaterializedPartitionFile(new Configuration(), materializationRoot, side, partitionId);
              } catch (IOException e) {
                throw new RuntimeException("Failed to read materialized partition file for partition " + partitionId, e);
              }
            },
            true);
    return JavaPairRDD.fromJavaRDD(loadedPartitions);
  }

  private SpatialPartitioner.EffectivePlan applyPartitionIntensityMode(
      SpatialPartitioner.EffectivePlan effectivePlan) {
    if (effectivePlan == null || !usesDerivedPartitionIntensity())
      return effectivePlan;
    List<PartProfile> profiles = effectivePlan.getPartitionProfiles();
    if (profiles == null || profiles.isEmpty())
      return effectivePlan;
    long startNanos = System.nanoTime();
    try {
      return derivePartitionIntensity(effectivePlan, profiles);
    } finally {
      lastPreparationIntensityElapsedNanos += System.nanoTime() - startNanos;
    }
  }

  private SpatialPartitioner.EffectivePlan derivePartitionIntensity(
      SpatialPartitioner.EffectivePlan effectivePlan,
      List<PartProfile> profiles) {
    List<PartProfile> updatedProfiles = new ArrayList<>(profiles.size());
    if (usesCiHybridPartitionIntensity()) {
      long globalMinVertexCount = Long.MAX_VALUE;
      long globalMaxVertexCount = Long.MIN_VALUE;
      for (PartProfile profile : profiles) {
        if (profile == null)
          continue;
        if (profile.getLeftFeatureCount() > 0L) {
          globalMinVertexCount = Math.min(globalMinVertexCount, Math.max(0L, profile.getLeftMinVertexCount()));
          globalMaxVertexCount = Math.max(globalMaxVertexCount, Math.max(0L, profile.getLeftMaxVertexCount()));
        }
        if (profile.getRightFeatureCount() > 0L) {
          globalMinVertexCount = Math.min(globalMinVertexCount, Math.max(0L, profile.getRightMinVertexCount()));
          globalMaxVertexCount = Math.max(globalMaxVertexCount, Math.max(0L, profile.getRightMaxVertexCount()));
        }
      }
      if (globalMinVertexCount == Long.MAX_VALUE || globalMaxVertexCount == Long.MIN_VALUE) {
        globalMinVertexCount = 0L;
        globalMaxVertexCount = 0L;
      }
      double minFeatureCount = Double.POSITIVE_INFINITY;
      double maxFeatureCount = Double.NEGATIVE_INFINITY;
      double minCandidateAverageVertexIntensity = Double.POSITIVE_INFINITY;
      double maxCandidateAverageVertexIntensity = Double.NEGATIVE_INFINITY;
      for (PartProfile profile : profiles) {
        if (profile == null)
          continue;
        double featureCount = Math.max(0L, profile.getTotalFeatureCount());
        double candidateAverageVertexIntensity = candidateAverageVertexIntensity(
            profile,
            globalMinVertexCount,
            globalMaxVertexCount);
        minFeatureCount = Math.min(minFeatureCount, featureCount);
        maxFeatureCount = Math.max(maxFeatureCount, featureCount);
        minCandidateAverageVertexIntensity = Math.min(minCandidateAverageVertexIntensity, candidateAverageVertexIntensity);
        maxCandidateAverageVertexIntensity = Math.max(maxCandidateAverageVertexIntensity, candidateAverageVertexIntensity);
      }
      double alpha = getPartitionIntensityCiAlpha();
      for (PartProfile profile : profiles) {
        if (profile == null)
          continue;
        long candidatePairs = profile.getCibCandidatePairCount();
        double normalizedFeatureCount = minMaxNormalize(
            Math.max(0L, profile.getTotalFeatureCount()),
            minFeatureCount,
            maxFeatureCount);
        double normalizedCandidateAverage = minMaxNormalize(
            candidateAverageVertexIntensity(profile, globalMinVertexCount, globalMaxVertexCount),
            minCandidateAverageVertexIntensity,
            maxCandidateAverageVertexIntensity);
        double ci = clamp01(alpha * normalizedFeatureCount + (1.0d - alpha) * normalizedCandidateAverage);
        updatedProfiles.add(profile.withCandidatePairAwareIntensity(candidatePairs, ci));
      }
    } else {
      for (PartProfile profile : profiles) {
        if (profile == null)
          continue;
        long candidatePairs = profile.getCibCandidatePairCount();
        updatedProfiles.add(profile.withCandidatePairAwareIntensity(
            candidatePairs,
            candidateAverageComplexityIntensity(profile)));
      }
    }
    return new SpatialPartitioner.EffectivePlan(
        effectivePlan.getPartitioner(),
        updatedProfiles,
        effectivePlan.getLeftAssignments(),
        effectivePlan.getRightAssignments());
  }

  private static double candidateAverageComplexityIntensity(PartProfile profile) {
    if (profile == null)
      return 0.0d;
    // CIB candidatePairs(P) is an estimate used for lightweight planning,
    // not an exact MBR-filtered candidate enumeration.
    long candidatePairs = Math.max(0L, profile.getCibCandidatePairCount());
    if (candidatePairs <= 0L)
      return 0.0d;
    double leftAverageComplexity =
        profile.getLeftFeatureCount() <= 0L ? 0.0d : profile.getLeftComplexity() / profile.getLeftFeatureCount();
    double rightAverageComplexity =
        profile.getRightFeatureCount() <= 0L ? 0.0d : profile.getRightComplexity() / profile.getRightFeatureCount();
    return candidatePairs * Math.log1p(Math.max(0.0d, leftAverageComplexity + rightAverageComplexity));
  }

  private static double candidateAverageVertexIntensity(PartProfile profile,
                                                        long minVertexCount,
                                                        long maxVertexCount) {
    if (profile == null)
      return 0.0d;
    long candidatePairs = Math.max(0L, profile.getCibCandidatePairCount());
    if (candidatePairs <= 0L)
      return 0.0d;
    double leftVertexComplexity = normalizedVertexComplexity(
        profile.getLeftVertexCount(),
        profile.getLeftFeatureCount(),
        minVertexCount,
        maxVertexCount);
    double rightVertexComplexity = normalizedVertexComplexity(
        profile.getRightVertexCount(),
        profile.getRightFeatureCount(),
        minVertexCount,
        maxVertexCount);
    double leftAverage =
        profile.getLeftFeatureCount() <= 0L ? 0.0d : leftVertexComplexity / profile.getLeftFeatureCount();
    double rightAverage =
        profile.getRightFeatureCount() <= 0L ? 0.0d : rightVertexComplexity / profile.getRightFeatureCount();
    return candidatePairs * Math.log1p(Math.max(0.0d, leftAverage + rightAverage));
  }

  private static double normalizedVertexComplexity(long totalVertexCount,
                                                   long featureCount,
                                                   long minVertexCount,
                                                   long maxVertexCount) {
    if (featureCount <= 0L || maxVertexCount <= minVertexCount)
      return 0.0d;
    double denominator = (double) maxVertexCount - (double) minVertexCount;
    double score = ((double) Math.max(0L, totalVertexCount) - (double) featureCount * (double) minVertexCount)
        / denominator;
    return Math.max(0.0d, score);
  }

  private static double minMaxNormalize(double value, double min, double max) {
    if (!Double.isFinite(value) || !Double.isFinite(min) || !Double.isFinite(max))
      return 0.0d;
    double denominator = max - min;
    if (!Double.isFinite(denominator) || denominator <= 0.0d)
      return 0.0d;
    return clamp01((value - min) / denominator);
  }

  /**
   * Writes one prepared in-memory execution context to the disk-hybrid external layout.
   *
   * <p>The method:
   * <ol>
   *   <li>reloads all effective left/right partition data from the prepared context,</li>
   *   <li>creates a fresh materialization root,</li>
   *   <li>writes metadata files describing the effective partitions and assignments,</li>
   *   <li>writes one Kryo data file per effective partition for each side, and</li>
   *   <li>optionally validates that reloading those files reproduces the in-memory partition contents.</li>
   * </ol>
   *
   * @param sparkContext active Spark context used for writing and optional validation.
   * @param prepared prepared in-memory context to convert into a disk-backed working set.
   * @return disk-backed execution context that points at the new materialization root.
   */
  private MaterializedPartitionExecution materializePreparedPartitionExecution(JavaSparkContext sparkContext,
                                                                               PreparedPartitionExecution prepared) {
    if (prepared == null || prepared.getEffectivePlan() == null) {
      return new MaterializedPartitionExecution(
          sparkContext,
          prepared == null ? null : prepared.getLeftSummary(),
          prepared == null ? null : prepared.getRightSummary(),
          prepared == null ? 0L : prepared.getLeftFeatureCount(),
          prepared == null ? 0L : prepared.getRightFeatureCount(),
          prepared == null ? null : prepared.getEffectivePlan(),
          null,
          null,
          isKeepMaterializedPartitions(),
          prepared == null ? getPartitionAssignmentStrategy() : prepared.getPartitionAssignmentStrategy(),
          prepared == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : prepared.getAnchorSide());
    }
    int[] effectivePartitionIds = prepared.getEffectivePlan().getPartitionProfiles().stream()
        .mapToInt(PartProfile::getPartitionId)
        .toArray();
    JavaPairRDD<Integer, IFeature> leftPartitionFeatures = prepared.getLeftPartitionedFeatures() == null
        ? loadLeftBinaryFeatures(prepared, effectivePartitionIds).persist(StorageLevel.MEMORY_ONLY_SER())
        : prepared.getLeftPartitionedFeatures();
    JavaPairRDD<Integer, IFeature> rightPartitionFeatures = prepared.getRightPartitionedFeatures() == null
        ? loadRightBinaryFeatures(prepared, effectivePartitionIds).persist(StorageLevel.MEMORY_ONLY_SER())
        : prepared.getRightPartitionedFeatures();
    boolean ownsLeftPartitionFeatures = prepared.getLeftPartitionedFeatures() == null;
    boolean ownsRightPartitionFeatures = prepared.getRightPartitionedFeatures() == null;
    String materializationRoot = resolveMaterializationRoot(sparkContext);
    try {
      deletePathIfExists(sparkContext, materializationRoot);
      writeMaterializationMetadata(sparkContext, prepared, materializationRoot);
      SpatialPartitioner finalPartitioner = prepared.getEffectivePlan().getPartitioner();
      materializePartitionSide(leftPartitionFeatures, finalPartitioner, materializationRoot, "left", effectivePartitionIds);
      materializePartitionSide(rightPartitionFeatures, finalPartitioner, materializationRoot, "right", effectivePartitionIds);
      MaterializedPartitionValidation validation = !isValidateMaterializedPartitions()
          ? null
          : validateMaterializedPartitions(
              sparkContext,
              materializationRoot,
              effectivePartitionIds,
              leftPartitionFeatures,
              rightPartitionFeatures);
      SpatialPartitioner.EffectivePlan strippedEffectivePlan = stripAssignments(prepared.getEffectivePlan());
      return new MaterializedPartitionExecution(
          sparkContext,
          prepared.getLeftSummary(),
          prepared.getRightSummary(),
          prepared.getLeftFeatureCount(),
          prepared.getRightFeatureCount(),
          strippedEffectivePlan,
          materializationRoot,
          validation,
          isKeepMaterializedPartitions(),
          prepared.getPartitionAssignmentStrategy(),
          prepared.getAnchorSide());
    } finally {
      if (ownsLeftPartitionFeatures)
        leftPartitionFeatures.unpersist(true);
      if (ownsRightPartitionFeatures)
        rightPartitionFeatures.unpersist(true);
    }
  }

  /**
   * Builds and materializes the final job-ready `(partitionId, feature)` working set for one input side.
   *
   * <p>This is the highest-priority execution optimization: instead of letting every later job
   * re-join `(oid, feature)` with the compact assignment list, we perform that assignment replay
   * once during preparation, partition the result by the final effective partitioner, persist it in
   * serialized memory, and materialize it immediately. Later jobs can then prune target partitions
   * directly from this pre-partitioned working set.
   */
  private PreparedPartitionBuildResult buildPreparedPartitionFeatures(JavaPairRDD<Long, IFeature> featuresByOid,
                                                                      List<SpatialPartitioner.FeatureRef> assignments,
                                                                      int[] effectivePartitionIds,
                                                                      SpatialPartitioner partitioner) {
    return buildAssignedPartitionFeatures(
        featuresByOid,
        assignments,
        effectivePartitionIds,
        partitioner,
        PREPARED_PARTITION_STORAGE_LEVEL,
        true);
  }

  /**
   * Directly expands cached `(oid, feature)` records to all overlapping final effective partitions.
   *
   * <p>This path is used by disk-hybrid materialization for the default dual-replication strategy.
   * It avoids holding and replaying large driver-side assignment lists when the final materialized
   * files can be produced directly from the final partition envelopes.
   */
  private PreparedPartitionBuildResult buildDirectPartitionFeaturesForMaterialization(
      JavaPairRDD<Long, IFeature> featuresByOid,
      SpatialPartitioner partitioner,
      StorageLevel storageLevel,
      boolean eagerMaterialize) {
    if (featuresByOid == null || partitioner == null) {
      JavaSparkContext sparkContext = featuresByOid == null
          ? JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate())
          : JavaSparkContext.fromSparkContext(featuresByOid.context());
      return new PreparedPartitionBuildResult(sparkContext.parallelizePairs(new ArrayList<>()), null);
    }
    JavaPairRDD<Integer, IFeature> partitioned = featuresByOid
        .mapPartitionsToPair((PairFlatMapFunction<Iterator<Tuple2<Long, IFeature>>, Integer, IFeature>) iterator ->
            new DirectAssignedFeatureIterator(iterator, partitioner))
        .partitionBy(partitioner);
    if (storageLevel != null)
      partitioned = partitioned.persist(storageLevel);
    if (eagerMaterialize)
      partitioned.count();
    return new PreparedPartitionBuildResult(partitioned, null);
  }

  /**
   * Directly expands raw features to all overlapping final effective partitions without creating an
   * intermediate OID mapping. This lighter path is used by the disk-hybrid dual-replication
   * preparation chain where later execution reloads only the final partition files.
   */
  private PreparedPartitionBuildResult buildDirectPartitionFeaturesForMaterialization(
      JavaRDD<IFeature> features,
      SpatialPartitioner partitioner,
      StorageLevel storageLevel,
      boolean eagerMaterialize) {
    if (features == null || partitioner == null) {
      JavaSparkContext sparkContext = features == null
          ? JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate())
          : JavaSparkContext.fromSparkContext(features.context());
      return new PreparedPartitionBuildResult(sparkContext.parallelizePairs(new ArrayList<>()), null);
    }
    JavaPairRDD<Integer, IFeature> partitioned = features
        .mapPartitionsToPair(iterator -> new DirectAssignedRawFeatureIterator(iterator, partitioner))
        .partitionBy(partitioner);
    if (storageLevel != null)
      partitioned = partitioned.persist(storageLevel);
    if (eagerMaterialize)
      partitioned.count();
    return new PreparedPartitionBuildResult(partitioned, null);
  }

  private PreparedPartitionBuildResult buildAssignedPartitionFeatures(JavaPairRDD<Long, IFeature> featuresByOid,
                                                                      List<SpatialPartitioner.FeatureRef> assignments,
                                                                      int[] effectivePartitionIds,
                                                                      SpatialPartitioner partitioner,
                                                                      StorageLevel storageLevel,
                                                                      boolean eagerMaterialize) {
    if (featuresByOid == null || assignments == null || assignments.isEmpty()
        || effectivePartitionIds == null || effectivePartitionIds.length == 0 || partitioner == null) {
      JavaSparkContext sparkContext = featuresByOid == null
          ? JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate())
          : JavaSparkContext.fromSparkContext(featuresByOid.context());
      return new PreparedPartitionBuildResult(sparkContext.parallelizePairs(new ArrayList<>()), null);
    }
    long broadcastThreshold = getPreparedAssignmentBroadcastMaxBytes();
    long lookupUpperBound = SpatialPartitioner
        .estimateAssignmentLookupTableUpperBoundBytes(assignments.size());
    if (lookupUpperBound <= broadcastThreshold) {
      SpatialPartitioner.AssignmentLookupTable lookupTable =
          SpatialPartitioner.buildAssignmentLookupTable(assignments);
      if (!lookupTable.isEmpty() && lookupTable.getEstimatedSizeInBytes() <= broadcastThreshold) {
        return buildAssignedPartitionFeaturesViaBroadcast(
            featuresByOid,
            lookupTable,
            partitioner,
            storageLevel,
            eagerMaterialize);
      }
    }
    return buildAssignedPartitionFeaturesViaJoinReplay(
        featuresByOid,
        assignments,
        effectivePartitionIds,
        partitioner,
        storageLevel,
        eagerMaterialize);
  }

  /**
   * Broadcast-based expansion path for one `(partitionId, feature)` replay.
   *
   * <p>The caller may choose whether the resulting RDD should be cached and eagerly materialized.
   * Disk-hybrid preparation uses the same replay mechanism as memory mode but skips the final
   * in-memory cache when validation is disabled, which keeps the materialization pass lighter.
   */
  private PreparedPartitionBuildResult buildAssignedPartitionFeaturesViaBroadcast(
      JavaPairRDD<Long, IFeature> featuresByOid,
      SpatialPartitioner.AssignmentLookupTable lookupTable,
      SpatialPartitioner partitioner,
      StorageLevel storageLevel,
      boolean eagerMaterialize) {
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(featuresByOid.context());
    Broadcast<SpatialPartitioner.AssignmentLookupTable> lookupBroadcast = sparkContext.broadcast(lookupTable);
    JavaPairRDD<Integer, IFeature> partitioned = featuresByOid
        .mapPartitionsToPair((PairFlatMapFunction<Iterator<Tuple2<Long, IFeature>>, Integer, IFeature>) iterator ->
            new BroadcastAssignedFeatureIterator(iterator, lookupBroadcast.value()))
        .partitionBy(partitioner);
    if (storageLevel != null)
      partitioned = partitioned.persist(storageLevel);
    if (eagerMaterialize)
      partitioned.count();
    return new PreparedPartitionBuildResult(partitioned, lookupBroadcast);
  }

  /**
   * Fallback replay path that recreates `(partitionId, feature)` via an OID join.
   *
   * <p>This remains available for unusually large assignment tables whose compact broadcast index
   * would exceed the configured short-lived memory budget.
   */
  private PreparedPartitionBuildResult buildAssignedPartitionFeaturesViaJoinReplay(
      JavaPairRDD<Long, IFeature> featuresByOid,
      List<SpatialPartitioner.FeatureRef> assignments,
      int[] effectivePartitionIds,
      SpatialPartitioner partitioner,
      StorageLevel storageLevel,
      boolean eagerMaterialize) {
    JavaPairRDD<Integer, IFeature> partitioned = SpatialPartitioner
        .loadAssignedBinaryFeatures(featuresByOid, assignments, effectivePartitionIds)
        .partitionBy(partitioner);
    if (storageLevel != null)
      partitioned = partitioned.persist(storageLevel);
    if (eagerMaterialize)
      partitioned.count();
    return new PreparedPartitionBuildResult(partitioned, null);
  }

  /**
   * Broadcast-based materialization path for the final job-ready `(partitionId, feature)` working set.
   *
   * <p>The effective plan already stores the compact assignment list on the driver. This method
   * repacks that list into a tighter primitive-array lookup table, broadcasts it only for the
   * one-shot materialization pass, and lets each executor expand cached `(oid, feature)` records
   * directly to all assigned partitions without performing another distributed OID join.
   */
  private PreparedPartitionBuildResult buildPreparedPartitionFeaturesViaBroadcast(
      JavaPairRDD<Long, IFeature> featuresByOid,
      SpatialPartitioner.AssignmentLookupTable lookupTable,
      SpatialPartitioner partitioner) {
    return buildAssignedPartitionFeaturesViaBroadcast(
        featuresByOid,
        lookupTable,
        partitioner,
        PREPARED_PARTITION_STORAGE_LEVEL,
        true);
  }

  /**
   * Fallback path that recreates `(partitionId, feature)` via an OID join.
   *
   * <p>This remains available for unusually large assignment tables whose compact broadcast index
   * would exceed the configured short-lived memory budget.
   */
  private PreparedPartitionBuildResult buildPreparedPartitionFeaturesViaJoinReplay(
      JavaPairRDD<Long, IFeature> featuresByOid,
      List<SpatialPartitioner.FeatureRef> assignments,
      int[] effectivePartitionIds,
      SpatialPartitioner partitioner) {
    return buildAssignedPartitionFeaturesViaJoinReplay(
        featuresByOid,
        assignments,
        effectivePartitionIds,
        partitioner,
        PREPARED_PARTITION_STORAGE_LEVEL,
        true);
  }

  private void materializePartitionSide(JavaPairRDD<Integer, IFeature> partitionFeatures,
                                        SpatialPartitioner partitioner,
                                        String materializationRoot,
                                        String side,
                                        int[] partitionIds) {
    if (partitionFeatures == null || partitionIds == null || partitioner == null)
      return;
    partitionFeatures
        .mapPartitionsWithIndex((Function2<Integer, Iterator<Tuple2<Integer, IFeature>>, Iterator<Integer>>) (partitionId, iterator) -> {
          try {
            writeMaterializedPartitionFile(new Configuration(), materializationRoot, side, partitionId, iterator);
          } catch (IOException e) {
            throw new RuntimeException("Failed to materialize partition " + partitionId + " for side " + side, e);
          }
          return Collections.singletonList(partitionId).iterator();
        }, true)
        .count();
  }

  private CandidatePartitionMaterializationStats materializeCandidatePartitionSideAndCollectStats(
      JavaRDD<IFeature> features,
      SpatialPartitioner candidatePartitioner,
      String materializationRoot,
      String side,
      String complexityMode,
      String planningLoadEstimateMode,
      String refinementLoadEstimateMode) {
    if (features == null || candidatePartitioner == null) {
      int partitionCount = candidatePartitioner == null ? 0 : candidatePartitioner.numPartitions();
      return new CandidatePartitionMaterializationStats(
          SpatialPartitioner.PartitionLoadStats.empty(partitionCount),
          SpatialPartitioner.PartitionRefinementStats.empty(
              partitionCount,
              DISK_HYBRID_EXACT_REFINEMENT_HISTOGRAM_BINS));
    }
    Map<Integer, MaterializedPartitionStats> aggregated = features
        .mapPartitionsToPair(iterator -> new DirectAssignedRawFeatureIterator(iterator, candidatePartitioner))
        .partitionBy(candidatePartitioner)
        .mapPartitionsWithIndex((Function2<Integer, Iterator<Tuple2<Integer, IFeature>>, Iterator<Tuple2<Integer, MaterializedPartitionStats>>>) (partitionId, iterator) -> {
          try {
            MaterializedPartitionStats stats = writeCandidateMaterializedPartitionFile(
                new Configuration(),
                materializationRoot,
                side,
                partitionId,
                candidatePartitioner.getPartitionMBR(partitionId),
                iterator,
                complexityMode,
                planningLoadEstimateMode,
                refinementLoadEstimateMode);
            return Collections.singletonList(new Tuple2<>(partitionId, stats)).iterator();
          } catch (IOException e) {
            throw new RuntimeException(
                "Failed to materialize candidate partition " + partitionId + " for side " + side,
                e);
          }
        }, true)
        .mapToPair(tuple -> tuple)
        .collectAsMap();
    int partitionCount = Math.max(0, candidatePartitioner.numPartitions());
    long[] featureCounts = new long[partitionCount];
    long[] planningTotalBytes = new long[partitionCount];
    long[] refinementTotalBytes = new long[partitionCount];
    double[] totalComplexities = new double[partitionCount];
    long[] totalVertexCounts = new long[partitionCount];
    long[] minVertexCounts = new long[partitionCount];
    long[] maxVertexCounts = new long[partitionCount];
    double[] cibCornerCounts = new double[partitionCount];
    double[] cibOverlapRatios = new double[partitionCount];
    double[] cibHorizontalRatios = new double[partitionCount];
    double[] cibVerticalRatios = new double[partitionCount];
    EnvelopeNDLite[] aggregateEnvelopes = new EnvelopeNDLite[partitionCount];
    long[][][] countHistograms =
        new long[partitionCount][2][DISK_HYBRID_EXACT_REFINEMENT_HISTOGRAM_BINS];
    long[][][] byteHistograms =
        new long[partitionCount][2][DISK_HYBRID_EXACT_REFINEMENT_HISTOGRAM_BINS];
    if (aggregated != null) {
      for (Map.Entry<Integer, MaterializedPartitionStats> entry : aggregated.entrySet()) {
        if (entry == null || entry.getKey() == null)
          continue;
        int partitionId = entry.getKey();
        if (partitionId < 0 || partitionId >= partitionCount)
          continue;
        MaterializedPartitionStats stats = entry.getValue();
        if (stats == null)
          continue;
        featureCounts[partitionId] = stats.featureCount;
        planningTotalBytes[partitionId] = stats.planningTotalBytes;
        refinementTotalBytes[partitionId] = stats.refinementTotalBytes;
        totalComplexities[partitionId] = stats.totalComplexity;
        totalVertexCounts[partitionId] = stats.totalVertexCount;
        minVertexCounts[partitionId] = stats.minVertexCount;
        maxVertexCounts[partitionId] = stats.maxVertexCount;
        cibCornerCounts[partitionId] = stats.cibStats.getCornerCount();
        cibOverlapRatios[partitionId] = stats.cibStats.getOverlapRatio();
        cibHorizontalRatios[partitionId] = stats.cibStats.getHorizontalRatio();
        cibVerticalRatios[partitionId] = stats.cibStats.getVerticalRatio();
        aggregateEnvelopes[partitionId] =
            stats.aggregateEnvelope == null ? null : new EnvelopeNDLite(stats.aggregateEnvelope);
        copyMaterializedHistograms(stats.countHistograms, countHistograms[partitionId]);
        copyMaterializedHistograms(stats.byteHistograms, byteHistograms[partitionId]);
      }
    }
    return new CandidatePartitionMaterializationStats(
        SpatialPartitioner.PartitionLoadStats.of(
            featureCounts,
            planningTotalBytes,
            totalComplexities,
            totalVertexCounts,
            minVertexCounts,
            maxVertexCounts,
            cibCornerCounts,
            cibOverlapRatios,
            cibHorizontalRatios,
            cibVerticalRatios,
            aggregateEnvelopes),
        SpatialPartitioner.PartitionRefinementStats.of(
            featureCounts,
            refinementTotalBytes,
            aggregateEnvelopes,
            countHistograms,
            byteHistograms,
            DISK_HYBRID_EXACT_REFINEMENT_HISTOGRAM_BINS));
  }

  private static void copyMaterializedHistograms(long[][] source, long[][] target) {
    if (source == null || target == null)
      return;
    for (int axis = 0; axis < Math.min(source.length, target.length); axis++) {
      if (source[axis] == null || target[axis] == null)
        continue;
      System.arraycopy(
          source[axis],
          0,
          target[axis],
          0,
          Math.min(source[axis].length, target[axis].length));
    }
  }

  private static long[] combineRetainedPartitionLoads(
      SpatialPartitioner.PartitionRefinementStats leftStats,
      SpatialPartitioner.PartitionRefinementStats rightStats,
      int[] retainedPartitionIds) {
    if (retainedPartitionIds == null || retainedPartitionIds.length == 0)
      return new long[0];
    long[] leftBytes = leftStats == null ? new long[0] : leftStats.getTotalBytes();
    long[] rightBytes = rightStats == null ? new long[0] : rightStats.getTotalBytes();
    long[] combined = new long[retainedPartitionIds.length];
    for (int i = 0; i < retainedPartitionIds.length; i++) {
      int partitionId = retainedPartitionIds[i];
      long left = partitionId >= 0 && partitionId < leftBytes.length ? Math.max(0L, leftBytes[partitionId]) : 0L;
      long right = partitionId >= 0 && partitionId < rightBytes.length ? Math.max(0L, rightBytes[partitionId]) : 0L;
      combined[i] = left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
    return combined;
  }

  private static String candidateMaterializationSide(String side) {
    return side == null ? "candidate" : side + "-candidate";
  }

  private static MaterializedPartitionStats writeCandidateMaterializedPartitionFile(
      Configuration configuration,
      String materializationRoot,
      String side,
      int partitionId,
      EnvelopeNDLite partitionEnvelope,
      Iterator<Tuple2<Integer, IFeature>> records,
      String complexityMode,
      String planningLoadEstimateMode,
      String refinementLoadEstimateMode) throws IOException {
    if (configuration == null || materializationRoot == null || side == null)
      return new MaterializedPartitionStats();
    String candidateSide = candidateMaterializationSide(side);
    Path partitionDir = new Path(partitionDataPath(materializationRoot, candidateSide, partitionId));
    Path partitionFile = new Path(partitionDataFilePath(materializationRoot, candidateSide, partitionId));
    FileSystem fileSystem = partitionFile.getFileSystem(configuration);
    if (fileSystem.exists(partitionFile))
      fileSystem.delete(partitionFile, false);
    fileSystem.mkdirs(partitionDir);
    Kryo kryo = createKryo();
    boolean wroteAny = false;
    MaterializedPartitionStats stats = new MaterializedPartitionStats();
    try (Output output = new Output(fileSystem.create(partitionFile, true))) {
      while (records.hasNext()) {
        Tuple2<Integer, IFeature> next = records.next();
        if (next == null || next._2 == null)
          continue;
        writeMaterializedFeatureRecord(kryo, output, next._2);
        accumulateMaterializedPartitionStats(
            stats,
            next._2,
            partitionEnvelope,
            complexityMode,
            planningLoadEstimateMode,
            refinementLoadEstimateMode);
        wroteAny = true;
      }
      output.writeByte(MATERIALIZED_RECORD_END);
    }
    if (!wroteAny && fileSystem.exists(partitionFile))
      fileSystem.delete(partitionFile, false);
    return stats;
  }

  private static void accumulateMaterializedPartitionStats(MaterializedPartitionStats stats,
                                                           IFeature feature,
                                                           EnvelopeNDLite partitionEnvelope,
                                                           String complexityMode,
                                                           String planningLoadEstimateMode,
                                                           String refinementLoadEstimateMode) {
    if (stats == null || feature == null)
      return;
    long planningLoadBytes =
        Math.max(0L, feature.getExecutionLoadEstimateBytes(planningLoadEstimateMode));
    long refinementLoadBytes =
        Math.max(0L, feature.getExecutionLoadEstimateBytes(refinementLoadEstimateMode));
    AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
        feature,
        -1L,
        feature.getStorageSize(),
        refinementLoadBytes,
        complexityMode);
    stats.featureCount++;
    long vertexCount = abstractFeature == null ? 0L : Math.max(0L, abstractFeature.getNumPoints());
    stats.planningTotalBytes += planningLoadBytes;
    stats.refinementTotalBytes +=
        abstractFeature == null
            ? refinementLoadBytes
            : Math.max(0L, abstractFeature.getLoadWeight());
    stats.totalComplexity += abstractFeature == null
        ? Math.max(0.0d, feature.getComplexityEstimate(complexityMode))
        : Math.max(0.0d, abstractFeature.getComplexityEstimate());
    stats.totalVertexCount += vertexCount;
    if (stats.featureCount == 1L || vertexCount < stats.minVertexCount)
      stats.minVertexCount = vertexCount;
    stats.maxVertexCount = Math.max(stats.maxVertexCount, vertexCount);
    if (abstractFeature != null) {
      stats.cibStats.add(abstractFeature, partitionEnvelope);
      stats.aggregateEnvelope = updateProjectedAggregateEnvelope(
          stats.aggregateEnvelope,
          abstractFeature,
          partitionEnvelope);
      accumulateMaterializedRefinementHistogram(
          stats,
          abstractFeature,
          partitionEnvelope,
          Math.max(0L, abstractFeature.getLoadWeight()));
    }
  }

  private static void accumulateMaterializedRefinementHistogram(
      MaterializedPartitionStats stats,
      AbstractFeature feature,
      EnvelopeNDLite partitionEnvelope,
      long loadBytes) {
    if (stats == null || feature == null || partitionEnvelope == null)
      return;
    int axisCount = Math.min(
        2,
        Math.min(feature.getCoordinateDimension(), partitionEnvelope.getCoordinateDimension()));
    for (int axis = 0; axis < axisCount; axis++) {
      double min = partitionEnvelope.getMinCoord(axis);
      double length = partitionEnvelope.getSideLength(axis);
      int bin = 0;
      if (length > 0.0d) {
        double center = Math.max(
            min,
            Math.min(partitionEnvelope.getMaxCoord(axis), feature.getCenter(axis)));
        bin = (int) Math.floor(
            ((center - min) / length) * stats.countHistograms[axis].length);
        bin = Math.max(0, Math.min(stats.countHistograms[axis].length - 1, bin));
      }
      stats.countHistograms[axis][bin]++;
      stats.byteHistograms[axis][bin] += loadBytes;
    }
  }

  private static EnvelopeNDLite updateProjectedAggregateEnvelope(EnvelopeNDLite aggregateEnvelope,
                                                                 EnvelopeNDLite featureEnvelope,
                                                                 EnvelopeNDLite partitionEnvelope) {
    if (featureEnvelope == null
        || partitionEnvelope == null
        || featureEnvelope.getCoordinateDimension() < 2
        || partitionEnvelope.getCoordinateDimension() < 2)
      return aggregateEnvelope;
    double minX = Math.max(featureEnvelope.getMinCoord(0), partitionEnvelope.getMinCoord(0));
    double minY = Math.max(featureEnvelope.getMinCoord(1), partitionEnvelope.getMinCoord(1));
    double maxX = Math.min(featureEnvelope.getMaxCoord(0), partitionEnvelope.getMaxCoord(0));
    double maxY = Math.min(featureEnvelope.getMaxCoord(1), partitionEnvelope.getMaxCoord(1));
    if (maxX < minX || maxY < minY)
      return aggregateEnvelope;
    if (aggregateEnvelope == null)
      return new EnvelopeNDLite(2, minX, minY, maxX, maxY);
    aggregateEnvelope.setMinCoord(0, Math.min(aggregateEnvelope.getMinCoord(0), minX));
    aggregateEnvelope.setMinCoord(1, Math.min(aggregateEnvelope.getMinCoord(1), minY));
    aggregateEnvelope.setMaxCoord(0, Math.max(aggregateEnvelope.getMaxCoord(0), maxX));
    aggregateEnvelope.setMaxCoord(1, Math.max(aggregateEnvelope.getMaxCoord(1), maxY));
    return aggregateEnvelope;
  }

  private static boolean intersectsPartitionEnvelopeClosed(EnvelopeNDLite partitionEnvelope,
                                                           org.locationtech.jts.geom.Envelope geometryEnvelope) {
    if (partitionEnvelope == null || partitionEnvelope.isEmpty() || geometryEnvelope == null || geometryEnvelope.isNull())
      return false;
    return geometryEnvelope.getMaxX() >= partitionEnvelope.getMinCoord(0)
        && partitionEnvelope.getMaxCoord(0) >= geometryEnvelope.getMinX()
        && geometryEnvelope.getMaxY() >= partitionEnvelope.getMinCoord(1)
        && partitionEnvelope.getMaxCoord(1) >= geometryEnvelope.getMinY();
  }

  private static void promoteCandidateMaterializedPartitions(JavaSparkContext sparkContext,
                                                             String materializationRoot,
                                                             String side,
                                                             int[] retainedCandidatePartitionIds) {
    if (sparkContext == null || materializationRoot == null || side == null)
      return;
    try {
      Configuration configuration = sparkContext.hadoopConfiguration();
      String candidateSide = candidateMaterializationSide(side);
      Path candidateRoot = new Path(new Path(materializationRoot), candidateSide);
      Path finalRoot = new Path(new Path(materializationRoot), side);
      FileSystem fileSystem = finalRoot.getFileSystem(configuration);
      if (fileSystem.exists(finalRoot))
        fileSystem.delete(finalRoot, true);
      fileSystem.mkdirs(finalRoot);
      if (retainedCandidatePartitionIds != null) {
        for (int newPartitionId = 0; newPartitionId < retainedCandidatePartitionIds.length; newPartitionId++) {
          int oldPartitionId = retainedCandidatePartitionIds[newPartitionId];
          Path oldDir = new Path(partitionDataPath(materializationRoot, candidateSide, oldPartitionId));
          Path newDir = new Path(partitionDataPath(materializationRoot, side, newPartitionId));
          if (!fileSystem.exists(oldDir)) {
            throw new IOException(
                "Missing candidate materialized partition for side "
                    + side
                    + ": newPartitionId="
                    + newPartitionId
                    + ", oldPartitionId="
                    + oldPartitionId
                    + ", path="
                    + oldDir);
          }
          fileSystem.mkdirs(newDir.getParent());
          if (!fileSystem.rename(oldDir, newDir)) {
            throw new IOException(
                "Failed to promote candidate materialized partition for side "
                    + side
                    + ": newPartitionId="
                    + newPartitionId
                    + ", oldPartitionId="
                    + oldPartitionId
                    + ", from="
                    + oldDir
                    + ", to="
                    + newDir);
          }
        }
      }
      if (fileSystem.exists(candidateRoot))
        fileSystem.delete(candidateRoot, true);
    } catch (IOException e) {
      throw new RuntimeException("Failed to promote candidate materialized partitions for side " + side, e);
    }
  }

  private static void ensurePromotedMaterializedPartitionCount(JavaSparkContext sparkContext,
                                                               String materializationRoot,
                                                               String side,
                                                               int expectedCount) {
    if (sparkContext == null || materializationRoot == null || side == null)
      return;
    try {
      Configuration configuration = sparkContext.hadoopConfiguration();
      Path finalRoot = new Path(new Path(materializationRoot), side);
      FileSystem fileSystem = finalRoot.getFileSystem(configuration);
      if (!fileSystem.exists(finalRoot)) {
        throw new IOException(
            "Missing promoted materialized root for side " + side + ": " + finalRoot);
      }
      FileStatus[] children = fileSystem.listStatus(finalRoot, path -> path != null && path.getName().startsWith("partitionId="));
      int actualCount = children == null ? 0 : children.length;
      if (actualCount != expectedCount) {
        throw new IOException(
            "Unexpected promoted materialized partition count for side "
                + side
                + ": expected="
                + expectedCount
                + ", actual="
                + actualCount
                + ", root="
                + finalRoot);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to validate promoted materialized partitions for side " + side, e);
    }
  }

  private MaterializedPartitionValidation validateMaterializedPartitions(JavaSparkContext sparkContext,
                                                                        String materializationRoot,
                                                                        int[] partitionIds,
                                                                        JavaPairRDD<Integer, IFeature> leftPartitionFeatures,
                                                                        JavaPairRDD<Integer, IFeature> rightPartitionFeatures) {
    if (!isValidateMaterializedPartitions() || sparkContext == null || materializationRoot == null || partitionIds == null)
      return null;
    MaterializedPartitionSideValidation leftValidation =
        validateMaterializedPartitionSide(sparkContext, materializationRoot, "left", partitionIds, leftPartitionFeatures);
    MaterializedPartitionSideValidation rightValidation =
        validateMaterializedPartitionSide(sparkContext, materializationRoot, "right", partitionIds, rightPartitionFeatures);
    return new MaterializedPartitionValidation(leftValidation, rightValidation);
  }

  private boolean canDirectlyMaterializeFromPartitioner(String partitionAssignmentStrategy) {
    String normalizedStrategy = PartitionAssignmentStrategyHelper.normalizeStrategy(partitionAssignmentStrategy);
    return SpatialJoinConstants.Execution.PARTITION_ASSIGNMENT_STRATEGY_DUAL_REPLICATION.equals(normalizedStrategy);
  }

  private MaterializedPartitionSideValidation validateMaterializedPartitionSide(JavaSparkContext sparkContext,
                                                                                String materializationRoot,
                                                                                String side,
                                                                                int[] partitionIds,
                                                                                JavaPairRDD<Integer, IFeature> expectedPartitionFeatures) {
    if (expectedPartitionFeatures == null) {
      return new MaterializedPartitionSideValidation(side, 0L, 0L, true, true, Collections.emptyList());
    }
    JavaPairRDD<Integer, IFeature> reloadedPartitionFeatures = loadMaterializedBinaryFeatures(
        sparkContext,
        materializationRoot,
        side,
        partitionIds).persist(StorageLevel.MEMORY_ONLY_SER());
    try {
      Map<Integer, Long> expectedCounts = collectPartitionCounts(expectedPartitionFeatures);
      Map<Integer, Long> reloadedCounts = collectPartitionCounts(reloadedPartitionFeatures);
      Map<String, Long> expectedSignatures = collectFeatureSignatureCounts(expectedPartitionFeatures);
      Map<String, Long> reloadedSignatures = collectFeatureSignatureCounts(reloadedPartitionFeatures);
      boolean partitionCountsMatch = expectedCounts.equals(reloadedCounts);
      boolean contentMatch = expectedSignatures.equals(reloadedSignatures);
      return new MaterializedPartitionSideValidation(
          side,
          sumValues(expectedCounts),
          sumValues(reloadedCounts),
          partitionCountsMatch,
          contentMatch,
          collectMismatchedPartitionIds(expectedCounts, reloadedCounts, expectedSignatures, reloadedSignatures));
    } finally {
      reloadedPartitionFeatures.unpersist(true);
    }
  }

  private Map<Integer, Long> collectPartitionCounts(JavaPairRDD<Integer, IFeature> partitionFeatures) {
    return partitionFeatures
        .mapToPair(tuple -> new Tuple2<>(tuple._1, 1L))
        .reduceByKey(Long::sum)
        .collectAsMap();
  }

  private Map<String, Long> collectFeatureSignatureCounts(JavaPairRDD<Integer, IFeature> partitionFeatures) {
    return partitionFeatures
        .mapToPair(tuple -> new Tuple2<>(buildFeatureSignature(tuple._1, tuple._2), 1L))
        .reduceByKey(Long::sum)
        .collectAsMap();
  }

  private long sumValues(Map<?, Long> values) {
    if (values == null || values.isEmpty())
      return 0L;
    long sum = 0L;
    for (Long value : values.values()) {
      if (value != null)
        sum += value;
    }
    return sum;
  }

  private List<Integer> collectMismatchedPartitionIds(Map<Integer, Long> expectedCounts,
                                                      Map<Integer, Long> reloadedCounts,
                                                      Map<String, Long> expectedSignatures,
                                                      Map<String, Long> reloadedSignatures) {
    TreeSet<Integer> mismatched = new TreeSet<>();
    if (expectedCounts != null)
      mismatched.addAll(expectedCounts.keySet());
    if (reloadedCounts != null)
      mismatched.addAll(reloadedCounts.keySet());
    mismatched.removeIf(partitionId -> {
      Long expected = expectedCounts == null ? null : expectedCounts.get(partitionId);
      Long actual = reloadedCounts == null ? null : reloadedCounts.get(partitionId);
      return Objects.equals(expected, actual);
    });
    TreeSet<String> signatureKeys = new TreeSet<>();
    if (expectedSignatures != null)
      signatureKeys.addAll(expectedSignatures.keySet());
    if (reloadedSignatures != null)
      signatureKeys.addAll(reloadedSignatures.keySet());
    for (String signatureKey : signatureKeys) {
      Long expected = expectedSignatures == null ? null : expectedSignatures.get(signatureKey);
      Long actual = reloadedSignatures == null ? null : reloadedSignatures.get(signatureKey);
      if (!Objects.equals(expected, actual))
        mismatched.add(extractPartitionIdFromSignature(signatureKey));
    }
    return new ArrayList<>(mismatched);
  }

  private int extractPartitionIdFromSignature(String signature) {
    if (signature == null)
      return -1;
    int delimiterIndex = signature.indexOf('|');
    String partitionId = delimiterIndex < 0 ? signature : signature.substring(0, delimiterIndex);
    try {
      return Integer.parseInt(partitionId);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private String buildFeatureSignature(int partitionId, IFeature feature) {
    Object oidValue = null;
    if (feature != null) {
      try {
        int oidIndex = feature.fieldIndex(SpatialJoinConstants.Operator.JOIN_OID_ATTRIBUTE);
        oidValue = feature.isNullAt(oidIndex) ? null : feature.get(oidIndex);
      } catch (RuntimeException ignored) {
        oidValue = null;
      }
    }
    byte[] geometryBytes = feature == null ? null : feature.getGeometryBytes();
    int geometryLength = geometryBytes == null ? 0 : geometryBytes.length;
    int geometryHash = geometryBytes == null ? 0 : Arrays.hashCode(geometryBytes);
    return partitionId + "|" + String.valueOf(oidValue) + "|" + geometryLength + "|" + geometryHash;
  }

  private void writeMaterializationMetadata(JavaSparkContext sparkContext,
                                            PreparedPartitionExecution prepared,
                                            String materializationRoot) {
    if (sparkContext == null || prepared == null || materializationRoot == null)
      return;
    Path metadataDir = new Path(materializationRoot, "metadata");
    try {
      FileSystem fileSystem = metadataDir.getFileSystem(sparkContext.hadoopConfiguration());
      fileSystem.mkdirs(metadataDir);
      writeManifest(fileSystem, new Path(metadataDir, "manifest.tsv"), prepared);
      writePartitionProfiles(fileSystem, new Path(metadataDir, "partition-profiles.tsv"),
          prepared.getEffectivePlan() == null ? Collections.emptyList() : prepared.getEffectivePlan().getPartitionProfiles());
      writeAssignments(fileSystem, new Path(metadataDir, "left-assignments.tsv"),
          prepared.getEffectivePlan() == null ? Collections.emptyList() : prepared.getEffectivePlan().getLeftAssignments());
      writeAssignments(fileSystem, new Path(metadataDir, "right-assignments.tsv"),
          prepared.getEffectivePlan() == null ? Collections.emptyList() : prepared.getEffectivePlan().getRightAssignments());
    } catch (IOException e) {
      throw new RuntimeException("Failed to write disk-hybrid materialization metadata", e);
    }
  }

  private void writeMaterializationMetadata(JavaSparkContext sparkContext,
                                            PartitionPreparationContext context,
                                            String materializationRoot) {
    if (sparkContext == null || context == null || materializationRoot == null)
      return;
    Path metadataDir = new Path(materializationRoot, "metadata");
    try {
      FileSystem fileSystem = metadataDir.getFileSystem(sparkContext.hadoopConfiguration());
      fileSystem.mkdirs(metadataDir);
      writeManifest(fileSystem, new Path(metadataDir, "manifest.tsv"), context);
      writePartitionProfiles(fileSystem, new Path(metadataDir, "partition-profiles.tsv"),
          context.getEffectivePlan() == null ? Collections.emptyList() : context.getEffectivePlan().getPartitionProfiles());
      writeAssignments(fileSystem, new Path(metadataDir, "left-assignments.tsv"),
          context.getEffectivePlan() == null ? Collections.emptyList() : context.getEffectivePlan().getLeftAssignments());
      writeAssignments(fileSystem, new Path(metadataDir, "right-assignments.tsv"),
          context.getEffectivePlan() == null ? Collections.emptyList() : context.getEffectivePlan().getRightAssignments());
    } catch (IOException e) {
      throw new RuntimeException("Failed to write disk-hybrid materialization metadata", e);
    }
  }

  private void writeManifest(FileSystem fileSystem,
                             Path outputPath,
                             PreparedPartitionExecution prepared) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outputPath, true), StandardCharsets.UTF_8))) {
      writer.write("key\tvalue\n");
      writer.write("workingSetMode\t" + SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID + "\n");
      writer.write("serializer\t" + options.getString("spark.serializer", "org.apache.spark.serializer.JavaSerializer") + "\n");
      writer.write("partitionAssignmentStrategy\t" + prepared.getPartitionAssignmentStrategy() + "\n");
      writer.write("anchorSide\t" + prepared.getAnchorSide() + "\n");
      writer.write("leftFeatureCount\t" + prepared.getLeftFeatureCount() + "\n");
      writer.write("rightFeatureCount\t" + prepared.getRightFeatureCount() + "\n");
      writer.write("effectivePartitionCount\t" + (prepared.getEffectivePlan() == null ? 0 : prepared.getEffectivePlan().getPartitionProfiles().size()) + "\n");
    }
  }

  private void writeManifest(FileSystem fileSystem,
                             Path outputPath,
                             PartitionPreparationContext context) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outputPath, true), StandardCharsets.UTF_8))) {
      writer.write("key\tvalue\n");
      writer.write("workingSetMode\t" + SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID + "\n");
      writer.write("serializer\t" + options.getString("spark.serializer", "org.apache.spark.serializer.JavaSerializer") + "\n");
      writer.write("partitionAssignmentStrategy\t" + context.getPartitionAssignmentStrategy() + "\n");
      writer.write("anchorSide\t" + context.getAnchorSide() + "\n");
      writer.write("leftFeatureCount\t" + context.getLeftFeatureCount() + "\n");
      writer.write("rightFeatureCount\t" + context.getRightFeatureCount() + "\n");
      writer.write("effectivePartitionCount\t" + (context.getEffectivePlan() == null ? 0 : context.getEffectivePlan().getPartitionProfiles().size()) + "\n");
    }
  }

  private void writePartitionProfiles(FileSystem fileSystem,
                                      Path outputPath,
                                      List<PartProfile> partitionProfiles) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outputPath, true), StandardCharsets.UTF_8))) {
      writer.write("partitionId\tcode\tleftCount\trightCount\ttotalMemoryLoadBytes\tpartitionIntensity\tenvelope\n");
      if (partitionProfiles == null)
        return;
      for (PartProfile partitionProfile : partitionProfiles) {
        writer.write(partitionProfile.getPartitionId() + "\t"
            + partitionProfile.getPartitionCode() + "\t"
            + partitionProfile.getLeftFeatureCount() + "\t"
            + partitionProfile.getRightFeatureCount() + "\t"
            + partitionProfile.getTotalMemoryLoadBytes() + "\t"
            + partitionProfile.getPartitionIntensity() + "\t"
            + formatEnvelope(partitionProfile.getEnvelope()) + "\n");
      }
    }
  }

  private void writeAssignments(FileSystem fileSystem,
                                Path outputPath,
                                List<SpatialPartitioner.FeatureRef> assignments) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outputPath, true), StandardCharsets.UTF_8))) {
      writer.write("oid\tpartitionId\n");
      if (assignments == null)
        return;
      for (SpatialPartitioner.FeatureRef assignment : assignments) {
        writer.write(assignment.getOid() + "\t" + assignment.getPartitionId() + "\n");
      }
    }
  }

  private String resolveMaterializationRoot(JavaSparkContext sparkContext) {
    String configuredBasePath = getDiskMaterializationBasePath();
    try {
      String normalizedBasePath =
          configuredBasePath == null || configuredBasePath.trim().isEmpty()
              ? SpatialJoinConstants.Execution.DEFAULT_DISK_MATERIALIZATION_BASE_PATH
              : configuredBasePath.trim();
      Path rootPath = new Path(normalizedBasePath, UUID.randomUUID().toString());
      return rootPath.getFileSystem(sparkContext.hadoopConfiguration()).makeQualified(rootPath).toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to resolve disk-hybrid materialization root path", e);
    }
  }

  private static String partitionDataPath(String materializationRoot, String side, int partitionId) {
    return new Path(new Path(new Path(materializationRoot), side), "partitionId=" + partitionId).toString();
  }

  private static String partitionDataFilePath(String materializationRoot, String side, int partitionId) {
    return new Path(partitionDataPath(materializationRoot, side, partitionId), "data.kryo").toString();
  }

  private static void writeMaterializedPartitionFile(Configuration configuration,
                                                     String materializationRoot,
                                                     String side,
                                                     int partitionId,
                                                     Iterator<Tuple2<Integer, IFeature>> records) throws IOException {
    if (configuration == null || materializationRoot == null || side == null)
      return;
    Path partitionDir = new Path(partitionDataPath(materializationRoot, side, partitionId));
    Path partitionFile = new Path(partitionDataFilePath(materializationRoot, side, partitionId));
    FileSystem fileSystem = partitionFile.getFileSystem(configuration);
    if (fileSystem.exists(partitionFile))
      fileSystem.delete(partitionFile, false);
    fileSystem.mkdirs(partitionDir);
    Kryo kryo = createKryo();
    boolean wroteAny = false;
    try (Output output = new Output(fileSystem.create(partitionFile, true))) {
      while (records.hasNext()) {
        Tuple2<Integer, IFeature> next = records.next();
        if (next == null || next._2 == null)
          continue;
        writeMaterializedFeatureRecord(kryo, output, next._2);
        wroteAny = true;
      }
      output.writeByte(MATERIALIZED_RECORD_END);
    }
    if (!wroteAny && fileSystem.exists(partitionFile))
      fileSystem.delete(partitionFile, false);
  }

  private static Iterator<Tuple2<Integer, IFeature>> streamMaterializedPartitionFile(Configuration configuration,
                                                                                     String materializationRoot,
                                                                                     String side,
                                                                                     int partitionId) throws IOException {
    if (configuration == null || materializationRoot == null || side == null)
      return Collections.emptyIterator();
    Path partitionFile = new Path(partitionDataFilePath(materializationRoot, side, partitionId));
    FileSystem fileSystem = partitionFile.getFileSystem(configuration);
    if (!fileSystem.exists(partitionFile))
      return Collections.emptyIterator();
    return new MaterializedPartitionFileIterator(fileSystem.open(partitionFile), partitionId);
  }

  private static Kryo createKryo() {
    Kryo kryo = new Kryo();
    kryo.setRegistrationRequired(false);
    kryo.register(Feature.class);
    return kryo;
  }

  private static void writeMaterializedFeatureRecord(Kryo kryo, Output output, IFeature feature) {
    if (feature instanceof Feature) {
      output.writeByte(MATERIALIZED_RECORD_FEATURE_FAST);
      ((Feature) feature).write(kryo, output);
      return;
    }
    output.writeByte(MATERIALIZED_RECORD_GENERIC);
    kryo.writeClassAndObject(output, feature);
  }

  private static Object readMaterializedFeatureRecord(Kryo kryo, Input input) {
    byte recordType = input.readByte();
    switch (recordType) {
      case MATERIALIZED_RECORD_END:
        return null;
      case MATERIALIZED_RECORD_FEATURE_FAST:
        Feature feature = new Feature();
        feature.read(kryo, input);
        return feature;
      case MATERIALIZED_RECORD_GENERIC:
        return kryo.readClassAndObject(input);
      default:
        throw new IllegalStateException("Unknown materialized feature record type: " + recordType);
    }
  }

  /**
   * Streams one materialized partition file record by record instead of loading the whole file
   * into a temporary list first.
   *
   * <p>This keeps disk-hybrid reload closer to a real out-of-core path: only the current decoded
   * feature needs to stay in memory while Spark consumes the iterator.
   */
  private static final class MaterializedPartitionFileIterator implements Iterator<Tuple2<Integer, IFeature>>, Closeable {
    private final Input input;
    private final Kryo kryo;
    private final int partitionId;

    private Tuple2<Integer, IFeature> nextRecord;
    private boolean finished;
    private boolean closed;

    private MaterializedPartitionFileIterator(org.apache.hadoop.fs.FSDataInputStream inputStream, int partitionId) {
      this.input = new Input(inputStream);
      this.kryo = createKryo();
      this.partitionId = partitionId;
    }

    @Override
    public boolean hasNext() {
      advance();
      return nextRecord != null;
    }

    @Override
    public Tuple2<Integer, IFeature> next() {
      if (!hasNext())
        throw new NoSuchElementException("No more materialized partition records");
      Tuple2<Integer, IFeature> current = nextRecord;
      nextRecord = null;
      return current;
    }

    private void advance() {
      if (nextRecord != null || finished)
        return;
      try {
        while (true) {
          Object feature = readMaterializedFeatureRecord(kryo, input);
          if (feature == null) {
            finish();
            return;
          }
          if (feature instanceof IFeature) {
            nextRecord = new Tuple2<>(partitionId, (IFeature) feature);
            return;
          }
        }
      } catch (RuntimeException e) {
        closeQuietly();
        throw e;
      }
    }

    private void finish() {
      finished = true;
      closeQuietly();
    }

    @Override
    public void close() {
      if (closed)
        return;
      closed = true;
      input.close();
    }

    private void closeQuietly() {
      try {
        close();
      } catch (Exception ignored) {
        // Best-effort cleanup while Spark is draining a partition-file iterator.
      }
    }
  }

  /**
   * Disk-backed exact-refinement state shared by both dataset sides.
   *
   * <p>The first generation is built from the full input once. Later generations contain only
   * records copied out of split parent leaves. Each leaf-side view is backed by one cached Spark
   * partition. Combining both sides into one generation preserves independent statistics while
   * requiring only one repartition-and-collect Spark job per refinement generation.
   */
  private static final class RefinementFrontier implements Closeable {
    private final int histogramBinCount;
    private final List<JavaPairRDD<Integer, SidedAbstractFeature>> persistedGenerations =
        new ArrayList<>();
    private Map<String, JavaRDD<SidedAbstractFeature>> leftRecordsByCode = new HashMap<>();
    private Map<String, JavaRDD<SidedAbstractFeature>> rightRecordsByCode = new HashMap<>();
    private Map<String, FrontierPartitionStats> leftStatsByCode = new HashMap<>();
    private Map<String, FrontierPartitionStats> rightStatsByCode = new HashMap<>();

    private RefinementFrontier(int histogramBinCount) {
      this.histogramBinCount = Math.max(8, histogramBinCount);
    }

    private static RefinementFrontier initialize(
        JavaRDD<AbstractFeature> leftFeatures,
        JavaRDD<AbstractFeature> rightFeatures,
        LAHilbertPartitioner partitioner,
        LAHilbertPartitioner.PartitionInfo[] layout,
        int histogramBinCount) {
      RefinementFrontier frontier = new RefinementFrontier(histogramBinCount);
      if (leftFeatures == null
          || rightFeatures == null
          || partitioner == null
          || layout == null
          || layout.length == 0)
        return frontier;

      JavaPairRDD<Integer, SidedAbstractFeature> leftAssigned = leftFeatures
          .mapPartitionsToPair(iterator -> new DirectAssignedRefinementFeatureIterator(
              iterator,
              partitioner,
              FRONTIER_SIDE_LEFT));
      JavaPairRDD<Integer, SidedAbstractFeature> rightAssigned = rightFeatures
          .mapPartitionsToPair(iterator -> new DirectAssignedRefinementFeatureIterator(
              iterator,
              partitioner,
              FRONTIER_SIDE_RIGHT));
      JavaPairRDD<Integer, SidedAbstractFeature> generation = leftAssigned
          .union(rightAssigned)
          .partitionBy(new HashPartitioner(frontierPartitionCount(layout.length)))
          .persist(DISK_HYBRID_PREPARED_FEATURE_STORAGE_LEVEL);
      Map<Integer, FrontierPartitionStats> generationStats = collectFrontierGenerationStats(
          generation,
          envelopesByFrontierKey(layout),
          histogramBinCount);
      frontier.persistedGenerations.add(generation);
      for (LAHilbertPartitioner.PartitionInfo info : layout) {
        if (info == null)
          continue;
        for (int side = 0; side < FRONTIER_SIDE_COUNT; side++) {
          int key = frontierKey(info.getPartitionId(), side);
          frontier.recordsByCode(side).put(
              info.getCode(),
              selectSparkPartitions(generation, Collections.singleton(key)).values());
          FrontierPartitionStats stats = generationStats.get(key);
          frontier.statsByCode(side).put(
              info.getCode(),
              stats == null ? new FrontierPartitionStats(histogramBinCount) : stats);
        }
      }
      return frontier;
    }

    private SpatialPartitioner.PartitionRefinementStats toStats(
        LAHilbertPartitioner.PartitionInfo[] layout,
        int side) {
      if (layout == null || layout.length == 0)
        return SpatialPartitioner.PartitionRefinementStats.empty(0, histogramBinCount);
      Map<String, FrontierPartitionStats> sideStatsByCode = statsByCode(side);
      long[] featureCounts = new long[layout.length];
      long[] totalBytes = new long[layout.length];
      EnvelopeNDLite[] aggregateEnvelopes = new EnvelopeNDLite[layout.length];
      long[][][] countHistograms = new long[layout.length][2][histogramBinCount];
      long[][][] byteHistograms = new long[layout.length][2][histogramBinCount];
      for (int partitionId = 0; partitionId < layout.length; partitionId++) {
        LAHilbertPartitioner.PartitionInfo info = layout[partitionId];
        FrontierPartitionStats stats = info == null ? null : sideStatsByCode.get(info.getCode());
        if (stats == null)
          continue;
        featureCounts[partitionId] = stats.featureCount;
        totalBytes[partitionId] = stats.totalBytes;
        aggregateEnvelopes[partitionId] = stats.aggregateEnvelope == null
            ? null
            : new EnvelopeNDLite(stats.aggregateEnvelope);
        copyMaterializedHistograms(stats.countHistograms, countHistograms[partitionId]);
        copyMaterializedHistograms(stats.byteHistograms, byteHistograms[partitionId]);
      }
      return SpatialPartitioner.PartitionRefinementStats.of(
          featureCounts,
          totalBytes,
          aggregateEnvelopes,
          countHistograms,
          byteHistograms,
          histogramBinCount);
    }

    private void apply(RefinementLayoutDelta delta) {
      if (delta == null)
        return;
      Map<String, JavaRDD<SidedAbstractFeature>> nextLeftRecordsByCode = new HashMap<>();
      Map<String, JavaRDD<SidedAbstractFeature>> nextRightRecordsByCode = new HashMap<>();
      Map<String, FrontierPartitionStats> nextLeftStatsByCode = new HashMap<>();
      Map<String, FrontierPartitionStats> nextRightStatsByCode = new HashMap<>();
      copyUnchanged(
          delta.unchangedCodes,
          leftRecordsByCode,
          leftStatsByCode,
          nextLeftRecordsByCode,
          nextLeftStatsByCode);
      copyUnchanged(
          delta.unchangedCodes,
          rightRecordsByCode,
          rightStatsByCode,
          nextRightRecordsByCode,
          nextRightStatsByCode);

      if (!delta.splitParents.isEmpty()) {
        List<JavaRDD<TaggedFrontierFeature>> taggedParents = new ArrayList<>();
        Map<Integer, FrontierSplitParent> splitParentsByIndex = new HashMap<>();
        for (FrontierSplitParent splitParent : delta.splitParents) {
          for (int side = 0; side < FRONTIER_SIDE_COUNT; side++) {
            JavaRDD<SidedAbstractFeature> parentRecords =
                recordsByCode(side).get(splitParent.parentCode);
            if (parentRecords == null)
              throw new IllegalStateException(
                  "Missing refinement frontier records for split parent "
                      + splitParent.parentCode
                      + " side="
                      + side);
            final int parentIndex = splitParent.parentIndex;
            taggedParents.add(
                parentRecords.map(feature -> new TaggedFrontierFeature(parentIndex, feature)));
          }
          splitParentsByIndex.put(splitParent.parentIndex, splitParent);
        }
        @SuppressWarnings("unchecked")
        JavaRDD<TaggedFrontierFeature>[] parentRdds =
            taggedParents.toArray(new JavaRDD[0]);
        JavaRDD<TaggedFrontierFeature> selectedParents =
            JavaSparkContext.fromSparkContext(taggedParents.get(0).context()).union(parentRdds);
        JavaPairRDD<Integer, SidedAbstractFeature> generation = selectedParents
            .mapPartitionsToPair(iterator ->
                new RedistributedFrontierFeatureIterator(iterator, splitParentsByIndex))
            .partitionBy(new HashPartitioner(frontierPartitionCount(delta.splitChildCount)))
            .persist(DISK_HYBRID_PREPARED_FEATURE_STORAGE_LEVEL);
        Map<Integer, FrontierPartitionStats> generationStats = collectFrontierGenerationStats(
            generation,
            envelopesByFrontierKey(delta.splitChildEnvelopes),
            histogramBinCount);
        persistedGenerations.add(generation);
        for (FrontierSplitParent splitParent : delta.splitParents) {
          for (FrontierSplitChild child : splitParent.children) {
            for (int side = 0; side < FRONTIER_SIDE_COUNT; side++) {
              int key = frontierKey(child.slot, side);
              recordsByCode(
                  side,
                  nextLeftRecordsByCode,
                  nextRightRecordsByCode).put(
                      child.code,
                      selectSparkPartitions(generation, Collections.singleton(key)).values());
              FrontierPartitionStats stats = generationStats.get(key);
              statsByCode(
                  side,
                  nextLeftStatsByCode,
                  nextRightStatsByCode).put(
                      child.code,
                      stats == null ? new FrontierPartitionStats(histogramBinCount) : stats);
            }
          }
        }
      }

      for (LAHilbertPartitioner.PartitionInfo info : delta.nextLayout) {
        if (info == null)
          continue;
        for (int side = 0; side < FRONTIER_SIDE_COUNT; side++) {
          Map<String, JavaRDD<SidedAbstractFeature>> nextRecords = recordsByCode(
              side,
              nextLeftRecordsByCode,
              nextRightRecordsByCode);
          Map<String, FrontierPartitionStats> nextStats = statsByCode(
              side,
              nextLeftStatsByCode,
              nextRightStatsByCode);
          if (!nextRecords.containsKey(info.getCode()))
            throw new IllegalStateException(
                "Refinement frontier did not produce records for leaf "
                    + info.getCode()
                    + " side="
                    + side);
          nextStats.putIfAbsent(info.getCode(), new FrontierPartitionStats(histogramBinCount));
        }
      }
      leftRecordsByCode = nextLeftRecordsByCode;
      rightRecordsByCode = nextRightRecordsByCode;
      leftStatsByCode = nextLeftStatsByCode;
      rightStatsByCode = nextRightStatsByCode;
    }

    private static void copyUnchanged(
        Set<String> unchangedCodes,
        Map<String, JavaRDD<SidedAbstractFeature>> recordsByCode,
        Map<String, FrontierPartitionStats> statsByCode,
        Map<String, JavaRDD<SidedAbstractFeature>> nextRecordsByCode,
        Map<String, FrontierPartitionStats> nextStatsByCode) {
      for (String unchangedCode : unchangedCodes) {
        JavaRDD<SidedAbstractFeature> records = recordsByCode.get(unchangedCode);
        if (records != null)
          nextRecordsByCode.put(unchangedCode, records);
        FrontierPartitionStats stats = statsByCode.get(unchangedCode);
        if (stats != null)
          nextStatsByCode.put(unchangedCode, stats);
      }
    }

    private Map<String, JavaRDD<SidedAbstractFeature>> recordsByCode(int side) {
      return recordsByCode(side, leftRecordsByCode, rightRecordsByCode);
    }

    private Map<String, FrontierPartitionStats> statsByCode(int side) {
      return statsByCode(side, leftStatsByCode, rightStatsByCode);
    }

    private static <T> Map<String, T> recordsByCode(
        int side,
        Map<String, T> left,
        Map<String, T> right) {
      return side == FRONTIER_SIDE_LEFT ? left : right;
    }

    private static <T> Map<String, T> statsByCode(
        int side,
        Map<String, T> left,
        Map<String, T> right) {
      return side == FRONTIER_SIDE_LEFT ? left : right;
    }

    @Override
    public void close() {
      for (JavaPairRDD<Integer, SidedAbstractFeature> generation : persistedGenerations)
        unpersistQuietly(generation);
      persistedGenerations.clear();
      leftRecordsByCode.clear();
      rightRecordsByCode.clear();
      leftStatsByCode.clear();
      rightStatsByCode.clear();
    }
  }

  /** Describes unchanged, pruned, and split leaves between two exact-refinement layouts. */
  private static final class RefinementLayoutDelta {
    private final LAHilbertPartitioner.PartitionInfo[] nextLayout;
    private final Set<String> unchangedCodes;
    private final List<FrontierSplitParent> splitParents;
    private final int prunedParentCount;
    private final int splitChildCount;
    private final EnvelopeNDLite[] splitChildEnvelopes;

    private RefinementLayoutDelta(
        LAHilbertPartitioner.PartitionInfo[] nextLayout,
        Set<String> unchangedCodes,
        List<FrontierSplitParent> splitParents,
        int prunedParentCount,
        int splitChildCount,
        EnvelopeNDLite[] splitChildEnvelopes) {
      this.nextLayout = nextLayout;
      this.unchangedCodes = unchangedCodes;
      this.splitParents = splitParents;
      this.prunedParentCount = prunedParentCount;
      this.splitChildCount = splitChildCount;
      this.splitChildEnvelopes = splitChildEnvelopes;
    }

    private static RefinementLayoutDelta between(
        LAHilbertPartitioner.PartitionInfo[] currentLayout,
        LAHilbertPartitioner.PartitionInfo[] nextLayout) {
      LAHilbertPartitioner.PartitionInfo[] safeCurrent = currentLayout == null
          ? new LAHilbertPartitioner.PartitionInfo[0]
          : currentLayout;
      LAHilbertPartitioner.PartitionInfo[] safeNext = nextLayout == null
          ? new LAHilbertPartitioner.PartitionInfo[0]
          : nextLayout;
      Map<String, LAHilbertPartitioner.PartitionInfo> nextByCode = new HashMap<>();
      for (LAHilbertPartitioner.PartitionInfo info : safeNext) {
        if (info != null)
          nextByCode.put(info.getCode(), info);
      }
      Set<String> unchangedCodes = new HashSet<>();
      Set<String> accountedNextCodes = new HashSet<>();
      List<FrontierSplitParent> splitParents = new ArrayList<>();
      List<EnvelopeNDLite> childEnvelopes = new ArrayList<>();
      int prunedParentCount = 0;
      int parentIndex = 0;
      for (LAHilbertPartitioner.PartitionInfo current : safeCurrent) {
        if (current == null)
          continue;
        if (nextByCode.containsKey(current.getCode())) {
          unchangedCodes.add(current.getCode());
          accountedNextCodes.add(current.getCode());
          continue;
        }
        List<FrontierSplitChild> children = new ArrayList<>();
        String childPrefix = current.getCode() + ".";
        for (LAHilbertPartitioner.PartitionInfo next : safeNext) {
          if (next == null
              || next.getDepth() != current.getDepth() + 1
              || !next.getCode().startsWith(childPrefix))
            continue;
          int slot = childEnvelopes.size();
          EnvelopeNDLite envelope = next.getEnvelope();
          childEnvelopes.add(envelope);
          children.add(new FrontierSplitChild(slot, next.getCode(), envelope));
          accountedNextCodes.add(next.getCode());
        }
        if (children.isEmpty()) {
          prunedParentCount++;
        } else {
          splitParents.add(new FrontierSplitParent(parentIndex++, current.getCode(), children));
        }
      }
      if (accountedNextCodes.size() != nextByCode.size()) {
        Set<String> missing = new TreeSet<>(nextByCode.keySet());
        missing.removeAll(accountedNextCodes);
        throw new IllegalStateException("Unaccounted exact-refinement leaves: " + missing);
      }
      return new RefinementLayoutDelta(
          safeNext,
          unchangedCodes,
          splitParents,
          prunedParentCount,
          childEnvelopes.size(),
          childEnvelopes.toArray(new EnvelopeNDLite[0]));
    }

    private Set<String> splitChildCodes() {
      Set<String> codes = new HashSet<>();
      for (FrontierSplitParent splitParent : splitParents) {
        for (FrontierSplitChild child : splitParent.children)
          codes.add(child.code);
      }
      return codes;
    }
  }

  private static final class FrontierSplitParent implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int parentIndex;
    private final String parentCode;
    private final List<FrontierSplitChild> children;

    private FrontierSplitParent(int parentIndex, String parentCode, List<FrontierSplitChild> children) {
      this.parentIndex = parentIndex;
      this.parentCode = parentCode;
      this.children = children;
    }
  }

  private static final class FrontierSplitChild implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int slot;
    private final String code;
    private final EnvelopeNDLite envelope;

    private FrontierSplitChild(int slot, String code, EnvelopeNDLite envelope) {
      this.slot = slot;
      this.code = code;
      this.envelope = new EnvelopeNDLite(envelope);
    }
  }

  private static final class SidedAbstractFeature implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int side;
    private final AbstractFeature feature;

    private SidedAbstractFeature(int side, AbstractFeature feature) {
      if (side < 0 || side >= FRONTIER_SIDE_COUNT)
        throw new IllegalArgumentException("Unsupported frontier side: " + side);
      this.side = side;
      this.feature = feature;
    }
  }

  private static final class TaggedFrontierFeature implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int parentIndex;
    private final SidedAbstractFeature sidedFeature;

    private TaggedFrontierFeature(int parentIndex, SidedAbstractFeature sidedFeature) {
      this.parentIndex = parentIndex;
      this.sidedFeature = sidedFeature;
    }
  }

  /** Mutable executor-local exact statistics for one frontier leaf. */
  private static final class FrontierPartitionStats implements Serializable {
    private static final long serialVersionUID = 1L;
    private long featureCount;
    private long totalBytes;
    private EnvelopeNDLite aggregateEnvelope;
    private final long[][] countHistograms;
    private final long[][] byteHistograms;

    private FrontierPartitionStats(int histogramBinCount) {
      int safeBinCount = Math.max(8, histogramBinCount);
      this.countHistograms = new long[2][safeBinCount];
      this.byteHistograms = new long[2][safeBinCount];
    }

    private void add(AbstractFeature feature, EnvelopeNDLite partitionEnvelope) {
      if (feature == null || partitionEnvelope == null)
        return;
      long loadBytes = Math.max(0L, feature.getLoadWeight());
      featureCount = saturatedAdd(featureCount, 1L);
      totalBytes = saturatedAdd(totalBytes, loadBytes);
      aggregateEnvelope = updateProjectedAggregateEnvelope(
          aggregateEnvelope,
          feature,
          partitionEnvelope);
      int axisCount = Math.min(
          2,
          Math.min(feature.getCoordinateDimension(), partitionEnvelope.getCoordinateDimension()));
      for (int axis = 0; axis < axisCount; axis++) {
        double min = partitionEnvelope.getMinCoord(axis);
        double length = partitionEnvelope.getSideLength(axis);
        int bin = 0;
        if (length > 0.0d) {
          double center = Math.max(
              min,
              Math.min(partitionEnvelope.getMaxCoord(axis), feature.getCenter(axis)));
          bin = (int) Math.floor(((center - min) / length) * countHistograms[axis].length);
          bin = Math.max(0, Math.min(countHistograms[axis].length - 1, bin));
        }
        countHistograms[axis][bin] = saturatedAdd(countHistograms[axis][bin], 1L);
        byteHistograms[axis][bin] = saturatedAdd(byteHistograms[axis][bin], loadBytes);
      }
    }

    private FrontierPartitionStats mergeInPlace(FrontierPartitionStats other) {
      if (other == null)
        return this;
      featureCount = saturatedAdd(featureCount, other.featureCount);
      totalBytes = saturatedAdd(totalBytes, other.totalBytes);
      if (other.aggregateEnvelope != null && !other.aggregateEnvelope.isEmpty()) {
        if (aggregateEnvelope == null)
          aggregateEnvelope = new EnvelopeNDLite(other.aggregateEnvelope);
        else
          aggregateEnvelope.merge(other.aggregateEnvelope);
      }
      for (int axis = 0; axis < countHistograms.length; axis++) {
        for (int bin = 0; bin < countHistograms[axis].length; bin++) {
          countHistograms[axis][bin] = saturatedAdd(
              countHistograms[axis][bin],
              other.countHistograms[axis][bin]);
          byteHistograms[axis][bin] = saturatedAdd(
              byteHistograms[axis][bin],
              other.byteHistograms[axis][bin]);
        }
      }
      return this;
    }
  }

  private static Map<Integer, FrontierPartitionStats> collectFrontierGenerationStats(
      JavaPairRDD<Integer, SidedAbstractFeature> generation,
      EnvelopeNDLite[] envelopesByKey,
      int histogramBinCount) {
    if (generation == null)
      return Collections.emptyMap();
    return generation
        .mapPartitionsToPair(iterator -> {
          Map<Integer, FrontierPartitionStats> local = new HashMap<>();
          while (iterator.hasNext()) {
            Tuple2<Integer, SidedAbstractFeature> next = iterator.next();
            if (next == null || next._1 == null || next._2 == null)
              continue;
            int key = next._1;
            if (key < 0 || envelopesByKey == null || key >= envelopesByKey.length)
              continue;
            FrontierPartitionStats stats = local.computeIfAbsent(
                key,
                ignored -> new FrontierPartitionStats(histogramBinCount));
            stats.add(next._2.feature, envelopesByKey[key]);
          }
          List<Tuple2<Integer, FrontierPartitionStats>> output = new ArrayList<>(local.size());
          for (Map.Entry<Integer, FrontierPartitionStats> entry : local.entrySet())
            output.add(new Tuple2<>(entry.getKey(), entry.getValue()));
          return output.iterator();
        }, true)
        .reduceByKey(FrontierPartitionStats::mergeInPlace)
        .collectAsMap();
  }

  private static EnvelopeNDLite[] envelopesByFrontierKey(
      LAHilbertPartitioner.PartitionInfo[] layout) {
    if (layout == null)
      return new EnvelopeNDLite[0];
    EnvelopeNDLite[] partitionEnvelopes = new EnvelopeNDLite[layout.length];
    for (LAHilbertPartitioner.PartitionInfo info : layout) {
      if (info != null
          && info.getPartitionId() >= 0
          && info.getPartitionId() < partitionEnvelopes.length)
        partitionEnvelopes[info.getPartitionId()] = info.getEnvelope();
    }
    return envelopesByFrontierKey(partitionEnvelopes);
  }

  private static EnvelopeNDLite[] envelopesByFrontierKey(EnvelopeNDLite[] partitionEnvelopes) {
    if (partitionEnvelopes == null)
      return new EnvelopeNDLite[0];
    EnvelopeNDLite[] envelopes = new EnvelopeNDLite[frontierPartitionCount(partitionEnvelopes.length)];
    for (int slot = 0; slot < partitionEnvelopes.length; slot++) {
      for (int side = 0; side < FRONTIER_SIDE_COUNT; side++)
        envelopes[frontierKey(slot, side)] = partitionEnvelopes[slot];
    }
    return envelopes;
  }

  private static int frontierKey(int slot, int side) {
    if (slot < 0)
      throw new IllegalArgumentException("Frontier slot must be non-negative: " + slot);
    if (side < 0 || side >= FRONTIER_SIDE_COUNT)
      throw new IllegalArgumentException("Unsupported frontier side: " + side);
    return Math.addExact(Math.multiplyExact(slot, FRONTIER_SIDE_COUNT), side);
  }

  private static int frontierPartitionCount(int slotCount) {
    if (slotCount <= 0)
      throw new IllegalArgumentException("Frontier slot count must be positive: " + slotCount);
    return Math.multiplyExact(slotCount, FRONTIER_SIDE_COUNT);
  }

  private static long saturatedAdd(long left, long right) {
    if (right > 0L && left > Long.MAX_VALUE - right)
      return Long.MAX_VALUE;
    if (right < 0L && left < Long.MIN_VALUE - right)
      return Long.MIN_VALUE;
    return left + right;
  }

  private static <K, V> JavaPairRDD<K, V> selectSparkPartitions(
      JavaPairRDD<K, V> source,
      Set<Integer> selectedPartitionIds) {
    if (source == null || selectedPartitionIds == null || selectedPartitionIds.isEmpty())
      throw new IllegalArgumentException("Cannot select an empty Spark partition set");
    RDD<Tuple2<K, V>> pruned = PartitionPruningRDD.create(
        source.rdd(),
        new SelectedPartitionPredicate(selectedPartitionIds));
    return source.wrapRDD(pruned);
  }

  private static final class SelectedPartitionPredicate
      extends AbstractFunction1<Object, Object>
      implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Set<Integer> selectedPartitionIds;

    private SelectedPartitionPredicate(Set<Integer> selectedPartitionIds) {
      this.selectedPartitionIds = new HashSet<>(selectedPartitionIds);
    }

    @Override
    public Object apply(Object partitionIndex) {
      return partitionIndex instanceof Number
          && selectedPartitionIds.contains(((Number) partitionIndex).intValue());
    }
  }

  private static final class MaterializedPartitionStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private long featureCount;
    private long planningTotalBytes;
    private long refinementTotalBytes;
    private long totalVertexCount;
    private long minVertexCount;
    private long maxVertexCount;
    private double totalComplexity;
    private final SpatialPartitioner.CibHistogramStats cibStats = new SpatialPartitioner.CibHistogramStats();
    private EnvelopeNDLite aggregateEnvelope;
    private final long[][] countHistograms =
        new long[2][DISK_HYBRID_EXACT_REFINEMENT_HISTOGRAM_BINS];
    private final long[][] byteHistograms =
        new long[2][DISK_HYBRID_EXACT_REFINEMENT_HISTOGRAM_BINS];
  }

  private static final class CandidatePartitionMaterializationStats {
    private final SpatialPartitioner.PartitionLoadStats loadStats;
    private final SpatialPartitioner.PartitionRefinementStats refinementStats;

    private CandidatePartitionMaterializationStats(
        SpatialPartitioner.PartitionLoadStats loadStats,
        SpatialPartitioner.PartitionRefinementStats refinementStats) {
      this.loadStats = loadStats;
      this.refinementStats = refinementStats;
    }
  }

  private static boolean pathExists(JavaSparkContext sparkContext, String path) {
    if (sparkContext == null || path == null || path.trim().isEmpty())
      return false;
    try {
      Path targetPath = new Path(path);
      FileSystem fileSystem = targetPath.getFileSystem(sparkContext.hadoopConfiguration());
      return fileSystem.exists(targetPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to check materialized partition path " + path, e);
    }
  }

  private static void deletePathIfExists(JavaSparkContext sparkContext, String path) {
    if (sparkContext == null || path == null || path.trim().isEmpty())
      return;
    try {
      Path targetPath = new Path(path);
      FileSystem fileSystem = targetPath.getFileSystem(sparkContext.hadoopConfiguration());
      if (fileSystem.exists(targetPath))
        fileSystem.delete(targetPath, true);
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete path " + path, e);
    }
  }

  private static String formatEnvelope(cn.edu.whu.spatialJoin.geom.EnvelopeNDLite envelope) {
    if (envelope == null || envelope.isEmpty())
      return "EMPTY";
    return envelope.toString();
  }

  private List<JobExecutionResult> executePartitionPlan(JavaSparkContext sparkContext,
                                                        SpatialPartitioner.EffectivePlan effectivePlan,
                                                        JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    long dispatchLoadBudgetBytes = createJobPlanner().getDispatchLoadBudgetBytes();
    try (JobDispatcher dispatcher = createJobDispatcher()) {
      return dispatcher.dispatchResourceAware(
          executionPlan.getJobGroups(),
          dispatchLoadBudgetBytes,
          jobGroup -> runWithJobGroup(
              sparkContext,
              jobGroup,
              () -> executePartitionJob(sparkContext, effectivePlan, jobGroup)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute partition-aware join plan", e);
    }
  }

  /**
   * Executes the partition-aware job queue against a prepared execution context.
   *
   * Reusing the prepared context avoids rereading the full left/right datasets for every job group.
   */
  private List<JobExecutionResult> executePartitionPlan(PreparedPartitionExecution prepared,
                                                        JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(prepared.getLeftPartitionedFeatures() == null
        ? prepared.getLeftFeaturesByOid().context()
        : prepared.getLeftPartitionedFeatures().context());
    long dispatchLoadBudgetBytes = createJobPlanner().getDispatchLoadBudgetBytes();
    try (JobDispatcher dispatcher = createJobDispatcher()) {
      return dispatcher.dispatchResourceAware(
          executionPlan.getJobGroups(),
          dispatchLoadBudgetBytes,
          jobGroup -> runWithJobGroup(
              sparkContext,
              jobGroup,
              () -> executePartitionJob(prepared, jobGroup)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute partition-aware join plan", e);
    }
  }

  /** Executes object-pair file output through the same resource-aware task-unit queue as normal compute. */
  private List<JobExecutionResult> executeObjectPairFilePlan(PreparedPartitionExecution prepared,
                                                             JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(prepared.getLeftPartitionedFeatures() == null
        ? prepared.getLeftFeaturesByOid().context()
        : prepared.getLeftPartitionedFeatures().context());
    long dispatchLoadBudgetBytes = createJobPlanner().getDispatchLoadBudgetBytes();
    try (JobDispatcher dispatcher = createJobDispatcher()) {
      return dispatcher.dispatchResourceAware(
          executionPlan.getJobGroups(),
          dispatchLoadBudgetBytes,
          jobGroup -> runWithJobGroup(
              sparkContext,
              jobGroup,
              () -> executeObjectPairFileJob(prepared, jobGroup)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute object-pair file plan", e);
    }
  }

  /**
   * Executes the partition-aware job queue against a disk-hybrid materialized working set.
   *
   * <p>Each dispatched job reloads only the partition files corresponding to its target partition ids.
   */
  private List<JobExecutionResult> executePartitionPlan(JavaSparkContext sparkContext,
                                                        MaterializedPartitionExecution materialized,
                                                        JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    long dispatchLoadBudgetBytes = createJobPlanner().getDispatchLoadBudgetBytes();
    try (JobDispatcher dispatcher = createJobDispatcher()) {
      List<JobExecutionResult> results = dispatcher.dispatchResourceAware(
          executionPlan.getJobGroups(),
          dispatchLoadBudgetBytes,
          jobGroup -> runWithJobGroup(
              sparkContext,
              jobGroup,
              () -> executePartitionJob(sparkContext, materialized, jobGroup)));
      return results;
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute disk-hybrid partition-aware join plan", e);
    }
  }

  /** Executes disk-hybrid object-pair file output through the normal resource-aware dispatcher. */
  private List<JobExecutionResult> executeObjectPairFilePlan(JavaSparkContext sparkContext,
                                                             MaterializedPartitionExecution materialized,
                                                             JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    long dispatchLoadBudgetBytes = createJobPlanner().getDispatchLoadBudgetBytes();
    try (JobDispatcher dispatcher = createJobDispatcher()) {
      return dispatcher.dispatchResourceAware(
          executionPlan.getJobGroups(),
          dispatchLoadBudgetBytes,
          jobGroup -> runWithJobGroup(
              sparkContext,
              jobGroup,
              () -> executeObjectPairFileJob(sparkContext, materialized, jobGroup)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute disk-hybrid object-pair file plan", e);
    }
  }

  /**
   * Executes all planned jobs strictly in dispatch order without using the dispatcher.
   *
   * This is intended for debug and verification runs where deterministic sequential progress
   * is preferred over resource-aware concurrency.
   */
  private List<JobExecutionResult> executePartitionPlanSequential(PreparedPartitionExecution prepared,
                                                                  JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    List<JobExecutionResult> results = new ArrayList<>(executionPlan.getJobGroups().size());
    int totalJobs = executionPlan.getJobGroups().size();
    int jobOrder = 0;
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(prepared.getLeftPartitionedFeatures() == null
        ? prepared.getLeftFeaturesByOid().context()
        : prepared.getLeftPartitionedFeatures().context());
    for (JobGroup jobGroup : executionPlan.getJobGroups()) {
      jobOrder++;
      long startNanos = System.nanoTime();
      System.out.println(String.format(
          "[Sequential job] start %d/%d, jobId=%d, type=%s, partitions=%s",
          jobOrder,
          totalJobs,
          jobGroup.getJobId(),
          jobGroup.getComplexityType(),
          Arrays.toString(jobGroup.getPartitionIds())));
      results.add(runWithJobGroup(sparkContext, jobGroup, () -> executePartitionJob(prepared, jobGroup)));
      long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
      System.out.println(String.format(
          "[Sequential job] done  %d/%d, jobId=%d, elapsed=%d ms",
          jobOrder,
          totalJobs,
          jobGroup.getJobId(),
          elapsedMillis));
    }
    return results;
  }

  /** Sequential object-pair file output for deterministic verification. */
  private List<JobExecutionResult> executeObjectPairFilePlanSequential(PreparedPartitionExecution prepared,
                                                                       JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    List<JobExecutionResult> results = new ArrayList<>(executionPlan.getJobGroups().size());
    int totalJobs = executionPlan.getJobGroups().size();
    int jobOrder = 0;
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(prepared.getLeftPartitionedFeatures() == null
        ? prepared.getLeftFeaturesByOid().context()
        : prepared.getLeftPartitionedFeatures().context());
    for (JobGroup jobGroup : executionPlan.getJobGroups()) {
      jobOrder++;
      long startNanos = System.nanoTime();
      System.out.println(String.format(
          "[Sequential object-pair job] start %d/%d, jobId=%d, type=%s, partitions=%s",
          jobOrder,
          totalJobs,
          jobGroup.getJobId(),
          jobGroup.getComplexityType(),
          Arrays.toString(jobGroup.getPartitionIds())));
      results.add(runWithJobGroup(sparkContext, jobGroup, () -> executeObjectPairFileJob(prepared, jobGroup)));
      long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
      System.out.println(String.format(
          "[Sequential object-pair job] done  %d/%d, jobId=%d, elapsed=%d ms",
          jobOrder,
          totalJobs,
          jobGroup.getJobId(),
          elapsedMillis));
    }
    return results;
  }

  /** Executes all disk-hybrid planned jobs strictly in dispatch order without using the dispatcher. */
  private List<JobExecutionResult> executePartitionPlanSequential(JavaSparkContext sparkContext,
                                                                  MaterializedPartitionExecution materialized,
                                                                  JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    List<JobExecutionResult> results = new ArrayList<>(executionPlan.getJobGroups().size());
    int totalJobs = executionPlan.getJobGroups().size();
    int jobOrder = 0;
    for (JobGroup jobGroup : executionPlan.getJobGroups()) {
      jobOrder++;
      long startNanos = System.nanoTime();
      System.out.println(String.format(
          "[Sequential job] start %d/%d, jobId=%d, type=%s, partitions=%s",
          jobOrder,
          totalJobs,
          jobGroup.getJobId(),
          jobGroup.getComplexityType(),
          Arrays.toString(jobGroup.getPartitionIds())));
      results.add(runWithJobGroup(sparkContext, jobGroup,
          () -> executePartitionJob(sparkContext, materialized, jobGroup)));
      long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
      System.out.println(String.format(
          "[Sequential job] done  %d/%d, jobId=%d, elapsed=%d ms",
          jobOrder,
          totalJobs,
          jobGroup.getJobId(),
          elapsedMillis));
    }
    return results;
  }

  /** Sequential disk-hybrid object-pair file output for deterministic verification. */
  private List<JobExecutionResult> executeObjectPairFilePlanSequential(JavaSparkContext sparkContext,
                                                                       MaterializedPartitionExecution materialized,
                                                                       JobQueueExecutionPlan executionPlan) {
    if (executionPlan == null || executionPlan.getJobGroups().isEmpty()) {
      return Collections.emptyList();
    }
    List<JobExecutionResult> results = new ArrayList<>(executionPlan.getJobGroups().size());
    int totalJobs = executionPlan.getJobGroups().size();
    int jobOrder = 0;
    for (JobGroup jobGroup : executionPlan.getJobGroups()) {
      jobOrder++;
      long startNanos = System.nanoTime();
      System.out.println(String.format(
          "[Sequential object-pair job] start %d/%d, jobId=%d, type=%s, partitions=%s",
          jobOrder,
          totalJobs,
          jobGroup.getJobId(),
          jobGroup.getComplexityType(),
          Arrays.toString(jobGroup.getPartitionIds())));
      results.add(runWithJobGroup(sparkContext, jobGroup,
          () -> executeObjectPairFileJob(sparkContext, materialized, jobGroup)));
      long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
      System.out.println(String.format(
          "[Sequential object-pair job] done  %d/%d, jobId=%d, elapsed=%d ms",
          jobOrder,
          totalJobs,
          jobGroup.getJobId(),
          elapsedMillis));
    }
    return results;
  }

  private long executePartitionJoin(Iterable<IFeature> leftFeatures, Iterable<IFeature> rightFeatures) {
    throw new UnsupportedOperationException("Use the partition-aware overload that includes the partition envelope");
  }

  /**
   * Executes one partition-local join and keeps only candidate pairs whose reference point
   * falls inside the current partition.
   *
   * <p>This is the current implementation of the document's reference-point rule used to
   * eliminate duplicate candidate pairs introduced by multi-partition feature replication.
   *
   * <p>The method receives the original left/right features already assigned to one logical
   * partition. It materializes those iterables into in-memory lists, runs the geometry helper
   * join on them, and counts only those matches whose duplicate-avoidance reference point falls
   * inside the current partition envelope.
   *
   * <p>Although the helper join may internally split large geometries into smaller pieces for
   * the filter phase, the callback indexes always refer back to {@code leftList} and
   * {@code rightList}. In other words, this method counts matches between the original features
   * loaded for the partition, not between temporary split fragments.
   */
  private long executePartitionJoin(SpatialPartitioner partitioner,
                                    int partitionId,
                                    Iterable<IFeature> leftFeatures,
                                    Iterable<IFeature> rightFeatures) {
    return executePartitionJoin(partitioner, partitionId, leftFeatures, rightFeatures, true, null, null);
  }

  private long executePartitionJoin(SpatialPartitioner partitioner,
                                    int partitionId,
                                    Iterable<IFeature> leftFeatures,
                                    Iterable<IFeature> rightFeatures,
                                    boolean useReferencePointDuplicateAvoidance) {
    return executePartitionJoin(
        partitioner,
        partitionId,
        leftFeatures,
        rightFeatures,
        useReferencePointDuplicateAvoidance,
        null,
        null);
  }

  long executePartitionJoin(SpatialPartitioner partitioner,
                            int partitionId,
                            Iterable<IFeature> leftFeatures,
                            Iterable<IFeature> rightFeatures,
                            boolean useReferencePointDuplicateAvoidance,
                            LongAccumulator localJoinNanos,
                            LongAccumulator partitionJoinCalls) {
    return executePartitionJoin(
        partitioner,
        partitionId,
        leftFeatures,
        rightFeatures,
        useReferencePointDuplicateAvoidance,
        localJoinNanos,
        partitionJoinCalls,
        null,
        null,
        null,
        null,
        -1,
        null,
        null);
  }

  long executePartitionJoin(SpatialPartitioner partitioner,
                            int partitionId,
                            Iterable<IFeature> leftFeatures,
                            Iterable<IFeature> rightFeatures,
                            boolean useReferencePointDuplicateAvoidance,
                            LongAccumulator localJoinNanos,
                            LongAccumulator partitionJoinCalls,
                            LongAccumulator planeSweepXAxisCalls,
                            LongAccumulator planeSweepYAxisCalls,
                            LongAccumulator planeSweepDiagonalAxisCalls) {
    return executePartitionJoin(
        partitioner,
        partitionId,
        leftFeatures,
        rightFeatures,
        useReferencePointDuplicateAvoidance,
        localJoinNanos,
        partitionJoinCalls,
        planeSweepXAxisCalls,
        planeSweepYAxisCalls,
        planeSweepDiagonalAxisCalls,
        null,
        -1,
        null,
        null);
  }

  long executePartitionJoin(SpatialPartitioner partitioner,
                            int partitionId,
                            Iterable<IFeature> leftFeatures,
                            Iterable<IFeature> rightFeatures,
                            boolean useReferencePointDuplicateAvoidance,
                            LongAccumulator localJoinNanos,
                            LongAccumulator partitionJoinCalls,
                            LongAccumulator planeSweepXAxisCalls,
                            LongAccumulator planeSweepYAxisCalls,
                            LongAccumulator planeSweepDiagonalAxisCalls,
                            String auditExecutionSource,
                            int auditJobId,
                            long[] expectedLeftCountsByPartitionId,
                            long[] expectedRightCountsByPartitionId) {
    long startNanos = System.nanoTime();
    List<IFeature> leftList = materialize(leftFeatures);
    List<IFeature> rightList = materialize(rightFeatures);
    PartitionJoinAuditContext auditContext = createPartitionJoinAuditContext(
        auditExecutionSource,
        auditJobId,
        partitionId,
        expectedLeftCountsByPartitionId,
        expectedRightCountsByPartitionId);
    int loadedLeftCount = leftList.size();
    int loadedRightCount = rightList.size();
    if (leftList.isEmpty() || rightList.isEmpty()) {
      printPartitionJoinAudit(auditContext, partitionId, loadedLeftCount, loadedRightCount, 0L, 0L, 0L);
      if (partitionJoinCalls != null)
        partitionJoinCalls.add(1L);
      if (localJoinNanos != null)
        localJoinNanos.add(System.nanoTime() - startNanos);
      return 0L;
    }
    cn.edu.whu.spatialJoin.geom.EnvelopeNDLite partitionEnvelope = partitioner.getPartitionMBR(partitionId);
    cn.edu.whu.spatialJoin.geom.EnvelopeNDLite partitioningEnvelope = partitioner.getEnvelope();
    final long[] rawCandidateCount = {0L};
    final long[] matchCount = {0L};
    final long[] rejectedByReferencePoint = {0L};
    SpatialJoinAlgorithms.PlaneSweepObserver observer = (axis, implementation) -> {
      if (axis == null)
        return;
      switch (axis) {
        case Y:
          if (planeSweepYAxisCalls != null)
            planeSweepYAxisCalls.add(1L);
          break;
        case DIAGONAL:
          if (planeSweepDiagonalAxisCalls != null)
            planeSweepDiagonalAxisCalls.add(1L);
          break;
        case X:
        default:
          if (planeSweepXAxisCalls != null)
            planeSweepXAxisCalls.add(1L);
          break;
      }
    };
    runConfiguredPartitionJoin(
        leftList,
        rightList,
        (leftIndex, rightIndex, refPoint) -> {
          rawCandidateCount[0]++;
          if (!useReferencePointDuplicateAvoidance ||
              ownsReferencePoint(partitioningEnvelope, partitionEnvelope, refPoint))
            matchCount[0]++;
          else
            rejectedByReferencePoint[0]++;
        },
        observer);
    printPartitionJoinAudit(
        auditContext,
        partitionId,
        loadedLeftCount,
        loadedRightCount,
        rawCandidateCount[0],
        matchCount[0],
        rejectedByReferencePoint[0]);
    if (partitionJoinCalls != null)
      partitionJoinCalls.add(1L);
    if (localJoinNanos != null)
      localJoinNanos.add(System.nanoTime() - startNanos);
    return matchCount[0];
  }

  private Iterator<String> executePartitionJoinObjectPairLines(SpatialPartitioner partitioner,
                                                               int partitionId,
                                                               Iterable<IFeature> leftFeatures,
                                                               Iterable<IFeature> rightFeatures,
                                                               boolean useReferencePointDuplicateAvoidance,
                                                               LongAccumulator planeSweepXAxisCalls,
                                                               LongAccumulator planeSweepYAxisCalls,
                                                               LongAccumulator planeSweepDiagonalAxisCalls,
                                                               String auditExecutionSource,
                                                               int auditJobId,
                                                               long[] expectedLeftCountsByPartitionId,
                                                               long[] expectedRightCountsByPartitionId,
                                                               LongAccumulator emittedPairRecords) {
    List<IFeature> leftList = materialize(leftFeatures);
    List<IFeature> rightList = materialize(rightFeatures);
    PartitionJoinAuditContext auditContext = createPartitionJoinAuditContext(
        auditExecutionSource,
        auditJobId,
        partitionId,
        expectedLeftCountsByPartitionId,
        expectedRightCountsByPartitionId);
    int loadedLeftCount = leftList.size();
    int loadedRightCount = rightList.size();
    if (leftList.isEmpty() || rightList.isEmpty()) {
      printPartitionJoinAudit(auditContext, partitionId, loadedLeftCount, loadedRightCount, 0L, 0L, 0L);
      return Collections.emptyIterator();
    }
    cn.edu.whu.spatialJoin.geom.EnvelopeNDLite partitionEnvelope = partitioner.getPartitionMBR(partitionId);
    cn.edu.whu.spatialJoin.geom.EnvelopeNDLite partitioningEnvelope = partitioner.getEnvelope();
    final long[] rawCandidateCount = {0L};
    final long[] emittedCount = {0L};
    final long[] rejectedByReferencePoint = {0L};
    SpatialJoinAlgorithms.PlaneSweepObserver observer = (axis, implementation) -> {
      if (axis == null)
        return;
      switch (axis) {
        case Y:
          if (planeSweepYAxisCalls != null)
            planeSweepYAxisCalls.add(1L);
          break;
        case DIAGONAL:
          if (planeSweepDiagonalAxisCalls != null)
            planeSweepDiagonalAxisCalls.add(1L);
          break;
        case X:
        default:
          if (planeSweepXAxisCalls != null)
            planeSweepXAxisCalls.add(1L);
          break;
      }
    };
    File spillFile = null;
    BufferedWriter writer = null;
    try {
      spillFile = createTaskLocalObjectPairSpillFile();
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(spillFile), StandardCharsets.UTF_8));
      final BufferedWriter pairWriter = writer;
      runConfiguredPartitionJoin(
          leftList,
          rightList,
          (leftIndex, rightIndex, refPoint) -> {
            rawCandidateCount[0]++;
            if (useReferencePointDuplicateAvoidance &&
                !ownsReferencePoint(partitioningEnvelope, partitionEnvelope, refPoint)) {
              rejectedByReferencePoint[0]++;
              return;
            }
            Long leftOid = readJoinOid(leftList, leftIndex);
            Long rightOid = readJoinOid(rightList, rightIndex);
            if (leftOid == null || rightOid == null)
              return;
            try {
              pairWriter.write(Long.toString(leftOid));
              pairWriter.write('\t');
              pairWriter.write(Long.toString(rightOid));
              pairWriter.newLine();
              emittedCount[0]++;
              if (emittedPairRecords != null)
                emittedPairRecords.add(1L);
            } catch (IOException e) {
              throw new RuntimeException("Failed to spill object-pair record", e);
            }
          },
          observer);
      writer.close();
      writer = null;
      printPartitionJoinAudit(
          auditContext,
          partitionId,
          loadedLeftCount,
          loadedRightCount,
          rawCandidateCount[0],
          emittedCount[0],
          rejectedByReferencePoint[0]);
      return new DeletingLineIterator(spillFile);
    } catch (IOException e) {
      deleteFileQuietly(spillFile);
      throw new RuntimeException("Failed to create task-local object-pair spill file", e);
    } catch (RuntimeException e) {
      closeQuietly(writer);
      deleteFileQuietly(spillFile);
      throw e;
    } finally {
      closeQuietly(writer);
    }
  }

  /**
   * Runs one partition-local join and writes original object-id pairs directly to one output file.
   *
   * <p>This is used by the compact object-pair execution path. It avoids {@code cogroup} and
   * {@code saveAsTextFile}, so one task unit can write pair files in a single no-shuffle Spark stage.
   */
  public long writePartitionJoinObjectPairLines(SpatialPartitioner partitioner,
                                                int partitionId,
                                                Iterable<IFeature> leftFeatures,
                                                Iterable<IFeature> rightFeatures,
                                                boolean useReferencePointDuplicateAvoidance,
                                                LongAccumulator planeSweepXAxisCalls,
                                                LongAccumulator planeSweepYAxisCalls,
                                                LongAccumulator planeSweepDiagonalAxisCalls,
                                                String auditExecutionSource,
                                                int auditJobId,
                                                long[] expectedLeftCountsByPartitionId,
                                                long[] expectedRightCountsByPartitionId,
                                                String outputFilePath) {
    List<IFeature> leftList = materialize(leftFeatures);
    List<IFeature> rightList = materialize(rightFeatures);
    PartitionJoinAuditContext auditContext = createPartitionJoinAuditContext(
        auditExecutionSource,
        auditJobId,
        partitionId,
        expectedLeftCountsByPartitionId,
        expectedRightCountsByPartitionId);
    int loadedLeftCount = leftList.size();
    int loadedRightCount = rightList.size();
    if (leftList.isEmpty() || rightList.isEmpty()) {
      printPartitionJoinAudit(auditContext, partitionId, loadedLeftCount, loadedRightCount, 0L, 0L, 0L);
      return 0L;
    }
    cn.edu.whu.spatialJoin.geom.EnvelopeNDLite partitionEnvelope = partitioner.getPartitionMBR(partitionId);
    cn.edu.whu.spatialJoin.geom.EnvelopeNDLite partitioningEnvelope = partitioner.getEnvelope();
    final long[] rawCandidateCount = {0L};
    final long[] emittedCount = {0L};
    final long[] rejectedByReferencePoint = {0L};
    SpatialJoinAlgorithms.PlaneSweepObserver observer = (axis, implementation) -> {
      if (axis == null)
        return;
      switch (axis) {
        case Y:
          if (planeSweepYAxisCalls != null)
            planeSweepYAxisCalls.add(1L);
          break;
        case DIAGONAL:
          if (planeSweepDiagonalAxisCalls != null)
            planeSweepDiagonalAxisCalls.add(1L);
          break;
        case X:
        default:
          if (planeSweepXAxisCalls != null)
            planeSweepXAxisCalls.add(1L);
          break;
      }
    };
    try (BufferedWriter writer = createObjectPairOutputWriter(outputFilePath)) {
      runConfiguredPartitionJoin(
          leftList,
          rightList,
          (leftIndex, rightIndex, refPoint) -> {
            rawCandidateCount[0]++;
            if (useReferencePointDuplicateAvoidance &&
                !ownsReferencePoint(partitioningEnvelope, partitionEnvelope, refPoint)) {
              rejectedByReferencePoint[0]++;
              return;
            }
            Long leftOid = readJoinOid(leftList, leftIndex);
            Long rightOid = readJoinOid(rightList, rightIndex);
            if (leftOid == null || rightOid == null)
              return;
            try {
              writer.write(Long.toString(leftOid));
              writer.write('\t');
              writer.write(Long.toString(rightOid));
              writer.newLine();
              emittedCount[0]++;
            } catch (IOException e) {
              throw new RuntimeException("Failed to write object-pair record to " + outputFilePath, e);
            }
          },
          observer);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write object-pair output file " + outputFilePath, e);
    }
    printPartitionJoinAudit(
        auditContext,
        partitionId,
        loadedLeftCount,
        loadedRightCount,
        rawCandidateCount[0],
        emittedCount[0],
        rejectedByReferencePoint[0]);
    return emittedCount[0];
  }

  private static BufferedWriter createObjectPairOutputWriter(String outputFilePath) throws IOException {
    if (outputFilePath == null || outputFilePath.trim().isEmpty())
      throw new IOException("Empty object-pair output file path");
    Path outputPath = new Path(outputFilePath);
    FileSystem fileSystem = outputPath.getFileSystem(new Configuration());
    Path parent = outputPath.getParent();
    if (parent != null)
      fileSystem.mkdirs(parent);
    return new BufferedWriter(new OutputStreamWriter(fileSystem.create(outputPath, true), StandardCharsets.UTF_8));
  }

  private void runConfiguredPartitionJoin(List<IFeature> leftList,
                                          List<IFeature> rightList,
                                          SpatialJoinAlgorithms.SJResult callback,
                                          SpatialJoinAlgorithms.PlaneSweepObserver observer) {
    if (isJoinSimplificationEnabled()) {
      SpatialJoinAlgorithms.spatialJoinIntersectsWithSimplification2(
          leftList,
          rightList,
          getSimplificationThreshold(),
          callback,
          null,
          getPlaneSweepAxis(),
          getPlaneSweepImplementation(),
          observer);
    } else {
      SpatialJoinAlgorithms.spatialJoinIntersectsBinaryFilterRefine(
          leftList,
          rightList,
          callback,
          null,
          getPlaneSweepAxis(),
          getPlaneSweepImplementation(),
          observer);
    }
  }

  private static Long readJoinOid(List<IFeature> features, int index) {
    if (features == null || index < 0 || index >= features.size())
      return null;
    IFeature feature = features.get(index);
    if (feature == null)
      return null;
    try {
      int oidIndex = feature.fieldIndex(SpatialJoinConstants.Operator.JOIN_OID_ATTRIBUTE);
      if (oidIndex < 0 || feature.isNullAt(oidIndex))
        return null;
      Object value = feature.get(oidIndex);
      if (value instanceof Number)
        return ((Number) value).longValue();
      if (value != null)
        return Long.parseLong(value.toString());
    } catch (RuntimeException ignored) {
      return null;
    }
    return null;
  }

  /**
   * Returns whether the current partition is the unique owner of a duplicate-avoidance reference point.
   *
   * <p>The pair belongs to the partition that contains the lower-left corner of the two feature MBRs'
   * intersection. Partition envelopes are
   * treated as half-open on their upper bounds, except for the global maximum bound, so boundary points
   * still have one deterministic owner without re-routing through a potentially pruned effective view.
   */
  private static boolean ownsReferencePoint(EnvelopeNDLite partitioningEnvelope,
                                            EnvelopeNDLite partitionEnvelope,
                                            double[] refPoint) {
    return containsReferencePoint(partitioningEnvelope, partitionEnvelope, refPoint);
  }

  private static boolean containsReferencePoint(EnvelopeNDLite globalEnvelope,
                                                EnvelopeNDLite envelope,
                                                double[] point) {
    if (envelope == null || point == null || envelope.isEmpty())
      return false;
    int dimensions = Math.min(envelope.getCoordinateDimension(), point.length);
    for (int d = 0; d < dimensions; d++) {
      double value = point[d];
      if (Double.isNaN(value) || value < envelope.getMinCoord(d) || value > envelope.getMaxCoord(d))
        return false;
      if (value == envelope.getMaxCoord(d) &&
          (globalEnvelope == null ||
              globalEnvelope.getCoordinateDimension() <= d ||
              value != globalEnvelope.getMaxCoord(d))) {
        return false;
      }
    }
    return dimensions > 0;
  }

  private static LongAccumulator createJobAccumulator(JavaSparkContext sparkContext, String metricPrefix, int jobId) {
    if (sparkContext == null)
      return null;
    String safePrefix = metricPrefix == null || metricPrefix.trim().isEmpty() ? "sj.metric" : metricPrefix.trim();
    return sparkContext.sc().longAccumulator(safePrefix + "." + jobId + "." + System.nanoTime());
  }

  private static List<IFeature> materialize(Iterable<IFeature> features) {
    if (features == null) {
      return new ArrayList<>();
    }
    if (features instanceof List)
      return (List<IFeature>) features;
    List<IFeature> values = new ArrayList<>();
    for (IFeature feature : features) {
      values.add(feature);
    }
    return values;
  }

  private static void closeBroadcastQuietly(Broadcast<?> broadcast) {
    if (broadcast == null)
      return;
    try {
      broadcast.destroy();
    } catch (Exception ignored) {
      try {
        broadcast.unpersist(true);
      } catch (Exception ignoredAgain) {
        // Ignore cleanup failures during shutdown or failed prepare unwinding.
      }
    }
  }

  /**
   * Builds an input-scoped options object for one side of the join.
   *
   * @param path input path
   * @param format input format
   * @return derived options object for a single input
   */
  /** Builds one input-scoped option object for a single dataset side. */
  private SpatialJoinOptions buildInputOptions(String path, String format) {
    SpatialJoinOptions inputOptions = new SpatialJoinOptions(options);
    inputOptions.set(SpatialJoinConstants.Operator.INPUT_PATH, path);
    inputOptions.set(SpatialJoinConstants.Operator.INPUT_FORMAT, format);
    return inputOptions;
  }

  /** Normalizes null or blank format strings to the auto-detection token. */
  private static String normalizeFormat(String format) {
    return format == null || format.trim().isEmpty() ? "*auto*" : format.trim();
  }

  /**
   * Materializes one persisted {@code (oid, feature)} RDD from the given feature RDD.
   *
   * The storage level is serialized and spillable so later per-job reloads can reuse the
   * same binary records without repeatedly touching the original input files.
   */
  private JavaPairRDD<Long, IFeature> prepareFeaturesByOid(JavaRDD<IFeature> features) {
    if (features == null)
      return null;
    return features
        .zipWithUniqueId()
        .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
        .persist(resolvePreparedFeatureStorageLevel());
  }

  /**
   * Materializes one spillable raw-feature staging RDD for disk-hybrid preparation.
   *
   * <p>Unlike the memory-oriented `(oid, feature)` path, the disk-hybrid dual-replication prepare
   * chain never replays features by OID. It only needs multiple passes over raw features until the
   * final partition files are written, so keeping a plain {@code JavaRDD<IFeature>} on disk is the
   * lightest stable staging representation.
   */
  private JavaRDD<IFeature> prepareDiskHybridStagingFeatures(JavaRDD<IFeature> features) {
    if (features == null)
      return null;
    return features.persist(StorageLevel.DISK_ONLY());
  }

  /**
   * Optionally splits complex geometries before summaries, candidate partitions, partition-load
   * profiles, and materialized files are built.
   *
   * <p>This keeps the whole disk-hybrid preparation chain in one consistent fragment-aware view.
   * In contrast to the removed materialization-time split, fragment counts and fragment loads are
   * visible before partition construction and job packing, so the compute phase does not have to
   * create extra temporary geometry fragments.
   */
  private JavaRDD<IFeature> preparePrePartitionSplitFeaturesIfConfigured(JavaRDD<IFeature> features) {
    if (features == null || !isPrePartitionSplitEnabled())
      return features;
    final int splitThreshold = getSimplificationThreshold();
    final int maxFragments = getPrePartitionSplitMaxFragments();
    final int maxDepth = getPrePartitionSplitMaxDepth();
    return features
        .zipWithUniqueId()
        .flatMap((FlatMapFunction<Tuple2<IFeature, Long>, IFeature>) tuple ->
            splitFeatureBeforePartition(
                tuple == null ? null : tuple._1,
                tuple == null ? -1L : tuple._2,
                splitThreshold,
                maxFragments,
                maxDepth))
        .persist(StorageLevel.DISK_ONLY());
  }

  private static Iterator<IFeature> splitFeatureBeforePartition(IFeature feature,
                                                                long oid,
                                                                int splitThreshold,
                                                                int maxFragments,
                                                                int maxDepth) {
    if (feature == null)
      return Collections.emptyIterator();
    Geometry geometry;
    try {
      geometry = feature.getGeometry();
    } catch (RuntimeException e) {
      return Collections.emptyIterator();
    }
    if (geometry == null || geometry.isEmpty() || GeometryHelper.getCoordinateDimension(geometry) != 2) {
      return Collections.singletonList((IFeature) Feature.projectToGeometryAndOid(feature, oid)).iterator();
    }
    int threshold = Math.max(1, splitThreshold);
    if (geometry.getNumPoints() <= threshold) {
      return Collections.singletonList((IFeature) Feature.projectToGeometryAndOid(feature, oid)).iterator();
    }
    List<Geometry> fragments = SpatialJoinAlgorithms.quadSplitBounded(
        geometry,
        threshold,
        maxFragments,
        maxDepth);
    if (fragments == null || fragments.isEmpty()) {
      return Collections.singletonList((IFeature) Feature.projectToGeometryAndOid(feature, oid)).iterator();
    }
    List<IFeature> output = new ArrayList<>(fragments.size());
    for (Geometry fragment : fragments) {
      if (fragment == null || fragment.isEmpty())
        continue;
      output.add(Feature.projectGeometryAndOid(fragment, oid));
    }
    if (output.isEmpty())
      output.add(Feature.projectToGeometryAndOid(feature, oid));
    return output.iterator();
  }

  private StorageLevel resolvePreparedFeatureStorageLevel() {
    if (SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID.equalsIgnoreCase(getWorkingSetMode()))
      return DISK_HYBRID_PREPARED_FEATURE_STORAGE_LEVEL;
    return PREPARED_FEATURE_STORAGE_LEVEL;
  }

  /**
   * Materializes an iterator into compact join-time features.
   *
   * <p>Only geometry and {@value SpatialJoinConstants.Operator#JOIN_OID_ATTRIBUTE}
   * are retained to reduce in-memory footprint before the actual join starts.
   *
   * @param iterator feature iterator
   * @return materialized join-ready feature list
   */
  private static List<IFeature> materializeJoinReady(java.util.Iterator<IFeature> iterator) {
    List<IFeature> features = new ArrayList<>();
    long oid = 0L;
    while (iterator.hasNext()) {
      features.add(Feature.projectToGeometryAndOid(iterator.next(), oid++));
    }
    return features;
  }

  /**
   * Immutable bundle that captures the partition-construction result shared by memory mode and
   * disk-hybrid mode before the final working-set representation diverges.
   */
  public static final class PartitionPreparationContext {
    private final Summary leftSummary;
    private final Summary rightSummary;
    private final long leftFeatureCount;
    private final long rightFeatureCount;
    private final SpatialPartitioner.EffectivePlan effectivePlan;
    private final String partitionAssignmentStrategy;
    private final String anchorSide;

    private PartitionPreparationContext(Summary leftSummary,
                                        Summary rightSummary,
                                        long leftFeatureCount,
                                        long rightFeatureCount,
                                        SpatialPartitioner.EffectivePlan effectivePlan,
                                        String partitionAssignmentStrategy,
                                        String anchorSide) {
      this.leftSummary = leftSummary == null ? new Summary() : leftSummary;
      this.rightSummary = rightSummary == null ? new Summary() : rightSummary;
      this.leftFeatureCount = Math.max(0L, leftFeatureCount);
      this.rightFeatureCount = Math.max(0L, rightFeatureCount);
      this.effectivePlan = effectivePlan;
      this.partitionAssignmentStrategy = partitionAssignmentStrategy == null
          ? SpatialJoinConstants.Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY
          : partitionAssignmentStrategy;
      this.anchorSide = anchorSide == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : anchorSide;
    }

    public Summary getLeftSummary() {
      return leftSummary;
    }

    public Summary getRightSummary() {
      return rightSummary;
    }

    public long getLeftFeatureCount() {
      return leftFeatureCount;
    }

    public long getRightFeatureCount() {
      return rightFeatureCount;
    }

    public SpatialPartitioner.EffectivePlan getEffectivePlan() {
      return effectivePlan;
    }

    public String getPartitionAssignmentStrategy() {
      return partitionAssignmentStrategy;
    }

    public String getAnchorSide() {
      return anchorSide;
    }
  }

  /**
   * Lightweight synopsis-driven candidate partition-construction context for disk-hybrid
   * dual-replication preparation.
   *
   * <p>This context intentionally stops before exact partition statistics are collected so the next
   * stage can fuse exact stats collection with candidate-file materialization in one full scan of
   * the raw staged features.
   */
  private static final class SynopsisCandidatePreparationContext {
    private final Summary leftSummary;
    private final Summary rightSummary;
    private final long leftFeatureCount;
    private final long rightFeatureCount;
    private final SpatialPartitioner candidatePartitioner;
    private final String partitionAssignmentStrategy;
    private final String anchorSide;

    private SynopsisCandidatePreparationContext(Summary leftSummary,
                                                Summary rightSummary,
                                                long leftFeatureCount,
                                                long rightFeatureCount,
                                                SpatialPartitioner candidatePartitioner,
                                                String partitionAssignmentStrategy,
                                                String anchorSide) {
      this.leftSummary = leftSummary == null ? new Summary() : leftSummary;
      this.rightSummary = rightSummary == null ? new Summary() : rightSummary;
      this.leftFeatureCount = Math.max(0L, leftFeatureCount);
      this.rightFeatureCount = Math.max(0L, rightFeatureCount);
      this.candidatePartitioner = candidatePartitioner;
      this.partitionAssignmentStrategy = partitionAssignmentStrategy == null
          ? SpatialJoinConstants.Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY
          : partitionAssignmentStrategy;
      this.anchorSide = anchorSide == null ? PartitionAssignmentStrategyHelper.ANCHOR_SIDE_NONE : anchorSide;
    }
  }

  /**
   * Holds one materialized `(partitionId, feature)` working set plus any short-lived broadcast
   * resource that must survive until the prepared execution context is closed.
   */
  private static final class PreparedPartitionBuildResult {
    private final JavaPairRDD<Integer, IFeature> partitionedFeatures;
    private final Broadcast<SpatialPartitioner.AssignmentLookupTable> lookupBroadcast;

    private PreparedPartitionBuildResult(JavaPairRDD<Integer, IFeature> partitionedFeatures,
                                         Broadcast<SpatialPartitioner.AssignmentLookupTable> lookupBroadcast) {
      this.partitionedFeatures = partitionedFeatures;
      this.lookupBroadcast = lookupBroadcast;
    }

    private JavaPairRDD<Integer, IFeature> getPartitionedFeatures() {
      return partitionedFeatures;
    }

    private Broadcast<SpatialPartitioner.AssignmentLookupTable> getLookupBroadcast() {
      return lookupBroadcast;
    }

    private void closeQuietly() {
      closeBroadcastQuietly(lookupBroadcast);
      if (partitionedFeatures != null) {
        try {
          partitionedFeatures.unpersist(true);
        } catch (Exception ignored) {
          // Ignore cleanup failures while unwinding a failed prepare path.
        }
      }
    }
  }

  /**
   * Lazily expands one iterator of cached `(oid, feature)` pairs into `(partitionId, feature)` pairs
   * by consulting the compact broadcast lookup table.
   *
   * <p>The iterator keeps only one source feature and its partition-id slice in memory at a time,
   * which avoids creating a temporary list per source feature during the final materialization pass.
   */
  private static final class BroadcastAssignedFeatureIterator implements Iterator<Tuple2<Integer, IFeature>> {
    private final Iterator<Tuple2<Long, IFeature>> source;
    private final SpatialPartitioner.AssignmentLookupTable lookupTable;

    private Tuple2<Long, IFeature> currentFeature;
    private int nextPartitionOffset = -1;
    private int currentPartitionEnd = -1;

    private BroadcastAssignedFeatureIterator(Iterator<Tuple2<Long, IFeature>> source,
                                             SpatialPartitioner.AssignmentLookupTable lookupTable) {
      this.source = source;
      this.lookupTable = lookupTable;
    }

    @Override
    public boolean hasNext() {
      advance();
      return currentFeature != null && nextPartitionOffset >= 0 && nextPartitionOffset < currentPartitionEnd;
    }

    @Override
    public Tuple2<Integer, IFeature> next() {
      if (!hasNext())
        throw new NoSuchElementException("No more partition-assigned features");
      Tuple2<Integer, IFeature> result =
          new Tuple2<>(lookupTable.getPartitionIdAt(nextPartitionOffset++), currentFeature._2);
      if (nextPartitionOffset >= currentPartitionEnd) {
        currentFeature = null;
        nextPartitionOffset = -1;
        currentPartitionEnd = -1;
      }
      return result;
    }

    private void advance() {
      while ((currentFeature == null || nextPartitionOffset >= currentPartitionEnd) && source.hasNext()) {
        Tuple2<Long, IFeature> candidate = source.next();
        int oidIndex = lookupTable.findOidIndex(candidate._1);
        if (oidIndex < 0)
          continue;
        int partitionStart = lookupTable.getPartitionStartOffset(oidIndex);
        int partitionEnd = lookupTable.getPartitionEndOffset(oidIndex);
        if (partitionStart < 0 || partitionStart >= partitionEnd)
          continue;
        currentFeature = candidate;
        nextPartitionOffset = partitionStart;
        currentPartitionEnd = partitionEnd;
        return;
      }
      if (currentFeature == null || nextPartitionOffset >= currentPartitionEnd) {
        currentFeature = null;
        nextPartitionOffset = -1;
        currentPartitionEnd = -1;
      }
    }
  }

  private static void unpersistQuietly(JavaPairRDD<?, ?> rdd) {
    if (rdd == null)
      return;
    try {
      rdd.unpersist(true);
    } catch (Exception ignored) {
      // Best-effort cleanup during normal completion and error unwinding.
    }
  }

  private static void unpersistQuietly(JavaRDD<?> rdd) {
    if (rdd == null)
      return;
    try {
      rdd.unpersist(true);
    } catch (Exception ignored) {
      // Best-effort cleanup during normal completion and error unwinding.
    }
  }

  /**
   * Lazily expands one iterator of cached `(oid, feature)` pairs into `(partitionId, feature)` pairs
   * by evaluating overlap against the final effective partitioner directly.
   */
  private static final class DirectAssignedFeatureIterator implements Iterator<Tuple2<Integer, IFeature>> {
    private final Iterator<Tuple2<Long, IFeature>> source;
    private final SpatialPartitioner partitioner;
    private final IntArray matchedPartitions = new IntArray();

    private Tuple2<Long, IFeature> currentFeature;
    private int nextPartitionIndex = -1;

    private DirectAssignedFeatureIterator(Iterator<Tuple2<Long, IFeature>> source,
                                          SpatialPartitioner partitioner) {
      this.source = source;
      this.partitioner = partitioner;
    }

    @Override
    public boolean hasNext() {
      advance();
      return currentFeature != null && nextPartitionIndex >= 0 && nextPartitionIndex < matchedPartitions.size();
    }

    @Override
    public Tuple2<Integer, IFeature> next() {
      if (!hasNext())
        throw new NoSuchElementException("No more directly partition-assigned features");
      Tuple2<Integer, IFeature> result =
          new Tuple2<>(matchedPartitions.get(nextPartitionIndex++), currentFeature._2);
      if (nextPartitionIndex >= matchedPartitions.size()) {
        currentFeature = null;
        nextPartitionIndex = -1;
        matchedPartitions.clear();
      }
      return result;
    }

    private void advance() {
      while ((currentFeature == null || nextPartitionIndex >= matchedPartitions.size()) && source.hasNext()) {
        Tuple2<Long, IFeature> candidate = source.next();
        if (candidate == null || candidate._2 == null)
          continue;
        matchedPartitions.clear();
        AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
            candidate._2,
            candidate._1,
            candidate._2.getStorageSize(),
            candidate._2.getHeapEstimateBytes(),
            null);
        if (abstractFeature.isEmpty())
          continue;
        partitioner.overlapPartitions(abstractFeature, matchedPartitions);
        if (matchedPartitions.isEmpty())
          continue;
        currentFeature = candidate;
        nextPartitionIndex = 0;
        return;
      }
      if (currentFeature == null || nextPartitionIndex >= matchedPartitions.size()) {
        currentFeature = null;
        nextPartitionIndex = -1;
        matchedPartitions.clear();
      }
    }
  }

  /**
   * Lazily expands one iterator of raw features into `(partitionId, feature)` pairs by evaluating
   * overlap against the final effective partitioner directly, without creating temporary OIDs.
   */
  private static final class DirectAssignedRawFeatureIterator implements Iterator<Tuple2<Integer, IFeature>> {
    private final Iterator<IFeature> source;
    private final SpatialPartitioner partitioner;
    private final IntArray matchedPartitions = new IntArray();

    private IFeature currentFeature;
    private int nextPartitionIndex = -1;

    private DirectAssignedRawFeatureIterator(Iterator<IFeature> source,
                                             SpatialPartitioner partitioner) {
      this.source = source;
      this.partitioner = partitioner;
    }

    @Override
    public boolean hasNext() {
      advance();
      return currentFeature != null && nextPartitionIndex >= 0 && nextPartitionIndex < matchedPartitions.size();
    }

    @Override
    public Tuple2<Integer, IFeature> next() {
      if (!hasNext())
        throw new NoSuchElementException("No more directly partition-assigned raw features");
      Tuple2<Integer, IFeature> result =
          new Tuple2<>(matchedPartitions.get(nextPartitionIndex++), currentFeature);
      if (nextPartitionIndex >= matchedPartitions.size()) {
        currentFeature = null;
        nextPartitionIndex = -1;
        matchedPartitions.clear();
      }
      return result;
    }

    private void advance() {
      while ((currentFeature == null || nextPartitionIndex >= matchedPartitions.size()) && source.hasNext()) {
        IFeature candidate = source.next();
        if (candidate == null)
          continue;
        matchedPartitions.clear();
        AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
            candidate,
            -1L,
            candidate.getStorageSize(),
            candidate.getHeapEstimateBytes(),
            null);
        if (abstractFeature.isEmpty())
          continue;
        partitioner.overlapPartitions(abstractFeature, matchedPartitions);
        if (matchedPartitions.isEmpty())
          continue;
        currentFeature = candidate;
        nextPartitionIndex = 0;
        return;
      }
      if (currentFeature == null || nextPartitionIndex >= matchedPartitions.size()) {
        currentFeature = null;
        nextPartitionIndex = -1;
        matchedPartitions.clear();
      }
    }
  }

  /** Streams one compact descriptor per raw feature into the disk-only preparation cache. */
  private static final class RefinementDescriptorIterator implements Iterator<AbstractFeature> {
    private final Iterator<IFeature> source;
    private final String complexityMode;
    private final String loadEstimateMode;
    private final boolean trackRepresentativeMoments;
    private AbstractFeature nextRecord;

    private RefinementDescriptorIterator(
        Iterator<IFeature> source,
        String complexityMode,
        String loadEstimateMode,
        boolean trackRepresentativeMoments) {
      this.source = source;
      this.complexityMode = complexityMode;
      this.loadEstimateMode = loadEstimateMode;
      this.trackRepresentativeMoments = trackRepresentativeMoments;
    }

    @Override
    public boolean hasNext() {
      advance();
      return nextRecord != null;
    }

    @Override
    public AbstractFeature next() {
      if (!hasNext())
        throw new NoSuchElementException("No more disk-backed refinement descriptors");
      AbstractFeature result = nextRecord;
      nextRecord = null;
      return result;
    }

    private void advance() {
      if (nextRecord != null)
        return;
      while (source.hasNext()) {
        IFeature feature = source.next();
        if (feature == null)
          continue;
        AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
            feature,
            -1L,
            feature.getStorageSize(),
            Math.max(0L, feature.getExecutionLoadEstimateBytes(loadEstimateMode)),
            complexityMode);
        if (abstractFeature == null || abstractFeature.isEmpty())
          continue;
        if (trackRepresentativeMoments)
          abstractFeature.withExactRepresentativeMoments();
        nextRecord = abstractFeature;
        return;
      }
    }
  }

  /** Replicates cached descriptors to all intersecting initial candidate leaves. */
  private static final class DirectAssignedRefinementFeatureIterator
      implements Iterator<Tuple2<Integer, SidedAbstractFeature>> {
    private final Iterator<AbstractFeature> source;
    private final SpatialPartitioner partitioner;
    private final int side;
    private final IntArray matchedPartitions = new IntArray();
    private AbstractFeature currentFeature;
    private int nextPartitionIndex = -1;

    private DirectAssignedRefinementFeatureIterator(
        Iterator<AbstractFeature> source,
        SpatialPartitioner partitioner,
        int side) {
      this.source = source;
      this.partitioner = partitioner;
      if (side < 0 || side >= FRONTIER_SIDE_COUNT)
        throw new IllegalArgumentException("Unsupported frontier side: " + side);
      this.side = side;
    }

    @Override
    public boolean hasNext() {
      advance();
      return currentFeature != null
          && nextPartitionIndex >= 0
          && nextPartitionIndex < matchedPartitions.size();
    }

    @Override
    public Tuple2<Integer, SidedAbstractFeature> next() {
      if (!hasNext())
        throw new NoSuchElementException("No more initial refinement-frontier features");
      int partitionId = matchedPartitions.get(nextPartitionIndex++);
      Tuple2<Integer, SidedAbstractFeature> result = new Tuple2<>(
          frontierKey(partitionId, side),
          new SidedAbstractFeature(side, currentFeature));
      if (nextPartitionIndex >= matchedPartitions.size()) {
        currentFeature = null;
        nextPartitionIndex = -1;
        matchedPartitions.clear();
      }
      return result;
    }

    private void advance() {
      while ((currentFeature == null || nextPartitionIndex >= matchedPartitions.size())
          && source.hasNext()) {
        AbstractFeature abstractFeature = source.next();
        if (abstractFeature == null || abstractFeature.isEmpty())
          continue;
        matchedPartitions.clear();
        partitioner.overlapPartitions(abstractFeature, matchedPartitions);
        if (matchedPartitions.isEmpty())
          continue;
        currentFeature = abstractFeature;
        nextPartitionIndex = 0;
        return;
      }
      if (currentFeature == null || nextPartitionIndex >= matchedPartitions.size()) {
        currentFeature = null;
        nextPartitionIndex = -1;
        matchedPartitions.clear();
      }
    }
  }

  /** Streams descriptors from split parents to all intersecting direct children. */
  private static final class RedistributedFrontierFeatureIterator
      implements Iterator<Tuple2<Integer, SidedAbstractFeature>> {
    private final Iterator<TaggedFrontierFeature> source;
    private final Map<Integer, FrontierSplitParent> splitParentsByIndex;
    private TaggedFrontierFeature current;
    private List<FrontierSplitChild> currentChildren;
    private int nextChildIndex = -1;
    private Tuple2<Integer, SidedAbstractFeature> nextRecord;

    private RedistributedFrontierFeatureIterator(
        Iterator<TaggedFrontierFeature> source,
        Map<Integer, FrontierSplitParent> splitParentsByIndex) {
      this.source = source;
      this.splitParentsByIndex = splitParentsByIndex;
    }

    @Override
    public boolean hasNext() {
      advance();
      return nextRecord != null;
    }

    @Override
    public Tuple2<Integer, SidedAbstractFeature> next() {
      if (!hasNext())
        throw new NoSuchElementException("No more split-frontier features");
      Tuple2<Integer, SidedAbstractFeature> result = nextRecord;
      nextRecord = null;
      return result;
    }

    private void advance() {
      if (nextRecord != null)
        return;
      while (true) {
        while (current != null
            && current.sidedFeature != null
            && current.sidedFeature.feature != null
            && currentChildren != null
            && nextChildIndex >= 0
            && nextChildIndex < currentChildren.size()) {
          FrontierSplitChild child = currentChildren.get(nextChildIndex++);
          if (child.envelope.intersectsEnvelopeClosed(current.sidedFeature.feature)) {
            nextRecord = new Tuple2<>(
                frontierKey(child.slot, current.sidedFeature.side),
                current.sidedFeature);
            return;
          }
        }
        current = null;
        currentChildren = null;
        nextChildIndex = -1;
        if (!source.hasNext())
          return;
        TaggedFrontierFeature candidate = source.next();
        if (candidate == null
            || candidate.sidedFeature == null
            || candidate.sidedFeature.feature == null)
          continue;
        FrontierSplitParent parent = splitParentsByIndex.get(candidate.parentIndex);
        if (parent == null || parent.children == null || parent.children.isEmpty())
          continue;
        current = candidate;
        currentChildren = parent.children;
        nextChildIndex = 0;
      }
    }
  }

  private SpatialPartitioner.EffectivePlan stripAssignments(SpatialPartitioner.EffectivePlan effectivePlan) {
    if (effectivePlan == null)
      return null;
    return new SpatialPartitioner.EffectivePlan(
        effectivePlan.getPartitioner(),
        effectivePlan.getPartitionProfiles(),
        Collections.emptyList(),
        Collections.emptyList());
  }

  @FunctionalInterface
  private interface SparkJobAction<T> {
    T run();
  }

  private <T> T runWithJobGroup(JavaSparkContext sparkContext,
                                JobGroup jobGroup,
                                SparkJobAction<T> action) {
    if (action == null)
      return null;
    if (sparkContext == null || jobGroup == null)
      return action.run();
    String groupId = "sj-job-" + jobGroup.getJobId();
    String description = String.format(
        "SpatialJoin jobId=%d type=%s partitions=%s loadBytes=%d",
        jobGroup.getJobId(),
        jobGroup.getComplexityType(),
        Arrays.toString(jobGroup.getPartitionIds()),
        jobGroup.getTotalMemoryLoadBytes());
    return runWithSparkJobGroup(sparkContext, groupId, description, action);
  }

  private <T> T runWithSparkJobGroup(JavaSparkContext sparkContext,
                                     String groupId,
                                     String description,
                                     SparkJobAction<T> action) {
    if (action == null)
      return null;
    if (sparkContext == null || groupId == null || description == null)
      return action.run();
    sparkContext.sc().setJobGroup(groupId, description, false);
    try {
      return action.run();
    } finally {
      sparkContext.sc().clearJobGroup();
    }
  }
}
