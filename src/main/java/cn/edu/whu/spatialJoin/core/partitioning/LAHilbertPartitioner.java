package cn.edu.whu.spatialJoin.core.partitioning;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.common.Summary;
import cn.edu.whu.spatialJoin.core.algorithms.HCurveSortable;
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeature;
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeatureHierarchy;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper;
import cn.edu.whu.spatialJoin.synopses.AbstractHistogram;
import cn.edu.whu.spatialJoin.utils.IntArray;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A preliminary load-aware Hilbert partitioner design.
 *
 * <p>This implementation follows the current codebase abstractions:
 * it reuses {@link HCurveSortable} for Hilbert ordering and the existing
 * {@link SpatialPartitioner} contract for setup/construct/query methods.
 *
 * <p>The full design in the document contains two phases:
 * 1. minimize the global domain and derive an initial Hilbert granularity from memory constraints
 * 2. refine heavy partitions using memory/load feedback
 *
 * <p>With the current interface, this first version focuses on:
 * - accepting a precomputed overlap domain, or deriving it as the intersection of left/right dataset MBRs
 * - honoring an explicitly configured initial Hilbert order {@code l} for the first grid
 * - refining overloaded cells recursively
 * - assigning hierarchical partition codes to the resulting leaf partitions
 *
 * <p>Code assignment follows the current document requirement:
 * - the first-level partitions are ordered by the Hilbert curve
 * - whenever a partition is refined, its children are sorted along the split direction
 * - each child code is generated as parentCode + "." + orderCode
 *
 * <p>The second-phase CAM-based refinement is intentionally left as an extension point,
 * but the hierarchical coding utilities are implemented here so later refinement can reuse them directly.
 */
@SpatialPartitioner.Metadata(
    disjointSupported = true,
    extension = "lahilbert",
    description = "Load-aware Hilbert partitioner with configurable global domain and memory-driven initial granularity")
public class LAHilbertPartitioner extends HCurvePartitioner {

  public static final String GLOBAL_MIN_COORDS = SpatialJoinConstants.LAHilbert.GLOBAL_MIN_COORDS;
  public static final String GLOBAL_MAX_COORDS = SpatialJoinConstants.LAHilbert.GLOBAL_MAX_COORDS;
  public static final String LEFT_MIN_COORDS = SpatialJoinConstants.LAHilbert.LEFT_MIN_COORDS;
  public static final String LEFT_MAX_COORDS = SpatialJoinConstants.LAHilbert.LEFT_MAX_COORDS;
  public static final String RIGHT_MIN_COORDS = SpatialJoinConstants.LAHilbert.RIGHT_MIN_COORDS;
  public static final String RIGHT_MAX_COORDS = SpatialJoinConstants.LAHilbert.RIGHT_MAX_COORDS;
  public static final String LEFT_SUMMARY_SIZE = SpatialJoinConstants.LAHilbert.LEFT_SUMMARY_SIZE;
  public static final String RIGHT_SUMMARY_SIZE = SpatialJoinConstants.LAHilbert.RIGHT_SUMMARY_SIZE;
  public static final String LEFT_SUMMARY_FEATURES = SpatialJoinConstants.LAHilbert.LEFT_SUMMARY_FEATURES;
  public static final String RIGHT_SUMMARY_FEATURES = SpatialJoinConstants.LAHilbert.RIGHT_SUMMARY_FEATURES;
  public static final String INITIAL_ORDER = SpatialJoinConstants.LAHilbert.INITIAL_ORDER;
  public static final String PARTITION_BYTES = SpatialJoinConstants.LAHilbert.PARTITION_BYTES;
  public static final String EXECUTOR_MEMORY = SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY_KEY;
  public static final String EXECUTOR_CORES = SpatialJoinConstants.SparkDefaults.EXECUTOR_CORES_KEY;
  public static final String SPARK_MEMORY_FRACTION = SpatialJoinConstants.SparkDefaults.MEMORY_FRACTION_KEY;
  public static final String SPARK_STORAGE_FRACTION = SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION_KEY;
  public static final String SAFETY_FRACTION = SpatialJoinConstants.LAHilbert.SAFETY_FRACTION;
  public static final String LOAD_SCALE_FACTOR = SpatialJoinConstants.LAHilbert.LOAD_SCALE_FACTOR;
  public static final String REFINE_FANOUT = SpatialJoinConstants.LAHilbert.REFINE_FANOUT;
  public static final String MAX_REFINEMENT_DEPTH = SpatialJoinConstants.LAHilbert.MAX_REFINEMENT_DEPTH;
  public static final String REFINE_STRATEGY = SpatialJoinConstants.LAHilbert.REFINE_STRATEGY;
  public static final String REFINE_AGGREGATE_MBR_PRUNING_ENABLED =
      SpatialJoinConstants.LAHilbert.REFINE_AGGREGATE_MBR_PRUNING_ENABLED;
  public static final String REFINE_EFFECTIVE_PRUNING_ENABLED =
      SpatialJoinConstants.LAHilbert.REFINE_EFFECTIVE_PRUNING_ENABLED;
  public static final String COALESCE_SMALL_SIBLINGS_ENABLED = SpatialJoinConstants.LAHilbert.COALESCE_SMALL_SIBLINGS_ENABLED;
  public static final String PARTITION_ROUTING_MODE = SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE;

  private transient SpatialJoinOptions options;
  private double[] configuredGlobalMin;
  private double[] configuredGlobalMax;
  private double[] configuredLeftMin;
  private double[] configuredLeftMax;
  private double[] configuredRightMin;
  private double[] configuredRightMax;
  private long configuredLeftSizeBytes = -1L;
  private long configuredRightSizeBytes = -1L;
  private long configuredLeftFeatureCount = -1L;
  private long configuredRightFeatureCount = -1L;
  private long targetPartitionSizeBytes = SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES;
  private long targetPartitionFeatureCount = Long.MAX_VALUE;
  private int initialHilbertOrder = 0;
  private int effectivePartitionCount = 1;
  private long initialHilbertCellCount = 1L;
  private int refinementFanout = SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_FANOUT;
  private int maxRefinementDepth = SpatialJoinConstants.LAHilbert.DEFAULT_MAX_REFINEMENT_DEPTH;
  private String refinementStrategy = SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_STRATEGY;
  private boolean aggregateMbrRefinementPruningEnabled =
      SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_AGGREGATE_MBR_PRUNING_ENABLED;
  private boolean effectiveRefinementPruningEnabled =
      SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_EFFECTIVE_PRUNING_ENABLED;
  private boolean coalesceSmallSiblingsEnabled = SpatialJoinConstants.LAHilbert.DEFAULT_COALESCE_SMALL_SIBLINGS_ENABLED;
  private String partitionRoutingMode = SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_ROUTING_MODE;
  private int coalescedSiblingGroupCount = 0;
  private int coalescedSiblingPartitionReductionCount = 0;
  private int performedSplitCount = 0;
  private int thresholdSatisfiedLeafCount = 0;
  private int splitLimitLeafCount = 0;
  private int maxObservedRefinementDepth = 0;
  private transient AbstractFeature[] refinementPruningLeftFeatures;
  private transient AbstractFeature[] refinementPruningRightFeatures;
  private transient Map<String, Long> exactCorrectionParentBytesByCode = new HashMap<>();
  private transient Map<String, Long> exactCorrectionParentCountsByCode = new HashMap<>();
  private transient ExactRefinementDiagnostics lastExactRefinementDiagnostics =
      ExactRefinementDiagnostics.empty();
  private boolean disjointPartitions = false;
  private String[] partitionCodes = new String[]{"0"};
  private PartitionLeaf[] leafPartitions = new PartitionLeaf[]{new PartitionLeaf(new EnvelopeNDLite(0), "0", 0, -1, 0L, 0)};
  private transient volatile LeafRoutingDirectory leafRoutingDirectory;

  /**
   * Loads all LA-Hilbert specific options before partition construction starts.
   *
   * <p>The method only parses configuration and stores it in fields; it does not
   * build partitions yet. The parsed values are consumed later by:
   * - global overlap-domain derivation
   * - initial Hilbert order estimation
   * - recursive load-aware refinement
   *
   * @param conf the user/operator options that carry LA-Hilbert, Spark-memory, and
   *             execution-planning parameters
   * @param disjoint whether the surrounding execution path expects disjoint partitions;
   *                 the flag is stored for compatibility even though this partitioner
   *                 still relies on explicit effective-partition mappings later
   */
  @Override
  public void setup(SpatialJoinOptions conf, boolean disjoint) {
    this.disjointPartitions = disjoint;
    this.options = conf == null ? new SpatialJoinOptions() : new SpatialJoinOptions(conf);
    this.partitionRoutingMode = normalizePartitionRoutingMode(
        this.options.contains(PARTITION_ROUTING_MODE)
            ? this.options.getString(PARTITION_ROUTING_MODE)
            : SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_ROUTING_MODE);
    this.leafRoutingDirectory = null;
    this.configuredGlobalMin = parseCoords(this.options.getString(GLOBAL_MIN_COORDS));
    this.configuredGlobalMax = parseCoords(this.options.getString(GLOBAL_MAX_COORDS));
    this.configuredLeftMin = parseCoords(this.options.getString(LEFT_MIN_COORDS));
    this.configuredLeftMax = parseCoords(this.options.getString(LEFT_MAX_COORDS));
    this.configuredRightMin = parseCoords(this.options.getString(RIGHT_MIN_COORDS));
    this.configuredRightMax = parseCoords(this.options.getString(RIGHT_MAX_COORDS));
    this.configuredLeftSizeBytes = parseSize(this.options, LEFT_SUMMARY_SIZE);
    this.configuredRightSizeBytes = parseSize(this.options, RIGHT_SUMMARY_SIZE);
    this.configuredLeftFeatureCount = parseLong(this.options, LEFT_SUMMARY_FEATURES);
    this.configuredRightFeatureCount = parseLong(this.options, RIGHT_SUMMARY_FEATURES);
    this.targetPartitionSizeBytes = resolveTargetPartitionBytes(this.options);
    this.refinementFanout = Math.max(2, this.options.getInt(REFINE_FANOUT, SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_FANOUT));
    int configuredMaxDepth =
        this.options.getInt(MAX_REFINEMENT_DEPTH, SpatialJoinConstants.LAHilbert.DEFAULT_MAX_REFINEMENT_DEPTH);
    this.maxRefinementDepth = configuredMaxDepth < 0 ? -1 : configuredMaxDepth;
    this.targetPartitionFeatureCount = resolveTargetPartitionFeatureCount();
    this.refinementStrategy = normalizeRefinementStrategy(this.options.getString(
        REFINE_STRATEGY,
        SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_STRATEGY));
    this.aggregateMbrRefinementPruningEnabled = this.options.getBoolean(
        REFINE_AGGREGATE_MBR_PRUNING_ENABLED,
        SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_AGGREGATE_MBR_PRUNING_ENABLED);
    this.effectiveRefinementPruningEnabled = this.options.getBoolean(
        REFINE_EFFECTIVE_PRUNING_ENABLED,
        SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_EFFECTIVE_PRUNING_ENABLED);
    this.coalesceSmallSiblingsEnabled = this.options.getBoolean(
        COALESCE_SMALL_SIBLINGS_ENABLED,
        SpatialJoinConstants.LAHilbert.DEFAULT_COALESCE_SMALL_SIBLINGS_ENABLED);
    this.coalescedSiblingGroupCount = 0;
    this.coalescedSiblingPartitionReductionCount = 0;
    this.performedSplitCount = 0;
    this.thresholdSatisfiedLeafCount = 0;
    this.splitLimitLeafCount = 0;
    this.maxObservedRefinementDepth = 0;
    this.lastExactRefinementDiagnostics = ExactRefinementDiagnostics.empty();
    this.exactCorrectionParentBytesByCode = new HashMap<>();
    this.exactCorrectionParentCountsByCode = new HashMap<>();
  }

  /**
   * Compatibility entry for callers that still construct a partitioner from raw representative points.
   *
   * <p>The points are first converted into lightweight {@link AbstractFeature} instances and
   * then forwarded to the full abstract-feature construction path.
   *
   * @param summary dataset summary used to derive the global domain and initial granularity
   * @param points representative point coordinates of the input objects
   * @param histogram reserved for future CAM/histogram refinement logic
   * @param ignoredNumPartitions shared interface hint that is intentionally ignored here
   */
  @Override
  public void construct(Summary summary, @Required double[][] points, AbstractHistogram histogram, int ignoredNumPartitions) {
    construct(summary, pointsToAbstractFeatures(summary, points), histogram, ignoredNumPartitions);
  }

  /**
   * Builds the geometric LA-Hilbert refinement tree from one flat set of lightweight features.
   *
   * @param summary dataset summary used by global-domain and initial-order derivation
   * @param features finest-level lightweight features participating in partition construction
   * @param histogram reserved for future CAM/histogram refinement logic
   * @param ignoredNumPartitions shared interface hint that is intentionally ignored here
   */
  @Override
  public void construct(Summary summary, AbstractFeature[] features, AbstractHistogram histogram, int ignoredNumPartitions) {
    construct(summary, new AbstractFeatureHierarchy(new AbstractFeature[][]{features}), histogram, ignoredNumPartitions);
  }

  /**
   * Builds the normal geometric refinement layout while preserving input-side identity only for
   * optional aggregate-MBR pruning. Split boundaries and load decisions still use the combined
   * synopsis exactly as in {@link #construct(Summary, AbstractFeature[], AbstractHistogram, int)}.
   */
  public void constructWithSideAwareRefinement(Summary summary,
                                               AbstractFeature[] leftFeatures,
                                               AbstractFeature[] rightFeatures,
                                               AbstractHistogram histogram,
                                               int ignoredNumPartitions) {
    AbstractFeature[] safeLeftFeatures = leftFeatures == null ? new AbstractFeature[0] : leftFeatures;
    AbstractFeature[] safeRightFeatures = rightFeatures == null ? new AbstractFeature[0] : rightFeatures;
    AbstractFeature[] combinedFeatures = concatFeatures(safeLeftFeatures, safeRightFeatures);
    this.refinementPruningLeftFeatures = safeLeftFeatures;
    this.refinementPruningRightFeatures = safeRightFeatures;
    try {
      construct(
          summary,
          combinedFeatures,
          histogram,
          ignoredNumPartitions,
          new AbstractFeatureHierarchy(new AbstractFeature[][]{combinedFeatures}));
    } finally {
      this.refinementPruningLeftFeatures = null;
      this.refinementPruningRightFeatures = null;
    }
  }

  /**
   * Builds the geometric LA-Hilbert refinement tree from a multi-level feature hierarchy.
   *
   * <p>The hierarchy allows the refinement logic to start from coarse aggregates and descend to
   * finer levels only when a partition remains heavy after earlier refinement steps.
   *
   * @param summary dataset summary used by global-domain and initial-order derivation
   * @param hierarchy lightweight hierarchy that can rebind heavy partitions to finer levels
   * @param histogram reserved for future CAM/histogram refinement logic
   * @param ignoredNumPartitions shared interface hint that is intentionally ignored here
   */
  @Override
  public void construct(Summary summary, AbstractFeatureHierarchy hierarchy, AbstractHistogram histogram, int ignoredNumPartitions) {
    AbstractFeature[] finestFeatures = hierarchy == null ? null : hierarchy.getFinestLevel();
    if (finestFeatures == null || finestFeatures.length == 0) {
      finestFeatures = new AbstractFeature[0];
    }
    if (finestFeatures.length == 0) {
      construct(summary, (AbstractFeature[]) null, histogram, ignoredNumPartitions, hierarchy);
      return;
    }
    construct(summary, finestFeatures, histogram, ignoredNumPartitions, hierarchy);
  }

  /**
   * Builds the effective LA-Hilbert partition set directly from left/right lightweight inputs.
   *
   * <p>This join-oriented path follows document section 1.1.3 more closely than the generic
   * single-input construction path:
   * - left and right features are mapped together onto the same initial Hilbert cells
   * - one-sided cells are pruned immediately
   * - recursive refinement keeps only children that still receive both sides
   *
   * @param summary dataset summary used to derive the overlap-driven global domain
   * @param leftFeatures lightweight descriptors of the left input
   * @param rightFeatures lightweight descriptors of the right input
   * @param histogram reserved for future CAM/histogram refinement logic
   * @param ignoredNumPartitions shared interface hint that is intentionally ignored here
   * @return an {@link EffectivePlan} containing the final effective leaves, per-partition
   *         profiles, and left/right feature-to-partition assignments
   */
  @Override
  public EffectivePlan constructEffective(Summary summary,
                                          AbstractFeature[] leftFeatures,
                                          AbstractFeature[] rightFeatures,
                                          AbstractHistogram histogram,
                                          int ignoredNumPartitions) {
    AbstractFeature[] safeLeftFeatures = leftFeatures == null ? new AbstractFeature[0] : leftFeatures;
    AbstractFeature[] safeRightFeatures = rightFeatures == null ? new AbstractFeature[0] : rightFeatures;
    List<AssignedPartition> effectiveLeaves =
        constructEffective(summary, safeLeftFeatures, safeRightFeatures, histogram);
    return buildEffectivePlanFromAssignedPartitions(effectiveLeaves, safeLeftFeatures, safeRightFeatures);
  }

  /**
   * Builds the full geometric LA-Hilbert refinement tree from lightweight input features.
   *
   * <p>This method corresponds to the "construct partitions first, derive effective partitions
   * later" path:
   * 1. choose the global working domain
   * 2. derive the initial Hilbert order
   * 3. build first-level Hilbert cells
   * 4. recursively split heavy cells while optionally descending hierarchy levels
   * 5. materialize the final leaf partitions
   *
   * <p>The {@code ignoredNumPartitions} hint is intentionally not used directly because this
   * partitioner follows the document's memory-driven initial order {@code l} derivation instead
   * of enforcing a strict partition-count target.
   *
   * @param summary dataset summary used to derive domain and initial order
   * @param features finest-level lightweight features that define partition load
   * @param histogram reserved for future CAM/histogram refinement logic
   * @param ignoredNumPartitions shared interface hint kept only for contract compatibility
   * @param hierarchy optional multi-level hierarchy used to rebind heavy partitions to finer levels
   */
  private void construct(Summary summary,
                         AbstractFeature[] features,
                         AbstractHistogram histogram,
                         int ignoredNumPartitions,
                         AbstractFeatureHierarchy hierarchy) {
    if (features == null || features.length == 0) {
      inputMBR.set(summary);
      splitPoints = null;
      hCurveSorter = null;
      effectivePartitionCount = 1;
      initialHilbertCellCount = 1L;
      initialHilbertOrder = 0;
      partitionCodes = new String[]{"0"};
      leafPartitions = new PartitionLeaf[]{new PartitionLeaf(new EnvelopeNDLite(inputMBR), "0", 0, -1, 0L, 0)};
      return;
    }

    final int numDimensions = resolveCoordinateDimension(summary, features);
    configureGlobalDomain(summary, numDimensions, features);
    this.initialHilbertOrder = resolveInitialHilbertOrder(summary, numDimensions);
    this.initialHilbertCellCount = safePow2((long) initialHilbertOrder * numDimensions);
    int initialLevelIndex = 0;
    AbstractFeature[] initialLevelFeatures =
        hierarchy == null || hierarchy.isEmpty() ? features : hierarchy.getCoarsestLevel();

    // At this stage the partitioner only builds the geometric refinement structure inside
    // the overlap-driven global domain. The later effective partition set P_e, which keeps
    // only partitions that finally receive both left and right sides, is still finalized by
    // the execution-side mapping/statistics stages described after document section 1.1.
    List<WorkingPartition> initialPartitions = buildInitialHilbertPartitions(initialLevelFeatures, initialLevelIndex);
    if (hasRefinementPruningSides())
      initialPartitions = attachRefinementSideAssignments(initialPartitions);
    List<PartitionLeaf> leaves = new ArrayList<>();
    for (WorkingPartition partition : initialPartitions)
      refinePartition(partition, hierarchy == null ? new AbstractFeatureHierarchy(new AbstractFeature[][]{features}) : hierarchy, leaves);

    if (leaves.isEmpty() && !hasRefinementPruningSides())
      leaves.add(new PartitionLeaf(new EnvelopeNDLite(inputMBR), "0", 0, -1, summary == null ? 0L : summary.size(), features.length));

    this.leafPartitions = leaves.toArray(new PartitionLeaf[0]);
    this.effectivePartitionCount = leafPartitions.length;
    this.partitionCodes = new String[effectivePartitionCount];
    for (int i = 0; i < effectivePartitionCount; i++)
      this.partitionCodes[i] = leafPartitions[i].code;

    // Keep the inherited fields aligned for compatibility/debugging even though
    // LAHilbertPartitioner routes using explicit leaf partitions.
    splitPoints = null;
    hCurveSorter = null;

    if (options != null && histogram == null)
      log.debug("LAHilbertPartitioner is currently driven by full abstract features; histogram/CAM refinement is reserved for a later pass.");
  }

  /**
   * Builds LA-Hilbert leaves while pruning one-sided partitions before recursive refinement.
   *
   * <p>This is the join-oriented construction path used by the current operator layer:
   * 1. derive the overlap-driven global domain
   * 2. create first-level Hilbert cells
   * 3. map left/right lightweight features to all intersecting cells
   * 4. keep only effective cells that receive both sides
   * 5. refine heavy effective cells recursively
   *
   * @param summary dataset summary used to derive domain and initial order
   * @param leftFeatures lightweight left-input descriptors
   * @param rightFeatures lightweight right-input descriptors
   * @param combinedFeatures concatenated left/right descriptors used only for dimension/domain fallback
   * @param histogram reserved for future CAM/histogram refinement logic
   * @return the final effective leaf partitions together with the left/right indexes assigned to each leaf
   */
  private List<AssignedPartition> constructEffective(Summary summary,
                                                     AbstractFeature[] leftFeatures,
                                                     AbstractFeature[] rightFeatures,
                                                     AbstractHistogram histogram) {
    if ((leftFeatures == null || leftFeatures.length == 0) && (rightFeatures == null || rightFeatures.length == 0)) {
      inputMBR.set(summary);
      splitPoints = null;
      hCurveSorter = null;
      effectivePartitionCount = 0;
      initialHilbertCellCount = 1L;
      initialHilbertOrder = 0;
      partitionCodes = new String[0];
      leafPartitions = new PartitionLeaf[0];
      return new ArrayList<>();
    }

    final int numDimensions = resolveCoordinateDimension(summary, leftFeatures, rightFeatures);
    configureGlobalDomain(summary, numDimensions, leftFeatures, rightFeatures);
    this.initialHilbertOrder = resolveInitialHilbertOrder(summary, numDimensions);
    this.initialHilbertCellCount = safePow2((long) initialHilbertOrder * numDimensions);

    List<AssignedPartition> initialPartitions = buildInitialEffectivePartitions(leftFeatures, rightFeatures);
    List<AssignedPartition> leaves = new ArrayList<>();
    for (AssignedPartition partition : initialPartitions)
      refineEffectivePartition(partition, leftFeatures, rightFeatures, leaves);

    this.leafPartitions = new PartitionLeaf[leaves.size()];
    for (int i = 0; i < leaves.size(); i++)
      this.leafPartitions[i] = leaves.get(i).toLeaf();
    this.effectivePartitionCount = leafPartitions.length;
    this.partitionCodes = new String[effectivePartitionCount];
    for (int i = 0; i < effectivePartitionCount; i++)
      this.partitionCodes[i] = leafPartitions[i].code;

    splitPoints = null;
    hCurveSorter = null;

    if (options != null && histogram == null)
      log.debug("LAHilbertPartitioner pruned one-sided partitions before refinement; histogram/CAM refinement is reserved for a later pass.");
    return leaves;
  }

  /** Resolves the working coordinate dimension from the summary first, then the features. */
  private int resolveCoordinateDimension(Summary summary, AbstractFeature[] features) {
    if (summary != null && summary.getCoordinateDimension() > 0)
      return summary.getCoordinateDimension();
    for (AbstractFeature feature : features) {
      if (feature != null && feature.getCoordinateDimension() > 0)
        return feature.getCoordinateDimension();
    }
    return 0;
  }

  /** Resolves the working coordinate dimension from summary first, then the left/right features. */
  private int resolveCoordinateDimension(Summary summary,
                                         AbstractFeature[] leftFeatures,
                                         AbstractFeature[] rightFeatures) {
    if (summary != null && summary.getCoordinateDimension() > 0)
      return summary.getCoordinateDimension();
    int leftDimension = resolveCoordinateDimension(summary, leftFeatures);
    if (leftDimension > 0)
      return leftDimension;
    return resolveCoordinateDimension(summary, rightFeatures);
  }

  /**
   * Chooses the overlap-driven global domain used by the initial Hilbert grid.
   *
   * <p>Preference order is:
   * 1. explicit left/right intersection from options
   * 2. explicit global min/max from options
   * 3. summary envelope
   * 4. merged feature envelopes
   *
   * <p>The chosen envelope becomes the common reference domain for:
   * - initial Hilbert cell creation
   * - feature-to-cell assignment
   * - later partition-id queries
   *
   * @param summary dataset summary that may already contain the overall input envelope
   * @param numDimensions resolved coordinate dimension of the current construction task
   * @param features lightweight features used as a last-resort envelope fallback
   */
  private void configureGlobalDomain(Summary summary, int numDimensions, AbstractFeature[] features) {
    inputMBR.setCoordinateDimension(numDimensions);
    EnvelopeNDLite globalIntersection = resolveConfiguredGlobalIntersection(numDimensions);
    if (globalIntersection != null && !globalIntersection.isEmpty()) {
      // Preferred document path: build the initial grid only on A.mbr ∩ B.mbr.
      inputMBR.set(globalIntersection);
    } else if (configuredGlobalMin != null && configuredGlobalMax != null &&
        configuredGlobalMin.length == numDimensions && configuredGlobalMax.length == numDimensions) {
      for (int d = 0; d < numDimensions; d++) {
        inputMBR.setMinCoord(d, configuredGlobalMin[d]);
        inputMBR.setMaxCoord(d, configuredGlobalMax[d]);
      }
    } else if (summary != null && summary.getCoordinateDimension() == numDimensions && !summary.isEmpty()) {
      inputMBR.set(summary);
    } else {
      inputMBR.setEmpty();
      for (AbstractFeature feature : features) {
        if (feature != null && !feature.isEmpty()) {
          if (inputMBR.isEmpty())
            inputMBR.set(feature);
          else
            inputMBR.merge(feature);
        }
      }
    }
  }

  /** Join-oriented global-domain configuration that merges the left/right lightweight features. */
  private void configureGlobalDomain(Summary summary,
                                     int numDimensions,
                                     AbstractFeature[] leftFeatures,
                                     AbstractFeature[] rightFeatures) {
    inputMBR.setCoordinateDimension(numDimensions);
    EnvelopeNDLite globalIntersection = resolveConfiguredGlobalIntersection(numDimensions);
    if (globalIntersection != null && !globalIntersection.isEmpty()) {
      inputMBR.set(globalIntersection);
    } else if (configuredGlobalMin != null && configuredGlobalMax != null &&
        configuredGlobalMin.length == numDimensions && configuredGlobalMax.length == numDimensions) {
      for (int d = 0; d < numDimensions; d++) {
        inputMBR.setMinCoord(d, configuredGlobalMin[d]);
        inputMBR.setMaxCoord(d, configuredGlobalMax[d]);
      }
    } else if (summary != null && summary.getCoordinateDimension() == numDimensions && !summary.isEmpty()) {
      inputMBR.set(summary);
    } else {
      inputMBR.setEmpty();
      mergeFeatureEnvelopeIntoInput(leftFeatures);
      mergeFeatureEnvelopeIntoInput(rightFeatures);
    }
  }

  private void mergeFeatureEnvelopeIntoInput(AbstractFeature[] features) {
    if (features == null)
      return;
    for (AbstractFeature feature : features) {
      if (feature != null && !feature.isEmpty()) {
        if (inputMBR.isEmpty())
          inputMBR.set(feature);
        else
          inputMBR.merge(feature);
      }
    }
  }

  /**
   * Derives the target in-memory load budget of one partition.
   *
   * <p>The budget is resolved in this order:
   * 1. explicit {@code lahilbert.partition.bytes}
   * 2. derived bytes from executor memory, execution memory fraction, storage fraction, and safety factor
   *
   * @param conf options that may provide an explicit threshold or the Spark-memory parameters needed to derive one
   * @return the per-partition load threshold in bytes used by {@link #shouldRefine(PartitionLeaf)}
   */
  private long resolveTargetPartitionBytes(SpatialJoinOptions conf) {
    if (conf == null)
      return SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES;
    if (conf.contains(PARTITION_BYTES))
      return conf.getSizeAsBytes(PARTITION_BYTES, SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES);

    long executorMemoryBytes = conf.getSizeAsBytes(EXECUTOR_MEMORY, SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY);
    int executorCores = Math.max(1, conf.getInt(EXECUTOR_CORES, SpatialJoinConstants.SparkDefaults.EXECUTOR_CORES));
    double memoryFraction = conf.getDouble(SPARK_MEMORY_FRACTION, SpatialJoinConstants.SparkDefaults.MEMORY_FRACTION);
    double storageFraction = conf.getDouble(SPARK_STORAGE_FRACTION, SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION);
    double safetyFraction = conf.getDouble(SAFETY_FRACTION, SpatialJoinConstants.LAHilbert.DEFAULT_SAFETY_FRACTION);

    long derivedBytes = (long) (executorMemoryBytes * memoryFraction * (1.0d - storageFraction) * safetyFraction / executorCores);
    return Math.max(1L, derivedBytes);
  }

  /**
   * Converts the configured global partition byte threshold into an approximate feature-count threshold.
   *
   * <p>The estimate uses the average bytes per feature derived from left/right dataset summaries:
   * {@code avgBytesPerFeature = (leftSize + rightSize) / (leftCount + rightCount)} and then
   * {@code targetCount = partitionBytes / avgBytesPerFeature}. The threshold is bounded from below
   * by the refinement fanout so count-based refinement never asks to split a partition that cannot
   * produce at least two meaningful children.
   *
   * @return derived feature-count threshold, or {@link Long#MAX_VALUE} when dataset summaries are
   *         incomplete and count-based refinement should effectively stay disabled
   */
  private long resolveTargetPartitionFeatureCount() {
    long totalSizeBytes = 0L;
    long totalFeatureCount = 0L;
    if (configuredLeftSizeBytes > 0L)
      totalSizeBytes += configuredLeftSizeBytes;
    if (configuredRightSizeBytes > 0L)
      totalSizeBytes += configuredRightSizeBytes;
    if (configuredLeftFeatureCount > 0L)
      totalFeatureCount += configuredLeftFeatureCount;
    if (configuredRightFeatureCount > 0L)
      totalFeatureCount += configuredRightFeatureCount;
    if (totalSizeBytes <= 0L || totalFeatureCount <= 0L || targetPartitionSizeBytes <= 0L)
      return Long.MAX_VALUE;
    double averageBytesPerFeature = Math.max(1.0d, totalSizeBytes / (double) totalFeatureCount);
    long derivedThreshold = (long) Math.floor(targetPartitionSizeBytes / averageBytesPerFeature);
    return Math.max(refinementFanout, Math.max(1L, derivedThreshold));
  }

  /**
   * Computes the initial Hilbert order `l` used to create the first `2^l x 2^l`
   * grid inside the global overlap domain.
   *
   * @param summary dataset summary that supplies total bytes when overlap-specific byte totals are absent
   * @param numDimensions resolved coordinate dimension of the input
   * @return the initial Hilbert order capped by the implementation maximum
   */
  private int resolveInitialHilbertOrder(Summary summary, int numDimensions) {
    if (options != null && options.contains(INITIAL_ORDER))
      return Math.max(0, options.getInt(INITIAL_ORDER, 0));

    if (targetPartitionSizeBytes <= 0)
      return 0;

    long estimatedRelevantBytes = estimateRelevantBytes(summary);
    if (estimatedRelevantBytes <= 0)
      return 0;

    double loadScaleFactor = options == null ? SpatialJoinConstants.LAHilbert.DEFAULT_LOAD_SCALE_FACTOR :
        options.getDouble(LOAD_SCALE_FACTOR, SpatialJoinConstants.LAHilbert.DEFAULT_LOAD_SCALE_FACTOR);
    // In 2D this matches the document's "2^l x 2^l" initial grid. The implementation
    // keeps the math dimension-generic so the same code path still works for future ND cases.
    double desiredCells = Math.max(1.0d, Math.ceil((estimatedRelevantBytes * loadScaleFactor) / (double) targetPartitionSizeBytes));
    int order = (int) Math.ceil(Math.log(desiredCells) / (Math.log(2.0d) * Math.max(1, numDimensions)));
    return Math.max(0, Math.min(order, SpatialJoinConstants.LAHilbert.MAX_ORDER));
  }

  /**
   * Estimates the bytes that are relevant to the overlap-driven global domain.
   *
   * <p>The left/right dataset totals supplied through {@code lahilbert.left.size}
   * and {@code lahilbert.right.size} are expected to come from real dataset
   * summaries gathered during reading. Only the overlap contribution is derived
   * proportionally from those exact totals.
   *
   * @param summary dataset summary used as a fallback total-byte estimate
   * @return estimated bytes that are likely to participate in overlap-aware partitioning
   */
  private long estimateRelevantBytes(Summary summary) {
    EnvelopeNDLite globalIntersection = resolveConfiguredGlobalIntersection(
        summary != null && summary.getCoordinateDimension() > 0 ? summary.getCoordinateDimension() : inputMBR.getCoordinateDimension());
    if (globalIntersection != null &&
        !globalIntersection.isEmpty() &&
        configuredLeftSizeBytes >= 0 &&
        configuredRightSizeBytes >= 0 &&
        configuredLeftMin != null && configuredLeftMax != null &&
        configuredRightMin != null && configuredRightMax != null) {
      EnvelopeNDLite leftEnvelope = new EnvelopeNDLite(configuredLeftMin, configuredLeftMax);
      EnvelopeNDLite rightEnvelope = new EnvelopeNDLite(configuredRightMin, configuredRightMax);
      double leftRelevantRatio = safeCoverageRatio(globalIntersection, leftEnvelope);
      double rightRelevantRatio = safeCoverageRatio(globalIntersection, rightEnvelope);
      long estimatedLeftBytes = Math.round(configuredLeftSizeBytes * leftRelevantRatio);
      long estimatedRightBytes = Math.round(configuredRightSizeBytes * rightRelevantRatio);
      return Math.max(1L, estimatedLeftBytes + estimatedRightBytes);
    }
    return summary == null ? 0L : Math.max(0L, summary.size());
  }

  /** Normalizes external refinement labels so unsupported values safely fall back to the baseline. */
  private String normalizeRefinementStrategy(String configuredStrategy) {
    if (SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_OBJECT_CENTER_OFFSET_AXIS_LOAD.equals(configuredStrategy))
      return SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_OBJECT_CENTER_OFFSET_AXIS_LOAD;
    if (SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_EQUAL_QUADTREE_COUNT.equals(configuredStrategy))
      return SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_EQUAL_QUADTREE_COUNT;
    if (SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_EQUAL_QUADTREE_LOAD.equals(configuredStrategy))
      return SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_EQUAL_QUADTREE_LOAD;
    return SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_ADAPTIVE_AXIS_LOAD;
  }

  /** Normalizes routing labels while keeping the indexed path as the production default. */
  private String normalizePartitionRoutingMode(String configuredMode) {
    if (SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE_LINEAR.equalsIgnoreCase(configuredMode))
      return SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE_LINEAR;
    if (SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE_VALIDATE.equalsIgnoreCase(configuredMode))
      return SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE_VALIDATE;
    return SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE_GRID_DIRECTORY;
  }

  /** Returns the overlap-area ratio while guarding against empty or degenerate envelopes. */
  private double safeCoverageRatio(EnvelopeNDLite overlapEnvelope, EnvelopeNDLite datasetEnvelope) {
    if (overlapEnvelope == null || datasetEnvelope == null || overlapEnvelope.isEmpty() || datasetEnvelope.isEmpty())
      return 0.0d;
    double datasetArea = datasetEnvelope.getArea();
    if (!(datasetArea > 0.0d))
      return 0.0d;
    double overlapArea = overlapEnvelope.getArea();
    if (!(overlapArea > 0.0d))
      return 0.0d;
    return Math.max(0.0d, Math.min(1.0d, overlapArea / datasetArea));
  }

  /**
   * Creates the first-level partitions by mapping every abstract feature to a cell
   * in the initial Hilbert grid and then ordering non-empty cells by Hilbert rank.
   *
   * @param features lightweight features that populate the first-level cells
   * @param levelIndex hierarchy level represented by the supplied features
   * @return non-empty initial Hilbert cells ordered by Hilbert rank and carrying their member indexes
   */
  private List<WorkingPartition> buildInitialHilbertPartitions(AbstractFeature[] features, int levelIndex) {
    if (inputMBR.isEmpty())
      return new ArrayList<>();
    final int numDimensions = inputMBR.getCoordinateDimension();
    final int sidePartitions = initialHilbertOrder <= 0 ? 1 : (1 << initialHilbertOrder);
    Map<CellKey, IntArray> groupedFeatures = new LinkedHashMap<>();

    for (int i = 0; i < features.length; i++) {
      AbstractFeature feature = features[i];
      if (feature == null || feature.isEmpty() || !feature.intersectsEnvelopeClosed(inputMBR))
        continue;
      int[] coords = new int[numDimensions];
      for (int d = 0; d < numDimensions; d++)
        coords[d] = locateCell(featureRepresentativeCoord(feature, d), d, sidePartitions);
      CellKey key = new CellKey(coords);
      groupedFeatures.computeIfAbsent(key, k -> new IntArray()).add(i);
    }

    List<WorkingPartition> partitions = new ArrayList<>(groupedFeatures.size());
    double[][] centers = new double[numDimensions][groupedFeatures.size()];
    int idx = 0;
    for (Map.Entry<CellKey, IntArray> entry : groupedFeatures.entrySet()) {
      EnvelopeNDLite envelope = createCellEnvelope(entry.getKey().coords, sidePartitions);
      for (int d = 0; d < numDimensions; d++)
        centers[d][idx] = envelope.getCenter(d);
      int[] featureIndexes = entry.getValue().toArray();
      int memberCount = sumMemberCount(features, featureIndexes);
      partitions.add(new WorkingPartition(
          envelope,
          null,
          0,
          -1,
          sumStorageBytes(features, featureIndexes),
          memberCount,
          levelIndex,
          featureIndexes));
      idx++;
    }

    Integer[] order = new Integer[partitions.size()];
    for (int i = 0; i < order.length; i++)
      order[i] = i;
    HCurveSortable sorter = new HCurveSortable(inputMBR);
    sorter.setPoints(centers);
    Arrays.sort(order, sorter::compare);

    List<WorkingPartition> orderedPartitions = new ArrayList<>(partitions.size());
    int width = codeWidth(partitions.size());
    for (int rank = 0; rank < order.length; rank++) {
      WorkingPartition partition = partitions.get(order[rank]);
      partition.code = formatOrdinal(rank, width);
      orderedPartitions.add(partition);
    }
    return orderedPartitions;
  }

  /**
   * Builds the initial Hilbert cells together with left/right assignment statistics.
   *
   * <p>Only cells that receive features from both sides are retained. This makes the
   * resulting first-level partitions already match the document's effective-partition
   * definition before recursive refinement starts.
   *
   * @param leftFeatures lightweight descriptors of the left input
   * @param rightFeatures lightweight descriptors of the right input
   * @return initial effective cells ordered by Hilbert rank
   */
  private List<AssignedPartition> buildInitialEffectivePartitions(AbstractFeature[] leftFeatures,
                                                                  AbstractFeature[] rightFeatures) {
    if (inputMBR.isEmpty())
      return new ArrayList<>();
    final int numDimensions = inputMBR.getCoordinateDimension();
    final int sidePartitions = initialHilbertOrder <= 0 ? 1 : (1 << initialHilbertOrder);
    Map<CellKey, CellAssignments> groupedAssignments = new LinkedHashMap<>();

    for (int i = 0; i < leftFeatures.length; i++) {
      AbstractFeature feature = leftFeatures[i];
      if (feature == null || feature.isEmpty() || !feature.intersectsEnvelopeClosed(inputMBR))
        continue;
      collectIntersectingInitialCells(feature, i, sidePartitions, groupedAssignments, true);
    }
    for (int i = 0; i < rightFeatures.length; i++) {
      AbstractFeature feature = rightFeatures[i];
      if (feature == null || feature.isEmpty() || !feature.intersectsEnvelopeClosed(inputMBR))
        continue;
      collectIntersectingInitialCells(feature, i, sidePartitions, groupedAssignments, false);
    }

    List<AssignedPartition> partitions = new ArrayList<>(groupedAssignments.size());
    double[][] centers = new double[numDimensions][groupedAssignments.size()];
    int idx = 0;
    for (Map.Entry<CellKey, CellAssignments> entry : groupedAssignments.entrySet()) {
      CellAssignments assignments = entry.getValue();
      if (!assignments.isEffective())
        continue;
      EnvelopeNDLite envelope = createCellEnvelope(entry.getKey().coords, sidePartitions);
      for (int d = 0; d < numDimensions; d++)
        centers[d][idx] = envelope.getCenter(d);
      partitions.add(new AssignedPartition(
          envelope,
          null,
          0,
          -1,
          assignments.totalBytes(leftFeatures, rightFeatures),
          assignments.totalMembers(leftFeatures, rightFeatures),
          assignments.leftIndexes.toArray(),
          assignments.rightIndexes.toArray()));
      idx++;
    }

    if (partitions.isEmpty())
      return partitions;

    double[][] compactCenters = centers;
    if (idx != centers[0].length) {
      compactCenters = new double[numDimensions][idx];
      for (int d = 0; d < numDimensions; d++)
        System.arraycopy(centers[d], 0, compactCenters[d], 0, idx);
    }
    Integer[] order = new Integer[partitions.size()];
    for (int i = 0; i < order.length; i++)
      order[i] = i;
    HCurveSortable sorter = new HCurveSortable(inputMBR);
    sorter.setPoints(compactCenters);
    Arrays.sort(order, sorter::compare);

    List<AssignedPartition> orderedPartitions = new ArrayList<>(partitions.size());
    int width = codeWidth(partitions.size());
    for (int rank = 0; rank < order.length; rank++) {
      AssignedPartition partition = partitions.get(order[rank]);
      partition.code = formatOrdinal(rank, width);
      orderedPartitions.add(partition);
    }
    return orderedPartitions;
  }

  /**
   * Recursively refines one working partition until its load is acceptable or it
   * cannot be split any further.
   *
   * @param partition the current partition candidate being evaluated
   * @param hierarchy hierarchy that can supply finer-level features when a coarse partition stays heavy
   * @param leaves output collector that receives final unsplit leaves
   */
  private void refinePartition(WorkingPartition partition,
                               AbstractFeatureHierarchy hierarchy,
                               List<PartitionLeaf> leaves) {
    if (partition == null)
      return;
    if (aggregateMbrRefinementPruningEnabled
        && partition.hasSideAssignments()
        && !projectedAggregateEnvelopesIntersect(
            partition.envelope,
            refinementPruningLeftFeatures,
            partition.leftFeatureIndexes,
            refinementPruningRightFeatures,
            partition.rightFeatureIndexes))
      return;
    maxObservedRefinementDepth = Math.max(maxObservedRefinementDepth, partition.depth);
    if (!shouldRefine(partition)) {
      recordRefinementStop(partition);
      leaves.add(partition.toLeaf());
      return;
    }

    if (partition.levelIndex < hierarchy.numLevels() - 1) {
      WorkingPartition reboundPartition = rebindToNextLevel(partition, hierarchy);
      if (reboundPartition != null) {
        refinePartition(reboundPartition, hierarchy, leaves);
        return;
      }
    }

    AbstractFeature[] currentLevelFeatures = hierarchy.getLevel(partition.levelIndex);
    WorkingPartition[] children = useEqualQuadtreeRefinement(partition.envelope)
        ? splitPartitionEqualQuadtree(partition, currentLevelFeatures)
        : splitPartition(
            partition,
            chooseSplitAxis(partition, currentLevelFeatures),
            currentLevelFeatures);
    if (children == null || children.length <= 1) {
      leaves.add(partition.toLeaf());
      return;
    }

    performedSplitCount++;
    for (WorkingPartition child : children) {
      WorkingPartition sideAwareChild = partition.hasSideAssignments()
          ? attachRefinementSideAssignments(child, partition)
          : child;
      refinePartition(sideAwareChild, hierarchy, leaves);
    }
  }

  /**
   * Recursively refines one effective partition while pruning one-sided children eagerly.
   *
   * @param partition current effective partition candidate
   * @param leftFeatures left-input lightweight descriptors
   * @param rightFeatures right-input lightweight descriptors
   * @param leaves output collector that receives final effective leaves
   */
  private void refineEffectivePartition(AssignedPartition partition,
                                        AbstractFeature[] leftFeatures,
                                        AbstractFeature[] rightFeatures,
                                        List<AssignedPartition> leaves) {
    if (partition == null || !partition.isEffective())
      return;
    if (aggregateMbrRefinementPruningEnabled
        && !projectedAggregateEnvelopesIntersect(
            partition.envelope,
            leftFeatures,
            partition.leftFeatureIndexes,
            rightFeatures,
            partition.rightFeatureIndexes))
      return;
    maxObservedRefinementDepth = Math.max(maxObservedRefinementDepth, partition.depth);
    if (!shouldRefine(partition)) {
      recordRefinementStop(partition);
      leaves.add(partition);
      return;
    }
    AssignedPartition[] children = useEqualQuadtreeRefinement(partition.envelope)
        ? splitEffectivePartitionEqualQuadtree(partition, leftFeatures, rightFeatures)
        : splitEffectivePartition(
            partition,
            chooseSplitAxis(partition, leftFeatures, rightFeatures),
            leftFeatures,
            rightFeatures);
    if (children == null || children.length == 0) {
      leaves.add(partition);
      return;
    }
    performedSplitCount++;
    for (AssignedPartition child : children)
      refineEffectivePartition(child, leftFeatures, rightFeatures, leaves);
  }

  /**
   * Tests whether the two side-specific aggregate MBRs overlap inside one refinement partition.
   *
   * <p>The method updates primitive bounds directly and creates no per-feature envelope objects.
   * A disjoint result is a safe negative condition for the current {@code intersects} join.
   */
  private boolean projectedAggregateEnvelopesIntersect(EnvelopeNDLite partitionEnvelope,
                                                       AbstractFeature[] leftFeatures,
                                                       int[] leftIndexes,
                                                       AbstractFeature[] rightFeatures,
                                                       int[] rightIndexes) {
    if (partitionEnvelope == null || partitionEnvelope.getCoordinateDimension() < 2)
      return true;
    double leftMinX = Double.POSITIVE_INFINITY;
    double leftMinY = Double.POSITIVE_INFINITY;
    double leftMaxX = Double.NEGATIVE_INFINITY;
    double leftMaxY = Double.NEGATIVE_INFINITY;
    for (int index : leftIndexes) {
      if (index < 0 || index >= leftFeatures.length)
        continue;
      AbstractFeature feature = leftFeatures[index];
      if (feature == null || feature.isEmpty() || feature.getCoordinateDimension() < 2)
        continue;
      double minX = Math.max(feature.getMinCoord(0), partitionEnvelope.getMinCoord(0));
      double minY = Math.max(feature.getMinCoord(1), partitionEnvelope.getMinCoord(1));
      double maxX = Math.min(feature.getMaxCoord(0), partitionEnvelope.getMaxCoord(0));
      double maxY = Math.min(feature.getMaxCoord(1), partitionEnvelope.getMaxCoord(1));
      if (maxX < minX || maxY < minY)
        continue;
      leftMinX = Math.min(leftMinX, minX);
      leftMinY = Math.min(leftMinY, minY);
      leftMaxX = Math.max(leftMaxX, maxX);
      leftMaxY = Math.max(leftMaxY, maxY);
    }
    if (leftMaxX < leftMinX || leftMaxY < leftMinY)
      return false;

    double rightMinX = Double.POSITIVE_INFINITY;
    double rightMinY = Double.POSITIVE_INFINITY;
    double rightMaxX = Double.NEGATIVE_INFINITY;
    double rightMaxY = Double.NEGATIVE_INFINITY;
    for (int index : rightIndexes) {
      if (index < 0 || index >= rightFeatures.length)
        continue;
      AbstractFeature feature = rightFeatures[index];
      if (feature == null || feature.isEmpty() || feature.getCoordinateDimension() < 2)
        continue;
      double minX = Math.max(feature.getMinCoord(0), partitionEnvelope.getMinCoord(0));
      double minY = Math.max(feature.getMinCoord(1), partitionEnvelope.getMinCoord(1));
      double maxX = Math.min(feature.getMaxCoord(0), partitionEnvelope.getMaxCoord(0));
      double maxY = Math.min(feature.getMaxCoord(1), partitionEnvelope.getMaxCoord(1));
      if (maxX < minX || maxY < minY)
        continue;
      rightMinX = Math.min(rightMinX, minX);
      rightMinY = Math.min(rightMinY, minY);
      rightMaxX = Math.max(rightMaxX, maxX);
      rightMaxY = Math.max(rightMaxY, maxY);
    }
    if (rightMaxX < rightMinX || rightMaxY < rightMinY)
      return false;
    return rightMaxX >= leftMinX
        && leftMaxX >= rightMinX
        && rightMaxY >= leftMinY
        && leftMaxY >= rightMinY;
  }

  /**
   * Descends from one hierarchy level to the next finer level while preserving the
   * already assigned partition code and accumulated refinement path.
   *
   * This is the bridge between distributed hierarchical aggregation and the final
   * driver-side recursive partition refinement.
   * @param partition heavy partition currently bound to a coarser hierarchy level
   * @param hierarchy multi-level feature hierarchy
   * @return the same logical partition rebound to the next finer level, or {@code null}
   *         when no child features intersect the partition envelope
   */
  private WorkingPartition rebindToNextLevel(WorkingPartition partition, AbstractFeatureHierarchy hierarchy) {
    int nextLevelIndex = partition.levelIndex + 1;
    AbstractFeature[] nextLevelFeatures = hierarchy.getLevel(nextLevelIndex);
    int[] candidateIndexes = hierarchy.getChildIndexes(partition.levelIndex, partition.featureIndexes);
    candidateIndexes = filterIndexesByEnvelope(nextLevelFeatures, candidateIndexes, partition.envelope);
    if (candidateIndexes.length == 0) {
      IntArray candidates = new IntArray();
      for (int i = 0; i < nextLevelFeatures.length; i++) {
        AbstractFeature feature = nextLevelFeatures[i];
        if (feature != null && partition.envelope.intersectsEnvelopeClosed(feature))
          candidates.add(i);
      }
      candidateIndexes = candidates.toArray();
    }
    if (candidateIndexes.length == 0)
      return null;
    WorkingPartition rebound = new WorkingPartition(
        computeEnvelope(nextLevelFeatures, candidateIndexes, partition.envelope),
        partition.code,
        partition.depth,
        partition.splitAxis,
        sumStorageBytes(nextLevelFeatures, candidateIndexes),
        sumMemberCount(nextLevelFeatures, candidateIndexes),
        nextLevelIndex,
        candidateIndexes);
    return partition.hasSideAssignments()
        ? attachRefinementSideAssignments(rebound, partition)
        : rebound;
  }

  private boolean hasRefinementPruningSides() {
    return refinementPruningLeftFeatures != null && refinementPruningRightFeatures != null;
  }

  private List<WorkingPartition> attachRefinementSideAssignments(List<WorkingPartition> partitions) {
    List<WorkingPartition> sideAware = new ArrayList<>(partitions == null ? 0 : partitions.size());
    if (partitions == null)
      return sideAware;
    int[] allLeftIndexes = sequentialIndexes(refinementPruningLeftFeatures.length);
    int[] allRightIndexes = sequentialIndexes(refinementPruningRightFeatures.length);
    for (WorkingPartition partition : partitions) {
      if (partition == null)
        continue;
      sideAware.add(partition.withSideAssignments(
          filterIndexesByEnvelope(refinementPruningLeftFeatures, allLeftIndexes, partition.envelope),
          filterIndexesByEnvelope(refinementPruningRightFeatures, allRightIndexes, partition.envelope)));
    }
    return sideAware;
  }

  private WorkingPartition attachRefinementSideAssignments(WorkingPartition child,
                                                           WorkingPartition parent) {
    return child.withSideAssignments(
        filterIndexesByEnvelope(
            refinementPruningLeftFeatures,
            parent.leftFeatureIndexes,
            child.envelope),
        filterIndexesByEnvelope(
            refinementPruningRightFeatures,
            parent.rightFeatureIndexes,
            child.envelope));
  }

  private static int[] sequentialIndexes(int length) {
    int[] indexes = new int[Math.max(0, length)];
    for (int i = 0; i < indexes.length; i++)
      indexes[i] = i;
    return indexes;
  }

  /**
   * Applies the current load threshold rule to decide whether one partition should split again.
   *
   * @param partition current partition candidate
   * @return {@code true} when the partition is still heavy enough and can legally split again
   */
  private boolean shouldRefine(PartitionLeaf partition) {
    if (partition == null)
      return false;
    if ((maxRefinementDepth >= 0 && partition.depth >= maxRefinementDepth) || partition.featureCount < refinementFanout)
      return false;
    if (useCountDrivenRefinement())
      return partition.featureCount > targetPartitionFeatureCount;
    return partition.estimatedBytes > targetPartitionSizeBytes;
  }

  private void recordRefinementStop(PartitionLeaf partition) {
    if (partition == null)
      return;
    if (maxRefinementDepth >= 0 && partition.depth >= maxRefinementDepth)
      splitLimitLeafCount++;
    else if (useCountDrivenRefinement()) {
      if (partition.featureCount <= targetPartitionFeatureCount)
        thresholdSatisfiedLeafCount++;
    } else if (partition.estimatedBytes <= targetPartitionSizeBytes) {
      thresholdSatisfiedLeafCount++;
    }
  }

  /** Chooses the split axis as the longest side of the current partition envelope. */
  private int chooseSplitAxis(EnvelopeNDLite envelope) {
    int axis = 0;
    double maxSideLength = Double.NEGATIVE_INFINITY;
    for (int d = 0; d < envelope.getCoordinateDimension(); d++) {
      double sideLength = envelope.getSideLength(d);
      if (sideLength > maxSideLength) {
        maxSideLength = sideLength;
        axis = d;
      }
    }
    return axis;
  }

  /** Chooses the split axis for one working partition under the configured one-axis strategy. */
  private int chooseSplitAxis(WorkingPartition partition, AbstractFeature[] features) {
    if (usesObjectCenterOffsetAxisStrategy())
      return chooseSplitAxisByObjectCenterOffset(
          partition.envelope,
          features,
          partition.featureIndexes,
          null,
          null);
    return chooseSplitAxis(partition.envelope);
  }

  /** Chooses the split axis for one effective left/right partition. */
  private int chooseSplitAxis(AssignedPartition partition,
                              AbstractFeature[] leftFeatures,
                              AbstractFeature[] rightFeatures) {
    if (usesObjectCenterOffsetAxisStrategy())
      return chooseSplitAxisByObjectCenterOffset(
          partition.envelope,
          leftFeatures,
          partition.leftFeatureIndexes,
          rightFeatures,
          partition.rightFeatureIndexes);
    return chooseSplitAxis(partition.envelope);
  }

  /**
   * Chooses the axis with the largest normalized mean squared object-center offset from the
   * current partition's lower-left corner.
   */
  private int chooseSplitAxisByObjectCenterOffset(EnvelopeNDLite envelope,
                                                  AbstractFeature[] firstFeatures,
                                                  int[] firstIndexes,
                                                  AbstractFeature[] secondFeatures,
                                                  int[] secondIndexes) {
    int fallbackAxis = chooseSplitAxis(envelope);
    int bestAxis = fallbackAxis;
    double bestScore = -1.0d;
    for (int axis = 0; axis < envelope.getCoordinateDimension(); axis++) {
      double sideLength = envelope.getSideLength(axis);
      if (!(sideLength > 0.0d))
        continue;
      AxisDistributionStats stats = new AxisDistributionStats();
      accumulateAxisDistribution(
          stats,
          envelope,
          axis,
          firstFeatures,
          firstIndexes);
      accumulateAxisDistribution(
          stats,
          envelope,
          axis,
          secondFeatures,
          secondIndexes);
      double normalizedMeanSquaredOffset =
          stats.totalWeight > 0.0d
              ? stats.sumSquaredOffset(envelope.getMinCoord(axis)) /
                  (stats.totalWeight * sideLength * sideLength)
              : 0.0d;
      if (normalizedMeanSquaredOffset > bestScore ||
          (Double.compare(normalizedMeanSquaredOffset, bestScore) == 0 && axis == fallbackAxis)) {
        bestScore = normalizedMeanSquaredOffset;
        bestAxis = axis;
      }
    }
    return bestScore >= 0.0d ? bestAxis : fallbackAxis;
  }

  /** Updates weighted online variance for one feature collection on one axis. */
  private void accumulateAxisDistribution(AxisDistributionStats stats,
                                          EnvelopeNDLite envelope,
                                          int axis,
                                          AbstractFeature[] features,
                                          int[] featureIndexes) {
    if (stats == null || features == null || featureIndexes == null)
      return;
    double minCoord = envelope.getMinCoord(axis);
    double maxCoord = envelope.getMaxCoord(axis);
    for (int featureIndex : featureIndexes) {
      if (featureIndex < 0 || featureIndex >= features.length)
        continue;
      AbstractFeature feature = features[featureIndex];
      if (feature == null || feature.isEmpty())
        continue;
      long memberCount = Math.max(1L, feature.getMemberCount());
      if (feature.hasExactRepresentativeMoments(axis)) {
        stats.merge(
            memberCount,
            feature.getRepresentativeCoordinateMean(axis),
            feature.getRepresentativeCoordinateM2(axis));
      } else {
        double coordinate =
            Math.max(minCoord, Math.min(maxCoord, feature.getCenter(axis)));
        stats.merge(memberCount, coordinate, 0.0d);
      }
    }
  }

  /**
   * Returns whether the current refinement strategy should use an equal four-way split.
   *
   * <p>The quadtree-like variant is intentionally limited to 2D envelopes with non-zero width and
   * height. All other cases fall back to the current adaptive-axis refinement automatically so the
   * normal LAHilbert recursion remains safe for degenerate envelopes and future ND callers.
   *
   * @param envelope current partition envelope
   * @return {@code true} when an equal-quadtree split should be applied
   */
  private boolean useEqualQuadtreeRefinement(EnvelopeNDLite envelope) {
    return usesEqualQuadtreeStrategy() &&
        envelope != null &&
        envelope.getCoordinateDimension() == 2 &&
        envelope.getSideLength(0) > 0.0d &&
        envelope.getSideLength(1) > 0.0d;
  }

  /** Returns whether the current strategy refines by equal four-way quadtree-style splits. */
  private boolean usesEqualQuadtreeStrategy() {
    return SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_EQUAL_QUADTREE_COUNT.equals(refinementStrategy) ||
        SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_EQUAL_QUADTREE_LOAD.equals(refinementStrategy);
  }

  /** Returns whether one-axis refinement follows object-center offsets from the lower-left corner. */
  private boolean usesObjectCenterOffsetAxisStrategy() {
    return SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_OBJECT_CENTER_OFFSET_AXIS_LOAD.equals(
        refinementStrategy);
  }

  /** Returns whether the current strategy decides recursive refinement using feature count instead of bytes. */
  private boolean useCountDrivenRefinement() {
    return SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_EQUAL_QUADTREE_COUNT.equals(refinementStrategy);
  }

  /**
   * Splits one overloaded working partition into four equal quadrants, similar to a quadtree node split.
   *
   * <p>Unlike the current adaptive-axis refinement, this variant always bisects both
   * x and y at the geometric mid-point of the partition envelope. Each synopsis feature is
   * assigned to exactly one child using the same representative-coordinate rule as the baseline
   * refinement, so child member counts and loads remain additive rather than being replicated
   * across every child touched by the feature MBR. Children are ordered as:
   * 0 = lower-left, 1 = upper-left, 2 = lower-right, 3 = upper-right.
   *
   * @param parent overloaded partition that should be subdivided
   * @param features hierarchy-level lightweight features bound to the partition
   * @return non-empty child partitions, or {@code null} when the parent cannot be meaningfully split
   */
  private WorkingPartition[] splitPartitionEqualQuadtree(WorkingPartition parent,
                                                         AbstractFeature[] features) {
    if (parent == null || features == null || parent.featureIndexes == null || parent.featureIndexes.length < 2)
      return null;
    EnvelopeNDLite[] childEnvelopes = createEqualQuadtreeChildEnvelopes(parent.envelope);
    if (childEnvelopes == null)
      return null;

    int[][] childFeatureGroups =
        assignIndexesByRepresentativeQuadrant(features, parent.featureIndexes, parent.envelope);
    List<WorkingPartition> children = new ArrayList<>(4);
    int width = codeWidth(childEnvelopes.length);
    for (int childOrder = 0; childOrder < childEnvelopes.length; childOrder++) {
      EnvelopeNDLite childEnvelope = childEnvelopes[childOrder];
      int[] childFeatures = childFeatureGroups[childOrder];
      if (childFeatures.length == 0)
        continue;
      children.add(new WorkingPartition(
          childEnvelope,
          composeChildCode(parent.code, childOrder, width),
          parent.depth + 1,
          -1,
          sumStorageBytes(features, childFeatures),
          sumMemberCount(features, childFeatures),
          parent.levelIndex,
          childFeatures));
    }
    return children.size() <= 1 ? null : children.toArray(new WorkingPartition[0]);
  }

  /**
   * Assigns every feature index to exactly one equal-quadtree child by representative coordinate.
   *
   * <p>Coordinates on a split line are assigned to the right/upper child. Coordinates outside the
   * current parent envelope are clamped before classification, which mirrors the initial-grid
   * locator and keeps an intersecting synopsis feature in one deterministic child.
   */
  private int[][] assignIndexesByRepresentativeQuadrant(AbstractFeature[] features,
                                                        int[] featureIndexes,
                                                        EnvelopeNDLite parentEnvelope) {
    IntArray[] grouped = new IntArray[]{new IntArray(), new IntArray(), new IntArray(), new IntArray()};
    if (features == null
        || featureIndexes == null
        || parentEnvelope == null
        || parentEnvelope.getCoordinateDimension() < 2)
      return new int[][]{new int[0], new int[0], new int[0], new int[0]};
    double minX = parentEnvelope.getMinCoord(0);
    double maxX = parentEnvelope.getMaxCoord(0);
    double minY = parentEnvelope.getMinCoord(1);
    double maxY = parentEnvelope.getMaxCoord(1);
    double midX = (minX + maxX) * 0.5d;
    double midY = (minY + maxY) * 0.5d;
    for (int featureIndex : featureIndexes) {
      if (featureIndex < 0 || featureIndex >= features.length)
        continue;
      AbstractFeature feature = features[featureIndex];
      if (feature == null || feature.isEmpty())
        continue;
      double representativeX = Math.max(minX, Math.min(maxX, featureRepresentativeCoord(feature, 0)));
      double representativeY = Math.max(minY, Math.min(maxY, featureRepresentativeCoord(feature, 1)));
      int childOrder = (representativeX >= midX ? 2 : 0) + (representativeY >= midY ? 1 : 0);
      grouped[childOrder].add(featureIndex);
    }
    return new int[][]{
        grouped[0].toArray(),
        grouped[1].toArray(),
        grouped[2].toArray(),
        grouped[3].toArray()
    };
  }

  /**
   * Splits one effective partition into four equal quadrants and keeps only children that still
   * receive both left and right features.
   *
   * <p>This branch preserves the current effective-partition semantics:
   * left/right feature indexes are still filtered by child envelopes, one-sided children are
   * discarded immediately, and all later profile/job-planning logic keeps using the same
   * memory-load statistics as the baseline LAHilbert path.
   *
   * @param parent overloaded effective partition
   * @param leftFeatures left-input lightweight descriptors
   * @param rightFeatures right-input lightweight descriptors
   * @return effective children retained after the equal four-way split
   */
  private AssignedPartition[] splitEffectivePartitionEqualQuadtree(AssignedPartition parent,
                                                                   AbstractFeature[] leftFeatures,
                                                                   AbstractFeature[] rightFeatures) {
    if (parent == null || leftFeatures == null || rightFeatures == null)
      return null;
    EnvelopeNDLite[] childEnvelopes = createEqualQuadtreeChildEnvelopes(parent.envelope);
    if (childEnvelopes == null)
      return null;

    List<EffectiveChildCandidate> children = new ArrayList<>(4);
    int width = codeWidth(childEnvelopes.length);
    for (int childOrder = 0; childOrder < childEnvelopes.length; childOrder++) {
      EnvelopeNDLite childEnvelope = childEnvelopes[childOrder];
      int[] childLeftIndexes = filterIndexesByEnvelope(leftFeatures, parent.leftFeatureIndexes, childEnvelope);
      int[] childRightIndexes = filterIndexesByEnvelope(rightFeatures, parent.rightFeatureIndexes, childEnvelope);
      if (childLeftIndexes.length == 0 || childRightIndexes.length == 0)
        continue;
      // Same reasoning as the working-partition branch above: if both sides keep the exact same
      // membership as the parent, the split does not reduce the problem and should terminate.
      if (sameIndexSet(childLeftIndexes, parent.leftFeatureIndexes) &&
          sameIndexSet(childRightIndexes, parent.rightFeatureIndexes))
        continue;
      children.add(new EffectiveChildCandidate(
          childOrder,
          new AssignedPartition(
              childEnvelope,
              composeChildCode(parent.code, childOrder, width),
              parent.depth + 1,
              -1,
              sumStorageBytes(leftFeatures, childLeftIndexes) + sumStorageBytes(rightFeatures, childRightIndexes),
              safeFeatureCount(sumMemberCount(leftFeatures, childLeftIndexes) + sumMemberCount(rightFeatures, childRightIndexes)),
              childLeftIndexes,
              childRightIndexes)));
    }
    if (children.isEmpty())
      return null;
    if (!coalesceSmallSiblingsEnabled)
      return extractEffectiveChildren(children);
    return coalesceAdjacentEffectiveChildren(parent, -1, children, leftFeatures, rightFeatures);
  }

  /**
   * Builds the four equal child envelopes used by the quadtree-style refinement.
   *
   * @param parentEnvelope current 2D parent envelope
   * @return four child envelopes ordered lower-left, upper-left, lower-right, upper-right
   */
  private EnvelopeNDLite[] createEqualQuadtreeChildEnvelopes(EnvelopeNDLite parentEnvelope) {
    if (parentEnvelope == null || parentEnvelope.getCoordinateDimension() != 2)
      return null;
    double minX = parentEnvelope.getMinCoord(0);
    double maxX = parentEnvelope.getMaxCoord(0);
    double minY = parentEnvelope.getMinCoord(1);
    double maxY = parentEnvelope.getMaxCoord(1);
    double midX = (minX + maxX) * 0.5d;
    double midY = (minY + maxY) * 0.5d;
    if (!(midX > minX && midX < maxX && midY > minY && midY < maxY))
      return null;

    EnvelopeNDLite lowerLeft = new EnvelopeNDLite(parentEnvelope);
    lowerLeft.setMaxCoord(0, midX);
    lowerLeft.setMaxCoord(1, midY);

    EnvelopeNDLite upperLeft = new EnvelopeNDLite(parentEnvelope);
    upperLeft.setMaxCoord(0, midX);
    upperLeft.setMinCoord(1, midY);

    EnvelopeNDLite lowerRight = new EnvelopeNDLite(parentEnvelope);
    lowerRight.setMinCoord(0, midX);
    lowerRight.setMaxCoord(1, midY);

    EnvelopeNDLite upperRight = new EnvelopeNDLite(parentEnvelope);
    upperRight.setMinCoord(0, midX);
    upperRight.setMinCoord(1, midY);

    return new EnvelopeNDLite[]{lowerLeft, upperLeft, lowerRight, upperRight};
  }

  /**
   * Splits one overloaded partition into ordered children along the selected axis.
   *
   * Child partition codes are assigned after children are sorted by the same axis
   * so the hierarchy follows the document's "parent code + ordinal" rule.
   *
   * @param parent overloaded partition that should be subdivided
   * @param splitAxis axis chosen for the current split
   * @param features hierarchy level features currently bound to the partition
   * @return ordered child partitions, or {@code null} when the parent cannot be split safely
   */
  private WorkingPartition[] splitPartition(WorkingPartition parent,
                                            int splitAxis,
                                            AbstractFeature[] features) {
    if (parent.featureIndexes == null || parent.featureIndexes.length < 2)
      return null;

    int[] sortedIndexes = sortFeatureIndexes(parent.featureIndexes, features, splitAxis);
    int splitStep = computeSplitStepLength(parent.estimatedBytes, sortedIndexes.length);
    if (splitStep >= sortedIndexes.length)
      return null;
    int childCount = computeChildCount(sortedIndexes.length, splitStep);
    if (childCount <= 1)
      return null;
    double[] boundaries = computeSplitBoundaries(parent.envelope, sortedIndexes, features, splitAxis, splitStep, childCount);
    if (boundaries == null)
      return null;

    WorkingPartition[] children = new WorkingPartition[childCount];
    int width = codeWidth(childCount);
    for (int childOrder = 0; childOrder < childCount; childOrder++) {
      EnvelopeNDLite childEnvelope = new EnvelopeNDLite(parent.envelope);
      childEnvelope.setMinCoord(splitAxis, boundaries[childOrder]);
      childEnvelope.setMaxCoord(splitAxis, boundaries[childOrder + 1]);
      int from = childOrder * splitStep;
      int to = Math.min(sortedIndexes.length, (childOrder + 1) * splitStep);
      if (to <= from)
        return null;
      int[] childFeatures = Arrays.copyOfRange(sortedIndexes, from, to);

      children[childOrder] = new WorkingPartition(
          childEnvelope,
          composeChildCode(parent.code, childOrder, width),
          parent.depth + 1,
          splitAxis,
          sumStorageBytes(features, childFeatures),
          sumMemberCount(features, childFeatures),
          parent.levelIndex,
          childFeatures);
    }
    return children;
  }

  /**
   * Splits one effective partition using the document's step-length rule and keeps only
   * children that still receive both left and right features.
   *
   * @param parent overloaded effective partition
   * @param splitAxis axis chosen for the current split
   * @param leftFeatures left-input lightweight descriptors
   * @param rightFeatures right-input lightweight descriptors
   * @return effective children that remain valid after the split, or {@code null} when no valid split exists
   */
  private AssignedPartition[] splitEffectivePartition(AssignedPartition parent,
                                                      int splitAxis,
                                                      AbstractFeature[] leftFeatures,
                                                      AbstractFeature[] rightFeatures) {
    OrderedFeatureRef[] orderedFeatures = buildOrderedFeatures(parent, leftFeatures, rightFeatures, splitAxis);
    if (orderedFeatures.length < 2)
      return null;
    int splitStep = computeSplitStepLength(parent.estimatedBytes, orderedFeatures.length);
    if (splitStep >= orderedFeatures.length)
      return null;
    int childCount = computeChildCount(orderedFeatures.length, splitStep);
    if (childCount <= 1)
      return null;
    double[] boundaries = computeSplitBoundaries(parent.envelope, orderedFeatures, splitAxis, splitStep, childCount);
    if (boundaries == null)
      return null;

    List<EffectiveChildCandidate> children = new ArrayList<>(childCount);
    int width = codeWidth(childCount);
    for (int childOrder = 0; childOrder < childCount; childOrder++) {
      EnvelopeNDLite childEnvelope = new EnvelopeNDLite(parent.envelope);
      childEnvelope.setMinCoord(splitAxis, boundaries[childOrder]);
      childEnvelope.setMaxCoord(splitAxis, boundaries[childOrder + 1]);
      int[] childLeftIndexes = filterIndexesByEnvelope(leftFeatures, parent.leftFeatureIndexes, childEnvelope);
      int[] childRightIndexes = filterIndexesByEnvelope(rightFeatures, parent.rightFeatureIndexes, childEnvelope);
      if (childLeftIndexes.length == 0 || childRightIndexes.length == 0)
        continue;
      children.add(new EffectiveChildCandidate(
          childOrder,
          new AssignedPartition(
              childEnvelope,
              composeChildCode(parent.code, childOrder, width),
              parent.depth + 1,
              splitAxis,
              sumStorageBytes(leftFeatures, childLeftIndexes) + sumStorageBytes(rightFeatures, childRightIndexes),
              safeFeatureCount(sumMemberCount(leftFeatures, childLeftIndexes) + sumMemberCount(rightFeatures, childRightIndexes)),
              childLeftIndexes,
              childRightIndexes)));
    }
    if (children.isEmpty())
      return null;
    if (!coalesceSmallSiblingsEnabled)
      return extractEffectiveChildren(children);
    return coalesceAdjacentEffectiveChildren(parent, splitAxis, children, leftFeatures, rightFeatures);
  }

  /**
   * Greedily merges contiguous effective sibling partitions from the same parent split.
   *
   * <p>The merge is intentionally local:
   * <ul>
   *   <li>only children generated by the same parent split participate,</li>
   *   <li>only contiguous child-order runs may merge, and</li>
   *   <li>the merged partition must still fit under the same memory-load threshold.</li>
   * </ul>
   *
   * <p>The load check is based on the unique union of left/right feature indexes rather than
   * summing child loads directly, because cross-boundary objects may already appear in several
   * adjacent children. This makes the method a natural "post-split compaction" pass that keeps
   * the original memory-driven semantics while reducing over-fragmentation.
   *
   * @param parent parent effective partition that produced the ordered child candidates
   * @param splitAxis axis used by the current split
   * @param orderedChildren effective child candidates sorted by their original child order
   * @param leftFeatures left-input lightweight descriptors used to recompute unique merged load
   * @param rightFeatures right-input lightweight descriptors used to recompute unique merged load
   * @return compacted ordered children, or {@code null} when the split collapses back to the
   *         original parent and should therefore be ignored
   */
  private AssignedPartition[] coalesceAdjacentEffectiveChildren(AssignedPartition parent,
                                                                int splitAxis,
                                                                List<EffectiveChildCandidate> orderedChildren,
                                                                AbstractFeature[] leftFeatures,
                                                                AbstractFeature[] rightFeatures) {
    if (orderedChildren == null || orderedChildren.isEmpty())
      return null;
    if (orderedChildren.size() == 1)
      return extractEffectiveChildren(orderedChildren);

    List<CoalescedAssignedPartitionState> groups = new ArrayList<>();
    EffectiveChildCandidate firstChild = orderedChildren.get(0);
    int previousOrder = firstChild.childOrder;
    EnvelopeNDLite currentEnvelope = new EnvelopeNDLite(firstChild.partition.envelope);
    int[] currentLeftIndexes = canonicalizeFeatureIndexes(firstChild.partition.leftFeatureIndexes);
    int[] currentRightIndexes = canonicalizeFeatureIndexes(firstChild.partition.rightFeatureIndexes);
    int currentGroupChildCount = 1;

    for (int i = 1; i < orderedChildren.size(); i++) {
      EffectiveChildCandidate child = orderedChildren.get(i);
      if (child.childOrder != previousOrder + 1) {
        groups.add(new CoalescedAssignedPartitionState(
            currentEnvelope,
            currentLeftIndexes,
            currentRightIndexes,
            currentGroupChildCount));
        currentEnvelope = new EnvelopeNDLite(child.partition.envelope);
        currentLeftIndexes = canonicalizeFeatureIndexes(child.partition.leftFeatureIndexes);
        currentRightIndexes = canonicalizeFeatureIndexes(child.partition.rightFeatureIndexes);
        currentGroupChildCount = 1;
        previousOrder = child.childOrder;
        continue;
      }

      int[] mergedLeftIndexes = mergeCanonicalFeatureIndexes(currentLeftIndexes, canonicalizeFeatureIndexes(child.partition.leftFeatureIndexes));
      int[] mergedRightIndexes = mergeCanonicalFeatureIndexes(currentRightIndexes, canonicalizeFeatureIndexes(child.partition.rightFeatureIndexes));
      long mergedBytes = sumStorageBytes(leftFeatures, mergedLeftIndexes) + sumStorageBytes(rightFeatures, mergedRightIndexes);
      if (mergedBytes <= targetPartitionSizeBytes) {
        currentEnvelope.merge(child.partition.envelope);
        currentLeftIndexes = mergedLeftIndexes;
        currentRightIndexes = mergedRightIndexes;
        currentGroupChildCount++;
      } else {
        groups.add(new CoalescedAssignedPartitionState(
            currentEnvelope,
            currentLeftIndexes,
            currentRightIndexes,
            currentGroupChildCount));
        currentEnvelope = new EnvelopeNDLite(child.partition.envelope);
        currentLeftIndexes = canonicalizeFeatureIndexes(child.partition.leftFeatureIndexes);
        currentRightIndexes = canonicalizeFeatureIndexes(child.partition.rightFeatureIndexes);
        currentGroupChildCount = 1;
      }
      previousOrder = child.childOrder;
    }
    groups.add(new CoalescedAssignedPartitionState(
        currentEnvelope,
        currentLeftIndexes,
        currentRightIndexes,
        currentGroupChildCount));

    if (groups.size() == 1 &&
        sameEnvelope(parent.envelope, groups.get(0).envelope) &&
        sameFeatureIndexes(parent.leftFeatureIndexes, groups.get(0).leftFeatureIndexes) &&
        sameFeatureIndexes(parent.rightFeatureIndexes, groups.get(0).rightFeatureIndexes)) {
      return null;
    }

    if (groups.size() < orderedChildren.size()) {
      coalescedSiblingPartitionReductionCount += orderedChildren.size() - groups.size();
      for (CoalescedAssignedPartitionState group : groups) {
        if (group.childCount > 1)
          coalescedSiblingGroupCount++;
      }
    }

    int width = codeWidth(groups.size());
    AssignedPartition[] mergedPartitions = new AssignedPartition[groups.size()];
    for (int i = 0; i < groups.size(); i++) {
      CoalescedAssignedPartitionState group = groups.get(i);
      long leftBytes = sumStorageBytes(leftFeatures, group.leftFeatureIndexes);
      long rightBytes = sumStorageBytes(rightFeatures, group.rightFeatureIndexes);
      int featureCount = safeFeatureCount(
          sumMemberCount(leftFeatures, group.leftFeatureIndexes) +
              sumMemberCount(rightFeatures, group.rightFeatureIndexes));
      mergedPartitions[i] = new AssignedPartition(
          group.envelope,
          composeChildCode(parent.code, i, width),
          parent.depth + 1,
          splitAxis,
          leftBytes + rightBytes,
          featureCount,
          group.leftFeatureIndexes,
          group.rightFeatureIndexes);
    }
    return mergedPartitions;
  }

  /** Extracts the concrete child partitions from the ordered candidate wrappers. */
  private AssignedPartition[] extractEffectiveChildren(List<EffectiveChildCandidate> children) {
    AssignedPartition[] extracted = new AssignedPartition[children.size()];
    for (int i = 0; i < children.size(); i++)
      extracted[i] = children.get(i).partition;
    return extracted;
  }

  /**
   * Canonicalizes one feature-index list by sorting it and removing duplicates.
   *
   * <p>Sibling merging compares and unions child memberships repeatedly, so a stable canonical
   * representation keeps later comparisons inexpensive and order-independent.
   *
   * @param featureIndexes raw feature indexes collected for one effective child
   * @return sorted unique indexes
   */
  private int[] canonicalizeFeatureIndexes(int[] featureIndexes) {
    if (featureIndexes == null || featureIndexes.length == 0)
      return new int[0];
    int[] sorted = Arrays.copyOf(featureIndexes, featureIndexes.length);
    Arrays.sort(sorted);
    int uniqueCount = 1;
    for (int i = 1; i < sorted.length; i++) {
      if (sorted[i] != sorted[uniqueCount - 1])
        sorted[uniqueCount++] = sorted[i];
    }
    return uniqueCount == sorted.length ? sorted : Arrays.copyOf(sorted, uniqueCount);
  }

  /**
   * Unions two already canonicalized feature-index arrays.
   *
   * @param left sorted unique feature indexes
   * @param right sorted unique feature indexes
   * @return sorted unique union of both arrays
   */
  private int[] mergeCanonicalFeatureIndexes(int[] left, int[] right) {
    if (left == null || left.length == 0)
      return right == null ? new int[0] : Arrays.copyOf(right, right.length);
    if (right == null || right.length == 0)
      return Arrays.copyOf(left, left.length);
    int[] merged = new int[left.length + right.length];
    int leftCursor = 0;
    int rightCursor = 0;
    int writeCursor = 0;
    while (leftCursor < left.length && rightCursor < right.length) {
      int leftValue = left[leftCursor];
      int rightValue = right[rightCursor];
      int nextValue;
      if (leftValue < rightValue) {
        nextValue = leftValue;
        leftCursor++;
      } else if (leftValue > rightValue) {
        nextValue = rightValue;
        rightCursor++;
      } else {
        nextValue = leftValue;
        leftCursor++;
        rightCursor++;
      }
      if (writeCursor == 0 || merged[writeCursor - 1] != nextValue)
        merged[writeCursor++] = nextValue;
    }
    while (leftCursor < left.length) {
      int nextValue = left[leftCursor++];
      if (writeCursor == 0 || merged[writeCursor - 1] != nextValue)
        merged[writeCursor++] = nextValue;
    }
    while (rightCursor < right.length) {
      int nextValue = right[rightCursor++];
      if (writeCursor == 0 || merged[writeCursor - 1] != nextValue)
        merged[writeCursor++] = nextValue;
    }
    return writeCursor == merged.length ? merged : Arrays.copyOf(merged, writeCursor);
  }

  /** Returns whether two feature-index arrays describe the same unique membership set. */
  private boolean sameFeatureIndexes(int[] left, int[] right) {
    return Arrays.equals(canonicalizeFeatureIndexes(left), canonicalizeFeatureIndexes(right));
  }

  /** Returns whether two envelopes cover the same coordinate range. */
  private boolean sameEnvelope(EnvelopeNDLite left, EnvelopeNDLite right) {
    if (left == right)
      return true;
    if (left == null || right == null || left.getCoordinateDimension() != right.getCoordinateDimension())
      return false;
    for (int d = 0; d < left.getCoordinateDimension(); d++) {
      if (Double.compare(left.getMinCoord(d), right.getMinCoord(d)) != 0 ||
          Double.compare(left.getMaxCoord(d), right.getMaxCoord(d)) != 0) {
        return false;
      }
    }
    return true;
  }

  /** Maps one coordinate to its zero-based cell position on a specific axis. */
  private int locateCell(double coordinate, int axis, int sidePartitions) {
    double min = inputMBR.getMinCoord(axis);
    double max = inputMBR.getMaxCoord(axis);
    double sideLength = max - min;
    if (sideLength <= 0 || sidePartitions <= 1)
      return 0;
    int cell = (int) Math.floor((coordinate - min) * sidePartitions / sideLength);
    if (cell < 0)
      return 0;
    if (cell >= sidePartitions)
      return sidePartitions - 1;
    return cell;
  }

  /** Materializes the envelope of one initial Hilbert cell from its per-axis indexes. */
  private EnvelopeNDLite createCellEnvelope(int[] coords, int sidePartitions) {
    EnvelopeNDLite envelope = new EnvelopeNDLite(inputMBR);
    for (int d = 0; d < coords.length; d++) {
      double min = inputMBR.getMinCoord(d);
      double max = inputMBR.getMaxCoord(d);
      double sideLength = max - min;
      if (sideLength <= 0 || sidePartitions <= 1) {
        envelope.setMinCoord(d, min);
        envelope.setMaxCoord(d, max);
      } else {
        double cellMin = min + sideLength * coords[d] / sidePartitions;
        double cellMax = coords[d] == sidePartitions - 1 ? max : min + sideLength * (coords[d] + 1) / sidePartitions;
        envelope.setMinCoord(d, cellMin);
        envelope.setMaxCoord(d, cellMax);
      }
    }
    return envelope;
  }

  /**
   * Registers one feature in every initial Hilbert cell that intersects its MBR.
   *
   * <p>This preserves the replication semantics required by the join-oriented
   * partitioning path where cross-cell objects may belong to multiple partitions.
   * @param feature feature being replicated across all intersecting initial cells
   * @param featureIndex index of the feature in the left or right feature array
   * @param sidePartitions number of cells per axis in the first-level Hilbert grid
   * @param groupedAssignments accumulator of cell-to-feature assignments
   * @param leftSide whether the feature belongs to the left input
   */
  private void collectIntersectingInitialCells(AbstractFeature feature,
                                               int featureIndex,
                                               int sidePartitions,
                                               Map<CellKey, CellAssignments> groupedAssignments,
                                               boolean leftSide) {
    int[] minCoords = new int[inputMBR.getCoordinateDimension()];
    int[] maxCoords = new int[inputMBR.getCoordinateDimension()];
    for (int d = 0; d < inputMBR.getCoordinateDimension(); d++) {
      minCoords[d] = locateCell(feature.getMinCoord(d), d, sidePartitions);
      maxCoords[d] = locateCell(feature.getMaxCoord(d), d, sidePartitions);
    }
    collectIntersectingInitialCells(feature, featureIndex, minCoords, maxCoords, 0, new int[minCoords.length], groupedAssignments, leftSide);
  }

  /**
   * Recursively enumerates the multi-dimensional initial cells overlapped by one feature.
   *
   * @param feature feature being replicated
   * @param featureIndex index of the feature in the side-specific feature array
   * @param minCoords per-axis minimum intersecting cell index
   * @param maxCoords per-axis maximum intersecting cell index
   * @param axis recursion cursor over dimensions
   * @param currentCoords mutable work array that records the current cell during enumeration
   * @param groupedAssignments accumulator of cell-to-feature assignments
   * @param leftSide whether the feature belongs to the left input
   */
  private void collectIntersectingInitialCells(AbstractFeature feature,
                                               int featureIndex,
                                               int[] minCoords,
                                               int[] maxCoords,
                                               int axis,
                                               int[] currentCoords,
                                               Map<CellKey, CellAssignments> groupedAssignments,
                                               boolean leftSide) {
    if (axis == currentCoords.length) {
      int[] coords = Arrays.copyOf(currentCoords, currentCoords.length);
      CellKey key = new CellKey(coords);
      groupedAssignments.computeIfAbsent(key, ignored -> new CellAssignments()).add(featureIndex, leftSide);
      return;
    }
    for (int coord = minCoords[axis]; coord <= maxCoords[axis]; coord++) {
      currentCoords[axis] = coord;
      collectIntersectingInitialCells(feature, featureIndex, minCoords, maxCoords, axis + 1, currentCoords, groupedAssignments, leftSide);
    }
  }

  /** Sums the heap-oriented load estimate of the selected lightweight features. */
  private long sumStorageBytes(AbstractFeature[] features, int[] featureIndexes) {
    long totalBytes = 0L;
    for (int featureIndex : featureIndexes) {
      AbstractFeature feature = features[featureIndex];
      if (feature != null)
        totalBytes += Math.max(0L, feature.getLoadWeight());
    }
    return Math.max(1L, totalBytes);
  }

  /** Computes the tight envelope of selected lightweight features, with a fallback when empty. */
  private EnvelopeNDLite computeEnvelope(AbstractFeature[] features, int[] featureIndexes, EnvelopeNDLite fallbackEnvelope) {
    EnvelopeNDLite computedEnvelope = fallbackEnvelope == null
        ? new EnvelopeNDLite(inputMBR.getCoordinateDimension())
        : new EnvelopeNDLite(fallbackEnvelope);
    computedEnvelope.setEmpty();
    for (int featureIndex : featureIndexes) {
      AbstractFeature feature = features[featureIndex];
      if (feature != null && !feature.isEmpty()) {
        if (computedEnvelope.isEmpty())
          computedEnvelope.set(feature);
        else
          computedEnvelope.merge(feature);
      }
    }
    if (computedEnvelope.isEmpty() && fallbackEnvelope != null)
      computedEnvelope.set(fallbackEnvelope);
    return computedEnvelope;
  }

  /** Sums the finest-level member counts represented by the selected lightweight features. */
  private int sumMemberCount(AbstractFeature[] features, int[] featureIndexes) {
    long totalMembers = 0L;
    for (int featureIndex : featureIndexes) {
      AbstractFeature feature = features[featureIndex];
      if (feature != null)
        totalMembers += Math.max(1L, feature.getMemberCount());
    }
    return safeFeatureCount(totalMembers);
  }

  /** Safely converts an aggregated member count to the int-based partition contract. */
  private int safeFeatureCount(long count) {
    return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, count));
  }

  /** Orders candidate child members by the representative coordinate of the split axis. */
  private int[] sortFeatureIndexes(int[] featureIndexes, AbstractFeature[] features, int splitAxis) {
    Integer[] boxed = new Integer[featureIndexes.length];
    for (int i = 0; i < featureIndexes.length; i++)
      boxed[i] = featureIndexes[i];
    Arrays.sort(boxed, Comparator
        // Document 1.1 uses the point coordinate for points and the lower-left MBR
        // coordinate for non-point objects as the ordering key during refinement.
        .comparingDouble((Integer i) -> featureOrderingCoord(features[i], splitAxis))
        .thenComparingInt(i -> i));
    int[] sorted = new int[boxed.length];
    for (int i = 0; i < boxed.length; i++)
      sorted[i] = boxed[i];
    return sorted;
  }

  /** Creates the ordered left/right feature sequence used by step-length-based refinement. */
  private OrderedFeatureRef[] buildOrderedFeatures(AssignedPartition partition,
                                                   AbstractFeature[] leftFeatures,
                                                   AbstractFeature[] rightFeatures,
                                                   int splitAxis) {
    OrderedFeatureRef[] ordered = new OrderedFeatureRef[partition.leftFeatureIndexes.length + partition.rightFeatureIndexes.length];
    int offset = 0;
    for (int featureIndex : partition.leftFeatureIndexes) {
      ordered[offset++] = new OrderedFeatureRef(
          true,
          featureIndex,
          featureOrderingCoord(leftFeatures[featureIndex], splitAxis));
    }
    for (int featureIndex : partition.rightFeatureIndexes) {
      ordered[offset++] = new OrderedFeatureRef(
          false,
          featureIndex,
          featureOrderingCoord(rightFeatures[featureIndex], splitAxis));
    }
    Arrays.sort(ordered, Comparator
        .comparingDouble((OrderedFeatureRef ref) -> ref.representativeCoord)
        .thenComparing(ref -> ref.leftSide)
        .thenComparingInt(ref -> ref.featureIndex));
    return ordered;
  }

  /** Computes child split boundaries from the ordered representative coordinates. */
  private double[] computeSplitBoundaries(EnvelopeNDLite envelope,
                                          int[] sortedIndexes,
                                          AbstractFeature[] features,
                                          int splitAxis,
                                          int splitStep,
                                          int childCount) {
    double[] boundaries = new double[childCount + 1];
    boundaries[0] = envelope.getMinCoord(splitAxis);
    boundaries[childCount] = envelope.getMaxCoord(splitAxis);
    double previous = boundaries[0];
    for (int child = 1; child < childCount; child++) {
      int splitIndex = child * splitStep;
      if (splitIndex <= 0 || splitIndex >= sortedIndexes.length)
        return null;
      double left = featureOrderingCoord(features[sortedIndexes[splitIndex - 1]], splitAxis);
      double right = featureOrderingCoord(features[sortedIndexes[splitIndex]], splitAxis);
      double boundary = (left + right) / 2.0d;
      if (!(boundary > previous && boundary < envelope.getMaxCoord(splitAxis)))
        return null;
      boundaries[child] = boundary;
      previous = boundary;
    }
    return boundaries;
  }

  /** Computes child split boundaries from ordered left/right feature references. */
  private double[] computeSplitBoundaries(EnvelopeNDLite envelope,
                                          OrderedFeatureRef[] orderedFeatures,
                                          int splitAxis,
                                          int splitStep,
                                          int childCount) {
    double[] boundaries = new double[childCount + 1];
    boundaries[0] = envelope.getMinCoord(splitAxis);
    boundaries[childCount] = envelope.getMaxCoord(splitAxis);
    double previous = boundaries[0];
    for (int child = 1; child < childCount; child++) {
      int splitIndex = child * splitStep;
      if (splitIndex <= 0 || splitIndex >= orderedFeatures.length)
        return null;
      double left = orderedFeatures[splitIndex - 1].representativeCoord;
      double right = orderedFeatures[splitIndex].representativeCoord;
      double boundary = (left + right) / 2.0d;
      if (!(boundary > previous && boundary < envelope.getMaxCoord(splitAxis)))
        return null;
      boundaries[child] = boundary;
      previous = boundary;
    }
    return boundaries;
  }

  /**
   * Computes the sequence step length used by dynamic refinement.
   *
   * <p>The average object size is estimated from the current partition load and the
   * number of currently assigned objects. The threshold budget is then converted to a
   * count-based step that controls how many ordered objects fall into one child.
   */
  private int computeSplitStepLength(long partitionBytes, int objectCount) {
    if (objectCount <= 1 || partitionBytes <= 0L || targetPartitionSizeBytes <= 0L)
      return objectCount;
    double averageObjectBytes = Math.max(1.0d, partitionBytes / (double) objectCount);
    return Math.max(1, (int) Math.floor(targetPartitionSizeBytes / averageObjectBytes));
  }

  /** Returns how many child subsets the current step length implies. */
  private int computeChildCount(int objectCount, int splitStep) {
    if (objectCount <= 0 || splitStep <= 0)
      return 0;
    return (int) Math.ceil(objectCount / (double) splitStep);
  }

  /**
   * Builds the immutable effective-plan result directly from the final assigned leaves.
   *
   * <p>This keeps the execution-side routing tables aligned with exactly the same
   * left/right assignments that guided effective-partition pruning and dynamic refinement.
   */
  private EffectivePlan buildEffectivePlanFromAssignedPartitions(List<AssignedPartition> effectiveLeaves,
                                                                 AbstractFeature[] leftFeatures,
                                                                 AbstractFeature[] rightFeatures) {
    if (effectiveLeaves == null || effectiveLeaves.isEmpty())
      return new EffectivePlan(this, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    List<cn.edu.whu.spatialJoin.cost.PartProfile> partitionProfiles = new ArrayList<>(effectiveLeaves.size());
    List<FeatureRef> leftAssignments = new ArrayList<>();
    List<FeatureRef> rightAssignments = new ArrayList<>();
    for (int partitionId = 0; partitionId < effectiveLeaves.size(); partitionId++) {
      AssignedPartition partition = effectiveLeaves.get(partitionId);
      long leftBytes = sumStorageBytes(leftFeatures, partition.leftFeatureIndexes);
      long rightBytes = sumStorageBytes(rightFeatures, partition.rightFeatureIndexes);
      double leftComplexity = sumComplexity(leftFeatures, partition.leftFeatureIndexes);
      double rightComplexity = sumComplexity(rightFeatures, partition.rightFeatureIndexes);
      long leftVertexCount = sumVertexCount(leftFeatures, partition.leftFeatureIndexes);
      long rightVertexCount = sumVertexCount(rightFeatures, partition.rightFeatureIndexes);
      partitionProfiles.add(new cn.edu.whu.spatialJoin.cost.PartProfile(
          partitionId,
          partition.code,
          partition.envelope,
          partition.leftFeatureIndexes.length,
          partition.rightFeatureIndexes.length,
          leftBytes,
          rightBytes,
          leftComplexity,
          rightComplexity,
          leftVertexCount,
          rightVertexCount,
          minVertexCount(leftFeatures, partition.leftFeatureIndexes),
          minVertexCount(rightFeatures, partition.rightFeatureIndexes),
          maxVertexCount(leftFeatures, partition.leftFeatureIndexes),
          maxVertexCount(rightFeatures, partition.rightFeatureIndexes)));
      appendFeatureAssignments(leftAssignments, leftFeatures, partition.leftFeatureIndexes, partitionId);
      appendFeatureAssignments(rightAssignments, rightFeatures, partition.rightFeatureIndexes, partitionId);
    }
    return new EffectivePlan(this, partitionProfiles, leftAssignments, rightAssignments);
  }

  /** Sums the feature-level computation complexities represented by one assigned partition side. */
  private double sumComplexity(AbstractFeature[] features, int[] featureIndexes) {
    double totalComplexity = 0.0d;
    if (features == null || featureIndexes == null)
      return totalComplexity;
    for (int featureIndex : featureIndexes) {
      if (featureIndex < 0 || featureIndex >= features.length)
        continue;
      AbstractFeature feature = features[featureIndex];
      if (feature != null)
        totalComplexity += Math.max(0.0d, feature.getComplexityEstimate());
    }
    return Math.max(0.0d, totalComplexity);
  }

  /** Sums the feature-level vertex counts represented by one assigned partition side. */
  private long sumVertexCount(AbstractFeature[] features, int[] featureIndexes) {
    long totalVertexCount = 0L;
    if (features == null || featureIndexes == null)
      return totalVertexCount;
    for (int featureIndex : featureIndexes) {
      if (featureIndex < 0 || featureIndex >= features.length)
        continue;
      AbstractFeature feature = features[featureIndex];
      if (feature != null)
        totalVertexCount += Math.max(0L, feature.getNumPoints());
    }
    return totalVertexCount;
  }

  /** Returns the smallest feature-level vertex count represented by one assigned partition side. */
  private long minVertexCount(AbstractFeature[] features, int[] featureIndexes) {
    long minVertexCount = Long.MAX_VALUE;
    if (features == null || featureIndexes == null)
      return 0L;
    for (int featureIndex : featureIndexes) {
      if (featureIndex < 0 || featureIndex >= features.length)
        continue;
      AbstractFeature feature = features[featureIndex];
      if (feature != null)
        minVertexCount = Math.min(minVertexCount, Math.max(0L, feature.getNumPoints()));
    }
    return minVertexCount == Long.MAX_VALUE ? 0L : minVertexCount;
  }

  /** Returns the largest feature-level vertex count represented by one assigned partition side. */
  private long maxVertexCount(AbstractFeature[] features, int[] featureIndexes) {
    long maxVertexCount = 0L;
    if (features == null || featureIndexes == null)
      return maxVertexCount;
    for (int featureIndex : featureIndexes) {
      if (featureIndex < 0 || featureIndex >= features.length)
        continue;
      AbstractFeature feature = features[featureIndex];
      if (feature != null)
        maxVertexCount = Math.max(maxVertexCount, Math.max(0L, feature.getNumPoints()));
    }
    return maxVertexCount;
  }

  /** Appends OID-to-partition mappings for one side of the input. */
  private void appendFeatureAssignments(List<FeatureRef> assignments,
                                        AbstractFeature[] features,
                                        int[] featureIndexes,
                                        int partitionId) {
    if (features == null || featureIndexes == null)
      return;
    for (int featureIndex : featureIndexes) {
      if (featureIndex < 0 || featureIndex >= features.length)
        continue;
      AbstractFeature feature = features[featureIndex];
      if (feature != null)
        assignments.add(new FeatureRef(feature.getOid(), partitionId));
    }
  }

  /**
   * Returns the representative coordinate used by LAHilbert for initial cell assignment
   * and one-axis refinement ordering.
   *
   * Points use their own coordinate and non-point objects use the lower-left MBR
   * corner described in the design document. Since a point's min/max coordinate is
   * identical, using the minimum bound keeps both cases aligned in one helper.
   */
  private double featureRepresentativeCoord(AbstractFeature feature, int axis) {
    if (feature == null || feature.getCoordinateDimension() <= axis)
      return 0.0d;
    return feature.getMinCoord(axis);
  }

  /**
   * Returns the coordinate used to order descriptors and derive child boundaries.
   *
   * <p>The object-center-offset strategy uses the MBR center along its selected axis. The baseline
   * adaptive-axis strategy keeps the lower-left-MBR coordinate. Both strategies still share the
   * same child-count calculation, intersection mapping, cross-partition replication, pruning, and
   * materialization paths.
   */
  private double featureOrderingCoord(AbstractFeature feature, int axis) {
    if (feature == null || feature.getCoordinateDimension() <= axis)
      return 0.0d;
    return usesObjectCenterOffsetAxisStrategy()
        ? feature.getCenter(axis)
        : featureRepresentativeCoord(feature, axis);
  }

  /** Converts legacy representative-point input into lightweight abstract features. */
  private AbstractFeature[] pointsToAbstractFeatures(Summary summary, double[][] points) {
    if (points == null || points.length == 0 || points[0].length == 0)
      return new AbstractFeature[0];
    int featureCount = points[0].length;
    AbstractFeature[] features = new AbstractFeature[featureCount];
    long estimatedBytesPerFeature =
        summary == null || featureCount == 0 ? 1L : Math.max(1L, Math.round(summary.size() / (double) featureCount));
    for (int i = 0; i < featureCount; i++) {
      EnvelopeNDLite pointEnvelope = new EnvelopeNDLite(points.length);
      for (int d = 0; d < points.length; d++) {
        pointEnvelope.setMinCoord(d, points[d][i]);
        pointEnvelope.setMaxCoord(d, points[d][i]);
      }
      features[i] = new AbstractFeature(pointEnvelope, i, 1, estimatedBytesPerFeature, pointEnvelope.getArea(),
          cn.edu.whu.spatialJoin.enums.GeometryType.POINT);
    }
    return features;
  }

  /** Returns the configured intersection of left/right dataset envelopes when available. */
  private EnvelopeNDLite resolveConfiguredGlobalIntersection(int numDimensions) {
    if (configuredLeftMin == null || configuredLeftMax == null || configuredRightMin == null || configuredRightMax == null)
      return null;
    if (configuredLeftMin.length != numDimensions || configuredLeftMax.length != numDimensions ||
        configuredRightMin.length != numDimensions || configuredRightMax.length != numDimensions) {
      return null;
    }
    EnvelopeNDLite leftEnvelope = new EnvelopeNDLite(configuredLeftMin, configuredLeftMax);
    EnvelopeNDLite rightEnvelope = new EnvelopeNDLite(configuredRightMin, configuredRightMax);
    return leftEnvelope.intersectionEnvelope(rightEnvelope);
  }

  /** Keeps only feature indexes whose envelopes intersect the target child envelope. */
  private int[] filterIndexesByEnvelope(AbstractFeature[] features, int[] candidateIndexes, EnvelopeNDLite envelope) {
    if (candidateIndexes == null || candidateIndexes.length == 0 || envelope == null)
      return new int[0];
    IntArray filtered = new IntArray();
    for (int candidateIndex : candidateIndexes) {
      AbstractFeature feature = features[candidateIndex];
      if (feature != null && feature.intersectsEnvelopeClosed(envelope))
        filtered.add(candidateIndex);
    }
    return filtered.toArray();
  }

  /** Returns whether the child preserved exactly the same feature membership as the parent. */
  private boolean sameIndexSet(int[] childIndexes, int[] parentIndexes) {
    return childIndexes != null &&
        parentIndexes != null &&
        childIndexes.length == parentIndexes.length &&
        Arrays.equals(childIndexes, parentIndexes);
  }

  /** Parses a comma-separated coordinate list from options into a numeric array. */
  private static double[] parseCoords(String value) {
    if (value == null || value.trim().isEmpty())
      return null;
    String[] parts = value.split(",");
    double[] coords = new double[parts.length];
    for (int i = 0; i < parts.length; i++)
      coords[i] = Double.parseDouble(parts[i].trim());
    return coords;
  }

  /** Reads an optional size value from options and returns `-1` when absent. */
  private static long parseSize(SpatialJoinOptions options, String key) {
    if (options == null || !options.contains(key))
      return -1L;
    return options.getSizeAsBytes(key, -1L);
  }

  /** Reads an optional long value from options and returns {@code -1} when absent. */
  private static long parseLong(SpatialJoinOptions options, String key) {
    if (options == null || !options.contains(key))
      return -1L;
    return options.getLong(key, -1L);
  }

  /** Computes `2^exponent` with overflow protection for large exponents. */
  private static long safePow2(long exponent) {
    if (exponent <= 0)
      return 1L;
    if (exponent >= 62)
      return Long.MAX_VALUE;
    return 1L << exponent;
  }

  @Override
  public int numPartitions() {
    return effectivePartitionCount;
  }

  /** Returns whether the current LAHilbert instance should replicate features on overlap. */
  @Override
  public boolean isDisjoint() {
    return disjointPartitions;
  }

  /** Returns the single target partition used by non-disjoint assignment paths. */
  @Override
  public synchronized int overlapPartition(EnvelopeNDLite mbr) {
    if (leafPartitions == null || leafPartitions.length == 0)
      return 0;
    for (int i = 0; i < leafPartitions.length; i++) {
      if (containsCenter(leafPartitions[i].envelope, mbr))
        return i;
    }
    return 0;
  }

  /** Collects every partition whose envelope overlaps the query envelope. */
  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    if (SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE_LINEAR.equals(partitionRoutingMode)) {
      overlapPartitionsLinear(mbr, matchedPartitions);
      return;
    }

    boolean routed = overlapPartitionsByGridDirectory(mbr, matchedPartitions);
    if (!routed) {
      overlapPartitionsLinear(mbr, matchedPartitions);
      return;
    }

    if (SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE_VALIDATE.equals(partitionRoutingMode)) {
      IntArray linearMatches = new IntArray();
      overlapPartitionsLinear(mbr, linearMatches);
      if (!samePartitionIds(matchedPartitions, linearMatches)) {
        throw new IllegalStateException(
            "LA-Hilbert grid-directory routing mismatch: indexed=" +
                Arrays.toString(matchedPartitions.toArray()) +
                ", linear=" + Arrays.toString(linearMatches.toArray()) +
                ", query=" + mbr);
      }
    }
  }

  /** Historical all-leaf scan retained as a correctness baseline and fallback. */
  private void overlapPartitionsLinear(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    matchedPartitions.clear();
    if (leafPartitions == null || mbr == null)
      return;
    for (int i = 0; i < leafPartitions.length; i++) {
      if (leafPartitions[i].envelope.intersectsEnvelopeClosed(mbr))
        matchedPartitions.add(i);
    }
  }

  /** Routes through the regular initial grid and checks only final leaves rooted in touched cells. */
  private boolean overlapPartitionsByGridDirectory(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    matchedPartitions.clear();
    if (leafPartitions == null || leafPartitions.length == 0 || mbr == null)
      return true;
    LeafRoutingDirectory directory = getOrBuildLeafRoutingDirectory();
    if (directory == null || mbr.getCoordinateDimension() != 2)
      return false;

    int minX = Math.max(0, locateCell(mbr.getMinCoord(0), 0, directory.sidePartitions) - 1);
    int maxX = Math.min(
        directory.sidePartitions - 1,
        locateCell(mbr.getMaxCoord(0), 0, directory.sidePartitions) + 1);
    int minY = Math.max(0, locateCell(mbr.getMinCoord(1), 1, directory.sidePartitions) - 1);
    int maxY = Math.min(
        directory.sidePartitions - 1,
        locateCell(mbr.getMaxCoord(1), 1, directory.sidePartitions) + 1);
    long touchedCellCount = (long) (maxX - minX + 1) * (long) (maxY - minY + 1);
    if (touchedCellCount > leafPartitions.length)
      return false;

    for (int x = minX; x <= maxX; x++) {
      int rowOffset = x * directory.sidePartitions;
      for (int y = minY; y <= maxY; y++) {
        for (int partitionId = directory.firstLeafByCell[rowOffset + y];
             partitionId >= 0;
             partitionId = directory.nextLeafByPartition[partitionId]) {
          if (leafPartitions[partitionId].envelope.intersectsEnvelopeClosed(mbr))
            matchedPartitions.add(partitionId);
        }
      }
    }
    if (matchedPartitions.size() > 1)
      matchedPartitions.sort();
    return true;
  }

  /** Returns a lazily built executor-local routing directory for the current immutable leaf array. */
  private LeafRoutingDirectory getOrBuildLeafRoutingDirectory() {
    LeafRoutingDirectory current = leafRoutingDirectory;
    if (current != null && current.sourceLeaves == leafPartitions)
      return current;
    synchronized (this) {
      current = leafRoutingDirectory;
      if (current == null || current.sourceLeaves != leafPartitions) {
        current = buildLeafRoutingDirectory();
        leafRoutingDirectory = current;
      }
      return current;
    }
  }

  /** Builds a compact dense directory when the initial 2-D grid is small enough to broadcast cheaply. */
  private LeafRoutingDirectory buildLeafRoutingDirectory() {
    if (leafPartitions == null || inputMBR == null || inputMBR.isEmpty() ||
        inputMBR.getCoordinateDimension() != 2 || initialHilbertOrder < 0 || initialHilbertOrder > 20) {
      return null;
    }
    int sidePartitions = initialHilbertOrder == 0 ? 1 : 1 << initialHilbertOrder;
    long cellCountLong = (long) sidePartitions * (long) sidePartitions;
    if (cellCountLong <= 0L || cellCountLong > 4L * 1024L * 1024L)
      return null;

    int[] firstLeafByCell = new int[(int) cellCountLong];
    int[] nextLeafByPartition = new int[leafPartitions.length];
    Arrays.fill(firstLeafByCell, -1);
    Arrays.fill(nextLeafByPartition, -1);
    for (int partitionId = 0; partitionId < leafPartitions.length; partitionId++) {
      EnvelopeNDLite envelope = leafPartitions[partitionId].envelope;
      if (envelope == null || envelope.getCoordinateDimension() != 2 || envelope.isEmpty())
        return null;
      int cellX = locateCell(envelope.getCenter(0), 0, sidePartitions);
      int cellY = locateCell(envelope.getCenter(1), 1, sidePartitions);
      if (!isContainedInInitialCell(envelope, cellX, cellY, sidePartitions))
        return null;
      int cellId = cellX * sidePartitions + cellY;
      nextLeafByPartition[partitionId] = firstLeafByCell[cellId];
      firstLeafByCell[cellId] = partitionId;
    }
    return new LeafRoutingDirectory(
        leafPartitions,
        sidePartitions,
        firstLeafByCell,
        nextLeafByPartition);
  }

  /** Verifies the invariant that recursive refinement never moves a leaf across its initial cell. */
  private boolean isContainedInInitialCell(EnvelopeNDLite leaf, int cellX, int cellY, int sidePartitions) {
    int[] coords = new int[]{cellX, cellY};
    EnvelopeNDLite initialCell = createCellEnvelope(coords, sidePartitions);
    for (int d = 0; d < 2; d++) {
      if (leaf.getMinCoord(d) < initialCell.getMinCoord(d) ||
          leaf.getMaxCoord(d) > initialCell.getMaxCoord(d)) {
        return false;
      }
    }
    return true;
  }

  private static boolean samePartitionIds(IntArray left, IntArray right) {
    if (left.size() != right.size())
      return false;
    for (int i = 0; i < left.size(); i++) {
      if (left.get(i) != right.get(i))
        return false;
    }
    return true;
  }

  /** Returns the configured object-to-partition routing implementation. */
  public String getPartitionRoutingMode() {
    return partitionRoutingMode;
  }

  /** Copies one partition envelope into the caller-provided MBR object. */
  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    if (partitionID < 0 || partitionID >= effectivePartitionCount)
      throw new IllegalArgumentException("Invalid partition ID: " + partitionID);
    mbr.set(leafPartitions[partitionID].envelope);
  }

  /** Returns the hierarchical code assigned to one final leaf partition. */
  public String getPartitionCode(int partitionId) {
    if (partitionCodes == null || partitionId < 0 || partitionId >= partitionCodes.length)
      throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
    return partitionCodes[partitionId];
  }

  @Override
  protected String getPartitionCodeForPartitionId(int partitionId) {
    return getPartitionCode(partitionId);
  }

  /** Returns a defensive copy of all final partition codes. */
  public String[] getPartitionCodes() {
    return partitionCodes == null ? new String[0] : Arrays.copyOf(partitionCodes, partitionCodes.length);
  }

  /** Returns the public read-only descriptor of one final leaf partition. */
  public PartitionInfo getPartitionInfo(int partitionId) {
    if (partitionId < 0 || partitionId >= effectivePartitionCount)
      throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
    PartitionLeaf leaf = leafPartitions[partitionId];
    return new PartitionInfo(
        partitionId,
        leaf.code,
        leaf.envelope,
        leaf.depth,
        leaf.splitAxis,
        leaf.estimatedBytes,
        leaf.featureCount);
  }

  /** Returns the public descriptors of all final leaf partitions. */
  public PartitionInfo[] getPartitionInfos() {
    PartitionInfo[] partitionInfos = new PartitionInfo[effectivePartitionCount];
    for (int i = 0; i < effectivePartitionCount; i++)
      partitionInfos[i] = getPartitionInfo(i);
    return partitionInfos;
  }

  /** Returns diagnostics from the latest exact-refinement statistics pass. */
  public ExactRefinementDiagnostics getLastExactRefinementDiagnostics() {
    return lastExactRefinementDiagnostics == null
        ? ExactRefinementDiagnostics.empty()
        : lastExactRefinementDiagnostics;
  }

  /**
   * Retains only the requested effective partitions and remaps them to a dense
   * zero-based partition id range.
   *
   * <p>This method is used after left/right feature assignment statistics are
   * available. At that point partitions that receive features from only one side
   * are pruned away, and only the final effective partition set is kept for later
   * job scheduling and binary feature loading.
   *
   * @param retainedPartitionIds partition ids to keep, in their desired new order
   * @return mapping from old partition id to new partition id, or {@code -1} when pruned
   */
  @Override
  public int[] retainPartitions(int[] retainedPartitionIds) {
    int oldPartitionCount = effectivePartitionCount;
    int[] oldToNew = new int[oldPartitionCount];
    Arrays.fill(oldToNew, -1);
    if (retainedPartitionIds == null) {
      this.leafPartitions = new PartitionLeaf[0];
      this.partitionCodes = new String[0];
      this.effectivePartitionCount = 0;
      return oldToNew;
    }
    PartitionLeaf[] retainedLeaves = new PartitionLeaf[retainedPartitionIds.length];
    String[] retainedCodes = new String[retainedPartitionIds.length];
    for (int newPartitionId = 0; newPartitionId < retainedPartitionIds.length; newPartitionId++) {
      int oldPartitionId = retainedPartitionIds[newPartitionId];
      if (oldPartitionId < 0 || oldPartitionId >= oldPartitionCount)
        throw new IllegalArgumentException("Invalid retained partition id: " + oldPartitionId);
      oldToNew[oldPartitionId] = newPartitionId;
      retainedLeaves[newPartitionId] = leafPartitions[oldPartitionId];
      retainedCodes[newPartitionId] = partitionCodes[oldPartitionId];
    }
    this.leafPartitions = retainedLeaves;
    this.partitionCodes = retainedCodes;
    this.effectivePartitionCount = retainedLeaves.length;
    return oldToNew;
  }

  /**
   * Feeds executor-aggregated raw-object statistics back into the current synopsis-built leaves.
   *
   * <p>The method never receives object-level descriptors. It updates each retained leaf with its
   * exact replicated load, prunes leaves that cannot produce cross-side candidates, and replaces
   * overloaded leaves with new geometric children. A later distributed statistics pass maps every
   * original object MBR to all intersecting children and supplies their exact loads.
   *
   * @return {@code true} when the partition layout changed and another exact statistics pass is
   *         required
   */
  public boolean refineFromExactPartitionStats(
      SpatialPartitioner.PartitionRefinementStats leftStats,
      SpatialPartitioner.PartitionRefinementStats rightStats) {
    return refineFromExactPartitionStats(leftStats, rightStats, Collections.emptySet());
  }

  /**
   * Refines the current leaves and separately diagnoses the supplied newly-created generation.
   * The generation codes normally identify children emitted by the preceding refinement pass.
   */
  public boolean refineFromExactPartitionStats(
      SpatialPartitioner.PartitionRefinementStats leftStats,
      SpatialPartitioner.PartitionRefinementStats rightStats,
      Set<String> observedGenerationCodes) {
    if (leafPartitions == null || leafPartitions.length == 0 || leftStats == null || rightStats == null) {
      lastExactRefinementDiagnostics = ExactRefinementDiagnostics.empty();
      return false;
    }
    long[] leftCounts = leftStats.getFeatureCounts();
    long[] rightCounts = rightStats.getFeatureCounts();
    long[] leftBytes = leftStats.getTotalBytes();
    long[] rightBytes = rightStats.getTotalBytes();
    EnvelopeNDLite[] leftAggregateEnvelopes = leftStats.getAggregateEnvelopes();
    EnvelopeNDLite[] rightAggregateEnvelopes = rightStats.getAggregateEnvelopes();
    int partitionCount = Math.min(
        leafPartitions.length,
            Math.min(
                Math.min(leftCounts.length, rightCounts.length),
                Math.min(leftBytes.length, rightBytes.length)));
    if (partitionCount <= 0) {
      lastExactRefinementDiagnostics = ExactRefinementDiagnostics.empty();
      return false;
    }

    List<PartitionLeaf> refinedLeaves = new ArrayList<>(partitionCount);
    Map<String, Long> nextParentBytesByCode = new HashMap<>();
    Map<String, Long> nextParentCountsByCode = new HashMap<>();
    boolean changed = partitionCount != leafPartitions.length;
    int emptyPartitionCount = 0;
    int nonEmptyPartitionCount = 0;
    int leftOnlyPartitionCount = 0;
    int rightOnlyPartitionCount = 0;
    int bothSidePartitionCount = 0;
    int bothSideAggregateMbrDisjointPartitionCount = 0;
    int effectiveBeforeRefinementPartitionCount = 0;
    int effectivePrunedPartitionCount = 0;
    int splitParentCount = 0;
    long nonEmptyPartitionLoadBytes = 0L;
    long leftOnlyPartitionLoadBytes = 0L;
    long rightOnlyPartitionLoadBytes = 0L;
    long bothSideAggregateMbrDisjointPartitionLoadBytes = 0L;
    long effectiveBeforeRefinementPartitionLoadBytes = 0L;
    int observedGenerationPartitionCount = 0;
    int observedGenerationNonEmptyPartitionCount = 0;
    int observedGenerationLeftOnlyPartitionCount = 0;
    int observedGenerationRightOnlyPartitionCount = 0;
    int observedGenerationBothSideAggregateMbrDisjointPartitionCount = 0;
    long observedGenerationPrunablePartitionLoadBytes = 0L;
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      PartitionLeaf current = leafPartitions[partitionId];
      boolean observedGeneration =
          observedGenerationCodes != null && observedGenerationCodes.contains(current.code);
      if (observedGeneration)
        observedGenerationPartitionCount++;
      long leftCount = leftCounts[partitionId];
      long rightCount = rightCounts[partitionId];
      long exactFeatureCount = safeAdd(leftCount, rightCount);
      long exactBytes = safeAdd(leftBytes[partitionId], rightBytes[partitionId]);
      if (leftCount <= 0L && rightCount <= 0L) {
        emptyPartitionCount++;
      } else {
        nonEmptyPartitionCount++;
        if (observedGeneration)
          observedGenerationNonEmptyPartitionCount++;
        nonEmptyPartitionLoadBytes = safeAdd(nonEmptyPartitionLoadBytes, exactBytes);
        if (leftCount > 0L && rightCount <= 0L) {
          leftOnlyPartitionCount++;
          leftOnlyPartitionLoadBytes = safeAdd(leftOnlyPartitionLoadBytes, exactBytes);
          if (observedGeneration) {
            observedGenerationLeftOnlyPartitionCount++;
            observedGenerationPrunablePartitionLoadBytes =
                safeAdd(observedGenerationPrunablePartitionLoadBytes, exactBytes);
          }
        } else if (leftCount <= 0L && rightCount > 0L) {
          rightOnlyPartitionCount++;
          rightOnlyPartitionLoadBytes = safeAdd(rightOnlyPartitionLoadBytes, exactBytes);
          if (observedGeneration) {
            observedGenerationRightOnlyPartitionCount++;
            observedGenerationPrunablePartitionLoadBytes =
                safeAdd(observedGenerationPrunablePartitionLoadBytes, exactBytes);
          }
        } else {
          bothSidePartitionCount++;
          if (projectedAggregateEnvelopesDisjoint(leftAggregateEnvelopes, rightAggregateEnvelopes, partitionId)) {
            bothSideAggregateMbrDisjointPartitionCount++;
            bothSideAggregateMbrDisjointPartitionLoadBytes =
                safeAdd(bothSideAggregateMbrDisjointPartitionLoadBytes, exactBytes);
            if (observedGeneration) {
              observedGenerationBothSideAggregateMbrDisjointPartitionCount++;
              observedGenerationPrunablePartitionLoadBytes =
                  safeAdd(observedGenerationPrunablePartitionLoadBytes, exactBytes);
            }
          } else {
            effectiveBeforeRefinementPartitionCount++;
            effectiveBeforeRefinementPartitionLoadBytes =
                safeAdd(effectiveBeforeRefinementPartitionLoadBytes, exactBytes);
          }
        }
      }
      boolean emptyPartition = exactFeatureCount <= 0L || exactBytes <= 0L;
      boolean ineffectivePartition =
          effectiveRefinementPruningEnabled
              && (leftCounts[partitionId] <= 0L
                  || rightCounts[partitionId] <= 0L
                  || projectedAggregateEnvelopesDisjoint(
                      leftAggregateEnvelopes,
                      rightAggregateEnvelopes,
                      partitionId));
      if (emptyPartition || ineffectivePartition) {
        if (!emptyPartition)
          effectivePrunedPartitionCount++;
        changed = true;
        continue;
      }

      PartitionLeaf exactLeaf = new PartitionLeaf(
          current.envelope,
          current.code,
          current.depth,
          current.splitAxis,
          exactBytes,
          safeFeatureCount(exactFeatureCount));
      if (!shouldRefine(exactLeaf)) {
        refinedLeaves.add(exactLeaf);
        continue;
      }
      if (exactCorrectionMadeNoProgress(current.code, exactBytes, exactFeatureCount)) {
        refinedLeaves.add(exactLeaf);
        // Keep the stop condition stable across later lightweight passes and the final
        // materialization verification. Dropping it here would let the same ineffective split
        // restart even though the child retained essentially all of its parent's load.
        nextParentBytesByCode.put(current.code, exactBytes);
        nextParentCountsByCode.put(current.code, exactFeatureCount);
        continue;
      }

      PartitionLeaf[] children = buildExactCorrectionChildren(
          exactLeaf,
          partitionId,
          leftStats,
          rightStats);
      if (children == null || children.length <= 1) {
        refinedLeaves.add(exactLeaf);
        continue;
      }
      performedSplitCount++;
      splitParentCount++;
      changed = true;
      for (PartitionLeaf child : children) {
        nextParentBytesByCode.put(child.code, exactBytes);
        nextParentCountsByCode.put(child.code, exactFeatureCount);
      }
      Collections.addAll(refinedLeaves, children);
    }
    exactCorrectionParentBytesByCode = nextParentBytesByCode;
    exactCorrectionParentCountsByCode = nextParentCountsByCode;
    lastExactRefinementDiagnostics = new ExactRefinementDiagnostics(
        partitionCount,
        emptyPartitionCount,
        nonEmptyPartitionCount,
        leftOnlyPartitionCount,
        rightOnlyPartitionCount,
        bothSidePartitionCount,
        bothSideAggregateMbrDisjointPartitionCount,
        effectiveBeforeRefinementPartitionCount,
        effectivePrunedPartitionCount,
        splitParentCount,
        refinedLeaves.size(),
        nonEmptyPartitionLoadBytes,
        leftOnlyPartitionLoadBytes,
        rightOnlyPartitionLoadBytes,
        bothSideAggregateMbrDisjointPartitionLoadBytes,
        effectiveBeforeRefinementPartitionLoadBytes,
        observedGenerationPartitionCount,
        observedGenerationNonEmptyPartitionCount,
        observedGenerationLeftOnlyPartitionCount,
        observedGenerationRightOnlyPartitionCount,
        observedGenerationBothSideAggregateMbrDisjointPartitionCount,
        observedGenerationPrunablePartitionLoadBytes,
        effectiveRefinementPruningEnabled,
        aggregateMbrRefinementPruningEnabled);
    if (!changed)
      return false;

    leafPartitions = refinedLeaves.toArray(new PartitionLeaf[0]);
    effectivePartitionCount = leafPartitions.length;
    partitionCodes = new String[effectivePartitionCount];
    for (int i = 0; i < effectivePartitionCount; i++) {
      partitionCodes[i] = leafPartitions[i].code;
      maxObservedRefinementDepth =
          Math.max(maxObservedRefinementDepth, leafPartitions[i].depth);
    }
    return true;
  }

  private PartitionLeaf[] buildExactCorrectionChildren(
      PartitionLeaf parent,
      int partitionId,
      SpatialPartitioner.PartitionRefinementStats leftStats,
      SpatialPartitioner.PartitionRefinementStats rightStats) {
    if (parent == null
        || parent.envelope == null
        || parent.envelope.isEmpty()
        || (maxRefinementDepth >= 0 && parent.depth >= maxRefinementDepth))
      return null;
    if (useEqualQuadtreeRefinement(parent.envelope))
      return buildExactQuadtreeChildren(parent);

    int splitAxis = usesObjectCenterOffsetAxisStrategy()
        ? chooseExactCorrectionAxis(parent.envelope, partitionId, leftStats, rightStats)
        : chooseSplitAxis(parent.envelope);
    long target = useCountDrivenRefinement()
        ? Math.max(1L, targetPartitionFeatureCount)
        : Math.max(1L, targetPartitionSizeBytes);
    long current = useCountDrivenRefinement()
        ? Math.max(1L, parent.featureCount)
        : Math.max(1L, parent.estimatedBytes);
    long desiredChildCount = Math.max(2L, (current + target - 1L) / target);
    int childCount = (int) Math.max(
        2L,
        Math.min(
            Math.min(8L, Math.max(2L, parent.featureCount)),
            desiredChildCount));
    long[] histogram = combineHistograms(
        useCountDrivenRefinement()
            ? leftStats.getCountHistogram(partitionId, splitAxis)
            : leftStats.getByteHistogram(partitionId, splitAxis),
        useCountDrivenRefinement()
            ? rightStats.getCountHistogram(partitionId, splitAxis)
            : rightStats.getByteHistogram(partitionId, splitAxis));
    double[] boundaries = quantileBoundaries(
        parent.envelope,
        splitAxis,
        histogram,
        childCount);
    if (boundaries == null)
      boundaries = equalWidthBoundaries(parent.envelope, splitAxis, childCount);
    if (boundaries == null || boundaries.length <= 2)
      return null;

    PartitionLeaf[] children = new PartitionLeaf[boundaries.length - 1];
    int width = codeWidth(children.length);
    for (int childOrder = 0; childOrder < children.length; childOrder++) {
      EnvelopeNDLite childEnvelope = new EnvelopeNDLite(parent.envelope);
      childEnvelope.setMinCoord(splitAxis, boundaries[childOrder]);
      childEnvelope.setMaxCoord(splitAxis, boundaries[childOrder + 1]);
      children[childOrder] = new PartitionLeaf(
          childEnvelope,
          composeChildCode(parent.code, childOrder, width),
          parent.depth + 1,
          splitAxis,
          0L,
          0);
    }
    return children;
  }

  private PartitionLeaf[] buildExactQuadtreeChildren(PartitionLeaf parent) {
    EnvelopeNDLite[] envelopes = createEqualQuadtreeChildEnvelopes(parent.envelope);
    if (envelopes == null)
      return null;
    PartitionLeaf[] children = new PartitionLeaf[envelopes.length];
    int width = codeWidth(children.length);
    for (int childOrder = 0; childOrder < children.length; childOrder++) {
      children[childOrder] = new PartitionLeaf(
          envelopes[childOrder],
          composeChildCode(parent.code, childOrder, width),
          parent.depth + 1,
          -1,
          0L,
          0);
    }
    return children;
  }

  private int chooseExactCorrectionAxis(
      EnvelopeNDLite envelope,
      int partitionId,
      SpatialPartitioner.PartitionRefinementStats leftStats,
      SpatialPartitioner.PartitionRefinementStats rightStats) {
    int fallbackAxis = chooseSplitAxis(envelope);
    int bestAxis = fallbackAxis;
    double bestScore = -1.0d;
    int axisCount = Math.min(2, envelope.getCoordinateDimension());
    for (int axis = 0; axis < axisCount; axis++) {
      double length = envelope.getSideLength(axis);
      if (!(length > 0.0d))
        continue;
      long[] histogram = combineHistograms(
          leftStats.getCountHistogram(partitionId, axis),
          rightStats.getCountHistogram(partitionId, axis));
      long total = 0L;
      double weightedSquaredOffset = 0.0d;
      for (int bin = 0; bin < histogram.length; bin++) {
        long count = Math.max(0L, histogram[bin]);
        if (count <= 0L)
          continue;
        double normalizedCenter = (bin + 0.5d) / histogram.length;
        total = safeAdd(total, count);
        weightedSquaredOffset += count * normalizedCenter * normalizedCenter;
      }
      double score = total <= 0L ? 0.0d : weightedSquaredOffset / total;
      if (score > bestScore
          || (Double.compare(score, bestScore) == 0 && axis == fallbackAxis)) {
        bestScore = score;
        bestAxis = axis;
      }
    }
    return bestAxis;
  }

  private long[] combineHistograms(long[] left, long[] right) {
    int length = Math.max(left == null ? 0 : left.length, right == null ? 0 : right.length);
    long[] combined = new long[length];
    for (int i = 0; i < length; i++) {
      long leftValue = left != null && i < left.length ? left[i] : 0L;
      long rightValue = right != null && i < right.length ? right[i] : 0L;
      combined[i] = safeAdd(leftValue, rightValue);
    }
    return combined;
  }

  private double[] quantileBoundaries(EnvelopeNDLite envelope,
                                      int splitAxis,
                                      long[] histogram,
                                      int childCount) {
    if (envelope == null
        || histogram == null
        || histogram.length == 0
        || childCount <= 1
        || !(envelope.getSideLength(splitAxis) > 0.0d))
      return null;
    long total = 0L;
    for (long value : histogram)
      total = safeAdd(total, Math.max(0L, value));
    if (total <= 0L)
      return null;
    double[] boundaries = new double[childCount + 1];
    boundaries[0] = envelope.getMinCoord(splitAxis);
    boundaries[childCount] = envelope.getMaxCoord(splitAxis);
    long cumulative = 0L;
    int nextChild = 1;
    for (int bin = 0; bin < histogram.length && nextChild < childCount; bin++) {
      cumulative = safeAdd(cumulative, Math.max(0L, histogram[bin]));
      while (nextChild < childCount
          && cumulative >= Math.ceil(total * nextChild / (double) childCount)) {
        double boundary = envelope.getMinCoord(splitAxis)
            + envelope.getSideLength(splitAxis) * (bin + 1.0d) / histogram.length;
        if (!(boundary > boundaries[nextChild - 1]
            && boundary < envelope.getMaxCoord(splitAxis)))
          return null;
        boundaries[nextChild++] = boundary;
      }
    }
    return nextChild == childCount ? boundaries : null;
  }

  private double[] equalWidthBoundaries(EnvelopeNDLite envelope,
                                        int splitAxis,
                                        int childCount) {
    if (envelope == null || childCount <= 1 || !(envelope.getSideLength(splitAxis) > 0.0d))
      return null;
    double[] boundaries = new double[childCount + 1];
    double min = envelope.getMinCoord(splitAxis);
    double length = envelope.getSideLength(splitAxis);
    for (int i = 0; i <= childCount; i++)
      boundaries[i] = i == childCount ? envelope.getMaxCoord(splitAxis) : min + length * i / childCount;
    return boundaries;
  }

  private boolean projectedAggregateEnvelopesDisjoint(
      EnvelopeNDLite[] leftEnvelopes,
      EnvelopeNDLite[] rightEnvelopes,
      int partitionId) {
    if (leftEnvelopes == null || rightEnvelopes == null || partitionId < 0)
      return false;
    EnvelopeNDLite left = partitionId < leftEnvelopes.length ? leftEnvelopes[partitionId] : null;
    EnvelopeNDLite right = partitionId < rightEnvelopes.length ? rightEnvelopes[partitionId] : null;
    return left != null
        && !left.isEmpty()
        && right != null
        && !right.isEmpty()
        && !left.intersectsEnvelopeClosed(right);
  }

  private boolean exactCorrectionMadeNoProgress(String partitionCode,
                                                long exactBytes,
                                                long exactFeatureCount) {
    if (partitionCode == null)
      return false;
    if (exactCorrectionParentBytesByCode == null)
      exactCorrectionParentBytesByCode = new HashMap<>();
    if (exactCorrectionParentCountsByCode == null)
      exactCorrectionParentCountsByCode = new HashMap<>();
    Long parentBytes = exactCorrectionParentBytesByCode.get(partitionCode);
    Long parentCount = exactCorrectionParentCountsByCode.get(partitionCode);
    if (parentBytes == null || parentCount == null || parentBytes <= 0L || parentCount <= 0L)
      return false;
    return exactFeatureCount >= parentCount
        && exactBytes >= Math.ceil(parentBytes * 0.98d);
  }

  private long safeAdd(long left, long right) {
    if (right > 0L && left > Long.MAX_VALUE - right)
      return Long.MAX_VALUE;
    if (right < 0L && left < Long.MIN_VALUE - right)
      return Long.MIN_VALUE;
    return left + right;
  }

  /** Exposes the configured byte target used by the load-aware refinement rule. */
  public long getTargetPartitionSizeBytes() {
    return targetPartitionSizeBytes;
  }

  /** Returns the refinement strategy label used by the latest construction. */
  public String getRefinementStrategy() {
    return refinementStrategy;
  }

  /** Returns the derived feature-count threshold used by count-based recursive refinement. */
  public long getTargetPartitionFeatureCount() {
    return targetPartitionFeatureCount;
  }

  /** Returns whether sibling coalescing was enabled for the latest construction. */
  public boolean isCoalesceSmallSiblingsEnabled() {
    return coalesceSmallSiblingsEnabled;
  }

  /** Returns how many merged sibling groups were materialized during the latest construction. */
  public int getCoalescedSiblingGroupCount() {
    return coalescedSiblingGroupCount;
  }

  /** Returns how many effective child partitions were removed by sibling coalescing. */
  public int getCoalescedSiblingPartitionReductionCount() {
    return coalescedSiblingPartitionReductionCount;
  }

  /** Returns how many recursive splits were actually performed during the latest construction. */
  public int getPerformedSplitCount() {
    return performedSplitCount;
  }

  /** Returns how many final leaves stopped because their load/count already satisfied the threshold. */
  public int getThresholdSatisfiedLeafCount() {
    return thresholdSatisfiedLeafCount;
  }

  /** Returns how many final leaves stopped because they hit the configured maximum refinement depth. */
  public int getSplitLimitLeafCount() {
    return splitLimitLeafCount;
  }

  /** Returns the deepest refinement level that was actually reached during the latest construction. */
  public int getMaxObservedRefinementDepth() {
    return maxObservedRefinementDepth;
  }

  /** Returns the initial Hilbert order `l` used to seed the first grid. */
  public int getInitialHilbertOrder() {
    return initialHilbertOrder;
  }

  /** Returns the number of cells in the initial Hilbert grid before refinement. */
  public long getInitialHilbertCellCount() {
    return initialHilbertCellCount;
  }

  /**
   * Generates child codes for a refined partition.
   *
   * <p>Children are first sorted by the chosen split axis, then assigned
   * parentCode + "." + orderCode according to their rank on that axis.
   */
  public String[] previewRefinedChildCodes(int parentPartitionId, EnvelopeNDLite[] childPartitions, int splitAxis) {
    if (childPartitions == null || childPartitions.length == 0)
      return new String[0];
    String parentCode = getPartitionCode(parentPartitionId);
    int[] orderedIndexes = orderChildrenBySplitAxis(childPartitions, splitAxis);
    String[] childCodes = new String[childPartitions.length];
    int width = codeWidth(childPartitions.length);
    for (int order = 0; order < orderedIndexes.length; order++) {
      int childIndex = orderedIndexes[order];
      childCodes[childIndex] = composeChildCode(parentCode, order, width);
    }
    return childCodes;
  }

  /** Orders refined child partitions along the split axis before hierarchical coding. */
  protected int[] orderChildrenBySplitAxis(EnvelopeNDLite[] childPartitions, int splitAxis) {
    Integer[] indexes = new Integer[childPartitions.length];
    for (int i = 0; i < childPartitions.length; i++)
      indexes[i] = i;
    Arrays.sort(indexes, Comparator
        .comparingDouble((Integer i) -> childPartitions[i].getMinCoord(splitAxis))
        .thenComparingDouble(i -> childPartitions[i].getMaxCoord(splitAxis))
        .thenComparingInt(i -> i));
    int[] ordered = new int[indexes.length];
    for (int i = 0; i < indexes.length; i++)
      ordered[i] = indexes[i];
    return ordered;
  }

  /** Composes one child code as `parentCode.ordinal`. */
  protected String composeChildCode(String parentCode, int order, int width) {
    return parentCode + "." + formatOrdinal(order, width);
  }

  /** Encodes one ordinal using zero padding so sibling codes stay lexicographically ordered. */
  private static String formatOrdinal(int ordinal, int width) {
    String format = "%0" + Math.max(1, width) + "d";
    return String.format(format, ordinal);
  }

  /** Computes the width needed to render sibling ordinals consistently. */
  private static int codeWidth(int partitionCount) {
    return Integer.toString(Math.max(0, partitionCount - 1)).length();
  }

  /** Serializes the compact partitioner state that executors need for assignment. */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(inputMBR, out);
    out.writeLong(targetPartitionSizeBytes);
    out.writeInt(initialHilbertOrder);
    out.writeInt(effectivePartitionCount);
    out.writeLong(initialHilbertCellCount);
    out.writeInt(refinementFanout);
    out.writeInt(maxRefinementDepth);
    out.writeBoolean(disjointPartitions);
    out.writeLong(configuredLeftSizeBytes);
    out.writeLong(configuredRightSizeBytes);
    out.writeLong(configuredLeftFeatureCount);
    out.writeLong(configuredRightFeatureCount);
    out.writeLong(targetPartitionFeatureCount);
    out.writeUTF(refinementStrategy == null ? SpatialJoinConstants.LAHilbert.DEFAULT_REFINE_STRATEGY : refinementStrategy);
    out.writeBoolean(coalesceSmallSiblingsEnabled);
    out.writeInt(coalescedSiblingGroupCount);
    out.writeInt(coalescedSiblingPartitionReductionCount);
    out.writeUTF(partitionRoutingMode == null
        ? SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_ROUTING_MODE
        : partitionRoutingMode);
    writeCoords(out, configuredGlobalMin);
    writeCoords(out, configuredGlobalMax);
    writeCoords(out, configuredLeftMin);
    writeCoords(out, configuredLeftMax);
    writeCoords(out, configuredRightMin);
    writeCoords(out, configuredRightMax);
    writeLeaves(out, leafPartitions);
  }

  /** Restores the compact partitioner state on executors or after deserialization. */
  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(inputMBR, in);
    this.targetPartitionSizeBytes = in.readLong();
    this.initialHilbertOrder = in.readInt();
    this.effectivePartitionCount = in.readInt();
    this.initialHilbertCellCount = in.readLong();
    this.refinementFanout = in.readInt();
    this.maxRefinementDepth = in.readInt();
    this.disjointPartitions = in.readBoolean();
    this.configuredLeftSizeBytes = in.readLong();
    this.configuredRightSizeBytes = in.readLong();
    this.configuredLeftFeatureCount = in.readLong();
    this.configuredRightFeatureCount = in.readLong();
    this.targetPartitionFeatureCount = in.readLong();
    this.refinementStrategy = normalizeRefinementStrategy(in.readUTF());
    this.coalesceSmallSiblingsEnabled = in.readBoolean();
    this.coalescedSiblingGroupCount = in.readInt();
    this.coalescedSiblingPartitionReductionCount = in.readInt();
    this.partitionRoutingMode = normalizePartitionRoutingMode(in.readUTF());
    this.configuredGlobalMin = readCoords(in);
    this.configuredGlobalMax = readCoords(in);
    this.configuredLeftMin = readCoords(in);
    this.configuredLeftMax = readCoords(in);
    this.configuredRightMin = readCoords(in);
    this.configuredRightMax = readCoords(in);
    this.leafPartitions = readLeaves(in);
    this.leafRoutingDirectory = null;
    this.partitionCodes = new String[leafPartitions.length];
    for (int i = 0; i < leafPartitions.length; i++)
      this.partitionCodes[i] = leafPartitions[i].code;
  }

  /** Writes one optional coordinate array to the externalized partitioner stream. */
  private static void writeCoords(ObjectOutput out, double[] coords) throws IOException {
    if (coords == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(coords.length);
    for (double coord : coords)
      out.writeDouble(coord);
  }

  /** Reads one optional coordinate array from the externalized partitioner stream. */
  private static double[] readCoords(ObjectInput in) throws IOException {
    int length = in.readInt();
    if (length < 0)
      return null;
    double[] coords = new double[length];
    for (int i = 0; i < length; i++)
      coords[i] = in.readDouble();
    return coords;
  }

  /** Writes the final LA-Hilbert leaves that executors need for partition lookup. */
  private static void writeLeaves(ObjectOutput out, PartitionLeaf[] leaves) throws IOException {
    if (leaves == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(leaves.length);
    for (PartitionLeaf leaf : leaves) {
      writeCoords(out, leaf.envelopeMin());
      writeCoords(out, leaf.envelopeMax());
      out.writeUTF(leaf.code == null ? "" : leaf.code);
      out.writeInt(leaf.depth);
      out.writeInt(leaf.splitAxis);
      out.writeLong(leaf.estimatedBytes);
      out.writeInt(leaf.featureCount);
    }
  }

  /** Restores the serialized final LA-Hilbert leaves from the externalized stream. */
  private static PartitionLeaf[] readLeaves(ObjectInput in) throws IOException {
    int length = in.readInt();
    if (length < 0)
      return null;
    PartitionLeaf[] leaves = new PartitionLeaf[length];
    for (int i = 0; i < length; i++) {
      double[] min = readCoords(in);
      double[] max = readCoords(in);
      EnvelopeNDLite envelope = new EnvelopeNDLite(min, max);
      String code = in.readUTF();
      int depth = in.readInt();
      int splitAxis = in.readInt();
      long estimatedBytes = in.readLong();
      int featureCount = in.readInt();
      leaves[i] = new PartitionLeaf(envelope, code, depth, splitAxis, estimatedBytes, featureCount);
    }
    return leaves;
  }

  /** Tests whether a query envelope should route to a leaf by its center point. */
  private boolean containsCenter(EnvelopeNDLite partition, EnvelopeNDLite query) {
    for (int d = 0; d < query.getCoordinateDimension(); d++) {
      double center = query.getCenter(d);
      if (center < partition.getMinCoord(d) || center > partition.getMaxCoord(d))
        return false;
      if (center == partition.getMaxCoord(d) && partition.getMaxCoord(d) != inputMBR.getMaxCoord(d))
        return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "LAHilbertPartitioner{" +
        "inputMBR=" + inputMBR +
        ", targetPartitionSizeBytes=" + targetPartitionSizeBytes +
        ", initialHilbertOrder=" + initialHilbertOrder +
        ", initialHilbertCellCount=" + initialHilbertCellCount +
        ", effectivePartitionCount=" + effectivePartitionCount +
        ", partitionCodes=" + Arrays.toString(partitionCodes) +
        ", refinementFanout=" + refinementFanout +
        ", maxRefinementDepth=" + maxRefinementDepth +
        ", leftSizeBytes=" + configuredLeftSizeBytes +
        ", rightSizeBytes=" + configuredRightSizeBytes +
        ", globalMin=" + Arrays.toString(configuredGlobalMin) +
        ", globalMax=" + Arrays.toString(configuredGlobalMax) +
        '}';
  }

  private static class CellKey {
    private final int[] coords;

    /** Captures the discrete Hilbert-grid cell coordinates of one non-empty initial cell. */
    private CellKey(int[] coords) {
      this.coords = Arrays.copyOf(coords, coords.length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CellKey cellKey = (CellKey) o;
      return Arrays.equals(coords, cellKey.coords);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(coords);
    }
  }

  private static class PartitionLeaf {
    protected final EnvelopeNDLite envelope;
    protected String code;
    protected final int depth;
    protected final int splitAxis;
    protected final long estimatedBytes;
    protected final int featureCount;

    /** Stores the immutable final state of one retained LA-Hilbert partition leaf. */
    private PartitionLeaf(EnvelopeNDLite envelope,
                          String code,
                          int depth,
                          int splitAxis,
                          long estimatedBytes,
                          int featureCount) {
      this.envelope = envelope == null || envelope.getCoordinateDimension() == 0
          ? new EnvelopeNDLite(0)
          : new EnvelopeNDLite(envelope);
      this.code = code;
      this.depth = depth;
      this.splitAxis = splitAxis;
      this.estimatedBytes = estimatedBytes;
      this.featureCount = featureCount;
    }

    /** Returns the serialized minimum coordinates of the leaf envelope. */
    private double[] envelopeMin() {
      double[] min = new double[envelope.getCoordinateDimension()];
      for (int d = 0; d < min.length; d++)
        min[d] = envelope.getMinCoord(d);
      return min;
    }

    /** Returns the serialized maximum coordinates of the leaf envelope. */
    private double[] envelopeMax() {
      double[] max = new double[envelope.getCoordinateDimension()];
      for (int d = 0; d < max.length; d++)
        max[d] = envelope.getMaxCoord(d);
      return max;
    }
  }

  /** Executor-local directory from initial grid cells to their retained final leaf partitions. */
  private static final class LeafRoutingDirectory {
    private final PartitionLeaf[] sourceLeaves;
    private final int sidePartitions;
    private final int[] firstLeafByCell;
    private final int[] nextLeafByPartition;

    private LeafRoutingDirectory(PartitionLeaf[] sourceLeaves,
                                 int sidePartitions,
                                 int[] firstLeafByCell,
                                 int[] nextLeafByPartition) {
      this.sourceLeaves = sourceLeaves;
      this.sidePartitions = sidePartitions;
      this.firstLeafByCell = firstLeafByCell;
      this.nextLeafByPartition = nextLeafByPartition;
    }
  }

  /**
   * Public read-only view of one constructed LA-Hilbert leaf partition.
   *
   * <p>The feature count reported here is the lightweight member count accumulated during
   * partition construction. In full mode it matches the number of source features assigned
   * to the leaf. In hierarchical mode it reflects the finest available member count reached
   * during refinement.
   */
  public static final class PartitionInfo {
    private final int partitionId;
    private final String code;
    private final EnvelopeNDLite envelope;
    private final int depth;
    private final int splitAxis;
    private final long estimatedBytes;
    private final int featureCount;

    private PartitionInfo(int partitionId,
                          String code,
                          EnvelopeNDLite envelope,
                          int depth,
                          int splitAxis,
                          long estimatedBytes,
                          int featureCount) {
      this.partitionId = partitionId;
      this.code = code;
      this.envelope = envelope == null ? new EnvelopeNDLite(0) : new EnvelopeNDLite(envelope);
      this.depth = depth;
      this.splitAxis = splitAxis;
      this.estimatedBytes = estimatedBytes;
      this.featureCount = featureCount;
    }

    /** Returns the dense final partition id after effective-partition pruning. */
    public int getPartitionId() {
      return partitionId;
    }

    /** Returns the hierarchical code assigned to this leaf. */
    public String getCode() {
      return code;
    }

    /** Returns a defensive copy of the leaf envelope. */
    public EnvelopeNDLite getEnvelope() {
      return new EnvelopeNDLite(envelope);
    }

    /** Returns the refinement depth of this leaf relative to the initial Hilbert grid. */
    public int getDepth() {
      return depth;
    }

    /** Returns the axis used by the parent split that produced this leaf. */
    public int getSplitAxis() {
      return splitAxis;
    }

    /** Returns the estimated load in bytes represented by this leaf. */
    public long getEstimatedBytes() {
      return estimatedBytes;
    }

    /** Returns the finest-level feature/member count represented by this leaf. */
    public int getFeatureCount() {
      return featureCount;
    }
  }

  /** Per-pass side/aggregate-MBR diagnostics for exact recursive refinement. */
  public static final class ExactRefinementDiagnostics implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final int candidatePartitionCount;
    private final int emptyPartitionCount;
    private final int nonEmptyPartitionCount;
    private final int leftOnlyPartitionCount;
    private final int rightOnlyPartitionCount;
    private final int bothSidePartitionCount;
    private final int bothSideAggregateMbrDisjointPartitionCount;
    private final int effectiveBeforeRefinementPartitionCount;
    private final int effectivePrunedPartitionCount;
    private final int splitParentCount;
    private final int outputPartitionCount;
    private final long nonEmptyPartitionLoadBytes;
    private final long leftOnlyPartitionLoadBytes;
    private final long rightOnlyPartitionLoadBytes;
    private final long bothSideAggregateMbrDisjointPartitionLoadBytes;
    private final long effectiveBeforeRefinementPartitionLoadBytes;
    private final int observedGenerationPartitionCount;
    private final int observedGenerationNonEmptyPartitionCount;
    private final int observedGenerationLeftOnlyPartitionCount;
    private final int observedGenerationRightOnlyPartitionCount;
    private final int observedGenerationBothSideAggregateMbrDisjointPartitionCount;
    private final long observedGenerationPrunablePartitionLoadBytes;
    private final boolean effectiveRefinementPruningEnabled;
    private final boolean aggregateMbrRefinementPruningEnabled;

    private ExactRefinementDiagnostics(
        int candidatePartitionCount,
        int emptyPartitionCount,
        int nonEmptyPartitionCount,
        int leftOnlyPartitionCount,
        int rightOnlyPartitionCount,
        int bothSidePartitionCount,
        int bothSideAggregateMbrDisjointPartitionCount,
        int effectiveBeforeRefinementPartitionCount,
        int effectivePrunedPartitionCount,
        int splitParentCount,
        int outputPartitionCount,
        long nonEmptyPartitionLoadBytes,
        long leftOnlyPartitionLoadBytes,
        long rightOnlyPartitionLoadBytes,
        long bothSideAggregateMbrDisjointPartitionLoadBytes,
        long effectiveBeforeRefinementPartitionLoadBytes,
        int observedGenerationPartitionCount,
        int observedGenerationNonEmptyPartitionCount,
        int observedGenerationLeftOnlyPartitionCount,
        int observedGenerationRightOnlyPartitionCount,
        int observedGenerationBothSideAggregateMbrDisjointPartitionCount,
        long observedGenerationPrunablePartitionLoadBytes,
        boolean effectiveRefinementPruningEnabled,
        boolean aggregateMbrRefinementPruningEnabled) {
      this.candidatePartitionCount = Math.max(0, candidatePartitionCount);
      this.emptyPartitionCount = Math.max(0, emptyPartitionCount);
      this.nonEmptyPartitionCount = Math.max(0, nonEmptyPartitionCount);
      this.leftOnlyPartitionCount = Math.max(0, leftOnlyPartitionCount);
      this.rightOnlyPartitionCount = Math.max(0, rightOnlyPartitionCount);
      this.bothSidePartitionCount = Math.max(0, bothSidePartitionCount);
      this.bothSideAggregateMbrDisjointPartitionCount =
          Math.max(0, bothSideAggregateMbrDisjointPartitionCount);
      this.effectiveBeforeRefinementPartitionCount =
          Math.max(0, effectiveBeforeRefinementPartitionCount);
      this.effectivePrunedPartitionCount = Math.max(0, effectivePrunedPartitionCount);
      this.splitParentCount = Math.max(0, splitParentCount);
      this.outputPartitionCount = Math.max(0, outputPartitionCount);
      this.nonEmptyPartitionLoadBytes = Math.max(0L, nonEmptyPartitionLoadBytes);
      this.leftOnlyPartitionLoadBytes = Math.max(0L, leftOnlyPartitionLoadBytes);
      this.rightOnlyPartitionLoadBytes = Math.max(0L, rightOnlyPartitionLoadBytes);
      this.bothSideAggregateMbrDisjointPartitionLoadBytes =
          Math.max(0L, bothSideAggregateMbrDisjointPartitionLoadBytes);
      this.effectiveBeforeRefinementPartitionLoadBytes =
          Math.max(0L, effectiveBeforeRefinementPartitionLoadBytes);
      this.observedGenerationPartitionCount = Math.max(0, observedGenerationPartitionCount);
      this.observedGenerationNonEmptyPartitionCount =
          Math.max(0, observedGenerationNonEmptyPartitionCount);
      this.observedGenerationLeftOnlyPartitionCount =
          Math.max(0, observedGenerationLeftOnlyPartitionCount);
      this.observedGenerationRightOnlyPartitionCount =
          Math.max(0, observedGenerationRightOnlyPartitionCount);
      this.observedGenerationBothSideAggregateMbrDisjointPartitionCount =
          Math.max(0, observedGenerationBothSideAggregateMbrDisjointPartitionCount);
      this.observedGenerationPrunablePartitionLoadBytes =
          Math.max(0L, observedGenerationPrunablePartitionLoadBytes);
      this.effectiveRefinementPruningEnabled = effectiveRefinementPruningEnabled;
      this.aggregateMbrRefinementPruningEnabled = aggregateMbrRefinementPruningEnabled;
    }

    private static ExactRefinementDiagnostics empty() {
      return new ExactRefinementDiagnostics(
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L,
          0, 0, 0, 0, 0, 0L, false, false);
    }

    public int getCandidatePartitionCount() {
      return candidatePartitionCount;
    }

    public int getEmptyPartitionCount() {
      return emptyPartitionCount;
    }

    public int getNonEmptyPartitionCount() {
      return nonEmptyPartitionCount;
    }

    public int getLeftOnlyPartitionCount() {
      return leftOnlyPartitionCount;
    }

    public int getRightOnlyPartitionCount() {
      return rightOnlyPartitionCount;
    }

    public int getBothSidePartitionCount() {
      return bothSidePartitionCount;
    }

    public int getBothSideAggregateMbrDisjointPartitionCount() {
      return bothSideAggregateMbrDisjointPartitionCount;
    }

    public int getEffectiveBeforeRefinementPartitionCount() {
      return effectiveBeforeRefinementPartitionCount;
    }

    public int getEffectivePrunedPartitionCount() {
      return effectivePrunedPartitionCount;
    }

    public int getSplitParentCount() {
      return splitParentCount;
    }

    public int getOutputPartitionCount() {
      return outputPartitionCount;
    }

    public long getNonEmptyPartitionLoadBytes() {
      return nonEmptyPartitionLoadBytes;
    }

    public long getLeftOnlyPartitionLoadBytes() {
      return leftOnlyPartitionLoadBytes;
    }

    public long getRightOnlyPartitionLoadBytes() {
      return rightOnlyPartitionLoadBytes;
    }

    public long getBothSideAggregateMbrDisjointPartitionLoadBytes() {
      return bothSideAggregateMbrDisjointPartitionLoadBytes;
    }

    public long getEffectiveBeforeRefinementPartitionLoadBytes() {
      return effectiveBeforeRefinementPartitionLoadBytes;
    }

    public long getPrunablePartitionLoadBytes() {
      return saturatedAdd(
          saturatedAdd(leftOnlyPartitionLoadBytes, rightOnlyPartitionLoadBytes),
          bothSideAggregateMbrDisjointPartitionLoadBytes);
    }

    public int getObservedGenerationPartitionCount() {
      return observedGenerationPartitionCount;
    }

    public int getObservedGenerationNonEmptyPartitionCount() {
      return observedGenerationNonEmptyPartitionCount;
    }

    public int getObservedGenerationLeftOnlyPartitionCount() {
      return observedGenerationLeftOnlyPartitionCount;
    }

    public int getObservedGenerationRightOnlyPartitionCount() {
      return observedGenerationRightOnlyPartitionCount;
    }

    public int getObservedGenerationBothSideAggregateMbrDisjointPartitionCount() {
      return observedGenerationBothSideAggregateMbrDisjointPartitionCount;
    }

    public int getObservedGenerationPrunablePartitionCount() {
      return observedGenerationLeftOnlyPartitionCount
          + observedGenerationRightOnlyPartitionCount
          + observedGenerationBothSideAggregateMbrDisjointPartitionCount;
    }

    public long getObservedGenerationPrunablePartitionLoadBytes() {
      return observedGenerationPrunablePartitionLoadBytes;
    }

    private static long saturatedAdd(long left, long right) {
      if (right > 0L && left > Long.MAX_VALUE - right)
        return Long.MAX_VALUE;
      if (right < 0L && left < Long.MIN_VALUE - right)
        return Long.MIN_VALUE;
      return left + right;
    }

    public boolean isEffectiveRefinementPruningEnabled() {
      return effectiveRefinementPruningEnabled;
    }

    public boolean isAggregateMbrRefinementPruningEnabled() {
      return aggregateMbrRefinementPruningEnabled;
    }
  }

  private static class WorkingPartition extends PartitionLeaf {
    private final int[] featureIndexes;
    private final int levelIndex;
    private final int[] leftFeatureIndexes;
    private final int[] rightFeatureIndexes;

    /** Stores the mutable state of a partition while recursive refinement is still in progress. */
    private WorkingPartition(EnvelopeNDLite envelope,
                             String code,
                             int depth,
                             int splitAxis,
                             long estimatedBytes,
                             int featureCount,
                             int levelIndex,
                             int[] featureIndexes) {
      this(
          envelope,
          code,
          depth,
          splitAxis,
          estimatedBytes,
          featureCount,
          levelIndex,
          featureIndexes,
          null,
          null);
    }

    private WorkingPartition(EnvelopeNDLite envelope,
                             String code,
                             int depth,
                             int splitAxis,
                             long estimatedBytes,
                             int featureCount,
                             int levelIndex,
                             int[] featureIndexes,
                             int[] leftFeatureIndexes,
                             int[] rightFeatureIndexes) {
      super(envelope, code, depth, splitAxis, estimatedBytes, featureCount);
      this.levelIndex = levelIndex;
      this.featureIndexes = featureIndexes;
      this.leftFeatureIndexes = leftFeatureIndexes;
      this.rightFeatureIndexes = rightFeatureIndexes;
    }

    private boolean hasSideAssignments() {
      return leftFeatureIndexes != null && rightFeatureIndexes != null;
    }

    private WorkingPartition withSideAssignments(int[] leftIndexes, int[] rightIndexes) {
      return new WorkingPartition(
          envelope,
          code,
          depth,
          splitAxis,
          estimatedBytes,
          featureCount,
          levelIndex,
          featureIndexes,
          leftIndexes == null ? new int[0] : leftIndexes,
          rightIndexes == null ? new int[0] : rightIndexes);
    }

    /** Freezes the working partition into the immutable final leaf representation. */
    private PartitionLeaf toLeaf() {
      return new PartitionLeaf(envelope, code, depth, splitAxis, estimatedBytes, featureCount);
    }
  }

  /** Mutable partition state used by the join-oriented effective construction path. */
  private static class AssignedPartition extends PartitionLeaf {
    private final int[] leftFeatureIndexes;
    private final int[] rightFeatureIndexes;

    private AssignedPartition(EnvelopeNDLite envelope,
                              String code,
                              int depth,
                              int splitAxis,
                              long estimatedBytes,
                              int featureCount,
                              int[] leftFeatureIndexes,
                              int[] rightFeatureIndexes) {
      super(envelope, code, depth, splitAxis, estimatedBytes, featureCount);
      this.leftFeatureIndexes = leftFeatureIndexes == null ? new int[0] : leftFeatureIndexes;
      this.rightFeatureIndexes = rightFeatureIndexes == null ? new int[0] : rightFeatureIndexes;
    }

    private boolean isEffective() {
      return leftFeatureIndexes.length > 0 && rightFeatureIndexes.length > 0;
    }

    private PartitionLeaf toLeaf() {
      return new PartitionLeaf(envelope, code, depth, splitAxis, estimatedBytes, featureCount);
    }
  }

  /** One effective child together with its original ordinal inside the parent split. */
  private static class EffectiveChildCandidate {
    private final int childOrder;
    private final AssignedPartition partition;

    private EffectiveChildCandidate(int childOrder, AssignedPartition partition) {
      this.childOrder = childOrder;
      this.partition = partition;
    }
  }

  /** Mutable aggregation state while greedily coalescing contiguous effective siblings. */
  private static class CoalescedAssignedPartitionState {
    private final EnvelopeNDLite envelope;
    private final int[] leftFeatureIndexes;
    private final int[] rightFeatureIndexes;
    private final int childCount;

    private CoalescedAssignedPartitionState(EnvelopeNDLite envelope,
                                            int[] leftFeatureIndexes,
                                            int[] rightFeatureIndexes,
                                            int childCount) {
      this.envelope = envelope == null ? new EnvelopeNDLite(0) : new EnvelopeNDLite(envelope);
      this.leftFeatureIndexes = leftFeatureIndexes == null ? new int[0] : Arrays.copyOf(leftFeatureIndexes, leftFeatureIndexes.length);
      this.rightFeatureIndexes = rightFeatureIndexes == null ? new int[0] : Arrays.copyOf(rightFeatureIndexes, rightFeatureIndexes.length);
      this.childCount = Math.max(1, childCount);
    }
  }

  /** Collects the left/right feature indexes assigned to one initial Hilbert cell. */
  private static class CellAssignments {
    private final IntArray leftIndexes = new IntArray();
    private final IntArray rightIndexes = new IntArray();

    private void add(int featureIndex, boolean leftSide) {
      if (leftSide)
        leftIndexes.add(featureIndex);
      else
        rightIndexes.add(featureIndex);
    }

    private boolean isEffective() {
      return leftIndexes.size() > 0 && rightIndexes.size() > 0;
    }

    private long totalBytes(AbstractFeature[] leftFeatures, AbstractFeature[] rightFeatures) {
      return sumBytes(leftFeatures, leftIndexes) + sumBytes(rightFeatures, rightIndexes);
    }

    private int totalMembers(AbstractFeature[] leftFeatures, AbstractFeature[] rightFeatures) {
      long totalMembers = sumMembers(leftFeatures, leftIndexes) + sumMembers(rightFeatures, rightIndexes);
      return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, totalMembers));
    }

    private static long sumBytes(AbstractFeature[] features, IntArray indexes) {
      long totalBytes = 0L;
      for (int i = 0; i < indexes.size(); i++) {
        AbstractFeature feature = features[indexes.get(i)];
        if (feature != null)
          totalBytes += Math.max(0L, feature.getLoadWeight());
      }
      return Math.max(1L, totalBytes);
    }

    private static long sumMembers(AbstractFeature[] features, IntArray indexes) {
      long totalMembers = 0L;
      for (int i = 0; i < indexes.size(); i++) {
        AbstractFeature feature = features[indexes.get(i)];
        if (feature != null)
          totalMembers += Math.max(1L, feature.getMemberCount());
      }
      return totalMembers;
    }

  }

  /** One ordered left/right feature reference used by step-length-based refinement. */
  private static class OrderedFeatureRef {
    private final boolean leftSide;
    private final int featureIndex;
    private final double representativeCoord;

    private OrderedFeatureRef(boolean leftSide, int featureIndex, double representativeCoord) {
      this.leftSide = leftSide;
      this.featureIndex = featureIndex;
      this.representativeCoord = representativeCoord;
    }
  }

  /** Mutable weighted online-variance state used only while choosing one split axis. */
  private static class AxisDistributionStats {
    private double totalWeight;
    private double mean;
    private double weightedSquaredDeviation;

    private void merge(long count, double otherMean, double otherM2) {
      double otherWeight = Math.max(1L, count);
      if (!(totalWeight > 0.0d)) {
        totalWeight = otherWeight;
        mean = otherMean;
        weightedSquaredDeviation = Math.max(0.0d, otherM2);
        return;
      }
      double mergedWeight = totalWeight + otherWeight;
      double delta = otherMean - mean;
      weightedSquaredDeviation +=
          Math.max(0.0d, otherM2) +
              delta * delta * totalWeight * otherWeight / mergedWeight;
      mean += delta * otherWeight / mergedWeight;
      totalWeight = mergedWeight;
    }

    private double sumSquaredOffset(double origin) {
      if (!(totalWeight > 0.0d))
        return 0.0d;
      double meanOffset = mean - origin;
      return Math.max(
          0.0d,
          weightedSquaredDeviation + totalWeight * meanOffset * meanOffset);
    }
  }
}
