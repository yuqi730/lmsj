/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.whu.spatialJoin.core.partitioning;

import cn.edu.whu.spatialJoin.common.Summary;
import cn.edu.whu.spatialJoin.geom.EnvelopeND;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.IFeature;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeature;
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeatureHierarchy;
import cn.edu.whu.spatialJoin.cost.PartProfile;
import cn.edu.whu.spatialJoin.synopses.AbstractHistogram;
import cn.edu.whu.spatialJoin.utils.IntArray;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Externalizable;
import java.io.PrintStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base abstraction for all spatial partitioners used in the project.
 *
 * <p>The class defines three cooperating responsibilities:
 * 1. construct partitions from summaries or lightweight features
 * 2. assign/query partitions for envelopes, geometries, and features
 * 3. support the later effective-partition and binary-reload workflow used by
 *    partition-aware spatial join execution
 */
public abstract class SpatialPartitioner extends Partitioner implements Externalizable, Iterable<EnvelopeNDLite> {
  protected Log log = LogFactory.getLog(SpatialPartitioner.class);

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Metadata {
    /**
     * Whether this global indexer supports disjoint partitions or not
     * @return {@code true} if disjoint partitioning is supported for this partitioner
     */
    boolean disjointSupported();

    /**
     * The extension used with the global index. Also used for choosing this partitioner from command line.
     * @return the extension including the dot
     */
    String extension();

    /**
     * A short description for this partitioner
     * @return the description of this partitioner
     */
    String description();
  }

  /**
   * Annotates the required parameters of the {@link #construct(Summary, double[][], AbstractHistogram, int)}
   * function in implementation of this interface.
   */
  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Required {}

  /**
   * Annotates the parameters of the {@link #construct(Summary, double[][], AbstractHistogram, int)}
   * function that can be used but are not required.
   */
  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Preferred {}

  /**
   * Initializes the partitioner before it is first constructed.
   * @param conf the environment configuration
   * @param disjoint set to true to produce disjoint partitions with probable replication of some features
   */
  /** Allows implementations to parse options before construction begins. */
  public void setup(SpatialJoinOptions conf, boolean disjoint) {}


  /**
   * Legacy point-array construction hook kept for compatibility with mature partitioners.
   *
   * <p>In the current project the preferred construction input is
   * {@link AbstractFeature[]}, i.e. one lightweight descriptor per feature from the
   * full dataset rather than a sampled subset. This point-array overload remains for
   * older implementations that internally partition a representative point set.
   * @param summary the summary of the input
   * @param points representative points arranged by dimension
   * @param histogram a histogram of the input
   * @param numPartitions the desired number of partitions. This is treated as a loose hint and not a strict value
   */
  public abstract void construct(Summary summary, double[][] points, AbstractHistogram histogram, int numPartitions);

  /**
   * Preferred partition-construction entry that consumes one lightweight descriptor per input feature.
   *
   * <p>The default implementation preserves compatibility with legacy point-based
   * partitioners by converting each feature descriptor to one representative point
   * and delegating to
   * {@link #construct(Summary, double[][], AbstractHistogram, int)}. Partitioners that need richer
   * per-feature load information, such as load-aware refinement, should override this method directly.
   *
   * @param summary summary of the full dataset
   * @param features one lightweight descriptor per input feature
   * @param histogram optional histogram
   * @param numPartitions requested number of partitions
   */
  public void construct(Summary summary, AbstractFeature[] features, AbstractHistogram histogram, int numPartitions) {
    construct(summary, toFeaturePoints(features), histogram, numPartitions);
  }

  /**
   * Preferred multi-level construction entry for hierarchical distributed aggregation.
   *
   * <p>The default implementation simply uses the finest level exposed by the hierarchy.
   * Partitioners that can exploit the hierarchy directly may override this method.
   *
   * @param summary summary of the full dataset
   * @param hierarchy multi-level hierarchy of aggregated abstract features
   * @param histogram optional histogram
   * @param numPartitions requested number of partitions
   */
  public void construct(Summary summary, AbstractFeatureHierarchy hierarchy, AbstractHistogram histogram, int numPartitions) {
    construct(summary, hierarchy == null ? null : hierarchy.getFinestLevel(), histogram, numPartitions);
  }

  /**
   * Preferred join-oriented construction entry that knows the left/right lightweight inputs separately.
   *
   * <p>The default implementation preserves backwards compatibility by first constructing
   * the partitioner from the union of both datasets and then deriving the effective
   * partition view from the resulting partition boundaries. Partitioners that need to
   * push the effective-partition decision into the construction phase itself may
   * override this method.
   *
   * @param summary summary of the combined input domain
   * @param leftFeatures left-side lightweight descriptors
   * @param rightFeatures right-side lightweight descriptors
   * @param histogram optional histogram
   * @param numPartitions requested number of partitions
   * @return effective partitioning result after left/right mapping
   */
  public EffectivePlan constructEffective(Summary summary,
                                          AbstractFeature[] leftFeatures,
                                          AbstractFeature[] rightFeatures,
                                          AbstractHistogram histogram,
                                          int numPartitions) {
    construct(summary, concatFeatures(leftFeatures, rightFeatures), histogram, numPartitions);
    return buildEffectivePlan(leftFeatures, rightFeatures);
  }

  /**
   * Converts one lightweight feature descriptor per record into one representative point per feature.
   *
   * Point-driven partitioners such as HCurve and STR reuse this helper so they can
   * still be constructed from the full abstract-feature set without reintroducing
   * a separate sampling stage.
   */
  protected double[][] toFeaturePoints(AbstractFeature[] features) {
    if (features == null || features.length == 0)
      return null;
    int coordinateDimension = 0;
    for (AbstractFeature feature : features) {
      if (feature != null && feature.getCoordinateDimension() > 0) {
        coordinateDimension = feature.getCoordinateDimension();
        break;
      }
    }
    if (coordinateDimension == 0)
      return null;
    double[][] points = new double[coordinateDimension][features.length];
    for (int i = 0; i < features.length; i++) {
      AbstractFeature feature = features[i];
      for (int d = 0; d < coordinateDimension; d++) {
        points[d][i] = feature == null || feature.getCoordinateDimension() <= d ? 0.0d : feature.getCenter(d);
      }
    }
    return points;
  }

  /** Concatenates left/right lightweight feature arrays for partitioner construction. */
  protected static AbstractFeature[] concatFeatures(AbstractFeature[] leftFeatures, AbstractFeature[] rightFeatures) {
    int leftLength = leftFeatures == null ? 0 : leftFeatures.length;
    int rightLength = rightFeatures == null ? 0 : rightFeatures.length;
    AbstractFeature[] combined = new AbstractFeature[leftLength + rightLength];
    if (leftLength > 0)
      System.arraycopy(leftFeatures, 0, combined, 0, leftLength);
    if (rightLength > 0)
      System.arraycopy(rightFeatures, 0, combined, leftLength, rightLength);
    return combined;
  }

  /**
   * Retains only the requested effective partitions and returns the old-to-new partition id mapping.
   *
   * <p>Most partitioners do not need to physically prune internal state, so the base implementation
   * simply returns a dense remapping. Specialized partitioners such as LAHilbert may override this
   * method to shrink their internal leaf set as well.
   *
   * @param retainedPartitionIds partition ids that remain effective
   * @return mapping from old partition ids to new dense ids, or {@code -1} for pruned partitions
   */
  public int[] retainPartitions(int[] retainedPartitionIds) {
    int[] oldToNew = new int[Math.max(0, numPartitions())];
    Arrays.fill(oldToNew, -1);
    if (retainedPartitionIds == null)
      return oldToNew;
    for (int newPartitionId = 0; newPartitionId < retainedPartitionIds.length; newPartitionId++) {
      int oldPartitionId = retainedPartitionIds[newPartitionId];
      if (oldPartitionId < 0 || oldPartitionId >= oldToNew.length)
        throw new IllegalArgumentException("Invalid retained partition id: " + oldPartitionId);
      oldToNew[oldPartitionId] = newPartitionId;
    }
    return oldToNew;
  }

  /**
   * Builds the final effective partition view after left/right feature mapping statistics are available.
   *
   * The method keeps only partitions that receive features from both sides and stores
   * the compact OID-to-partition routing tables needed to reload binary features later.
   *
   * <p>Feature-to-partition mapping here follows the current join design: one lightweight
   * feature is assigned to every partition whose envelope intersects the feature MBR.
   * This preserves the replication semantics needed later by reference-point-based
   * duplicate filtering during the actual join computation.
   */
  public EffectivePlan buildEffectivePlan(AbstractFeature[] leftFeatures, AbstractFeature[] rightFeatures) {
    int originalPartitionCount = Math.max(0, numPartitions());
    long[] leftCounts = new long[originalPartitionCount];
    long[] rightCounts = new long[originalPartitionCount];
    long[] leftBytes = new long[originalPartitionCount];
    long[] rightBytes = new long[originalPartitionCount];
    double[] leftComplexities = new double[originalPartitionCount];
    double[] rightComplexities = new double[originalPartitionCount];
    long[] leftVertexCounts = new long[originalPartitionCount];
    long[] rightVertexCounts = new long[originalPartitionCount];
    long[] leftMinVertexCounts = new long[originalPartitionCount];
    long[] rightMinVertexCounts = new long[originalPartitionCount];
    long[] leftMaxVertexCounts = new long[originalPartitionCount];
    long[] rightMaxVertexCounts = new long[originalPartitionCount];
    CibHistogramStats[] leftCibStats = createCibStatsArray(originalPartitionCount);
    CibHistogramStats[] rightCibStats = createCibStatsArray(originalPartitionCount);
    EnvelopeNDLite[] leftAggregateEnvelopes = new EnvelopeNDLite[originalPartitionCount];
    EnvelopeNDLite[] rightAggregateEnvelopes = new EnvelopeNDLite[originalPartitionCount];
    List<FeatureRef> leftAssignments = assign(
        leftFeatures,
        leftCounts,
        leftBytes,
        leftComplexities,
        leftVertexCounts,
        leftMinVertexCounts,
        leftMaxVertexCounts,
        leftCibStats,
        leftAggregateEnvelopes);
    List<FeatureRef> rightAssignments = assign(
        rightFeatures,
        rightCounts,
        rightBytes,
        rightComplexities,
        rightVertexCounts,
        rightMinVertexCounts,
        rightMaxVertexCounts,
        rightCibStats,
        rightAggregateEnvelopes);
    int[] traditionalEffectivePartitionIds = collectEffectivePartitionIds(
        leftCounts,
        rightCounts,
        leftAggregateEnvelopes,
        rightAggregateEnvelopes,
        false);
    int nonEmptyCandidatePartitionCount = countNonEmptyPartitions(leftCounts, rightCounts);
    long nonEmptyCandidateLoadBytes = sumNonEmptyCandidateLoadBytes(leftCounts, rightCounts, leftBytes, rightBytes);
    int[] effectivePartitionIds = collectEffectivePartitionIds(
        leftCounts,
        rightCounts,
        leftAggregateEnvelopes,
        rightAggregateEnvelopes,
        true);
    int[] oldToNewPartitionIds = retainPartitions(effectivePartitionIds);
    SpatialPartitioner effectivePartitioner = this;
    if (numPartitions() != effectivePartitionIds.length) {
      effectivePartitioner = buildExplicitEffectivePartitioner(effectivePartitionIds);
    }
    long[] effectiveLeftCounts = remapLoads(leftCounts, oldToNewPartitionIds);
    long[] effectiveRightCounts = remapLoads(rightCounts, oldToNewPartitionIds);
    long[] effectiveLeftBytes = remapLoads(leftBytes, oldToNewPartitionIds);
    long[] effectiveRightBytes = remapLoads(rightBytes, oldToNewPartitionIds);
    double[] effectiveLeftComplexities = remapLoads(leftComplexities, oldToNewPartitionIds);
    double[] effectiveRightComplexities = remapLoads(rightComplexities, oldToNewPartitionIds);
    long[] effectiveLeftVertexCounts = remapLoads(leftVertexCounts, oldToNewPartitionIds);
    long[] effectiveRightVertexCounts = remapLoads(rightVertexCounts, oldToNewPartitionIds);
    long[] effectiveLeftMinVertexCounts = remapLoads(leftMinVertexCounts, oldToNewPartitionIds);
    long[] effectiveRightMinVertexCounts = remapLoads(rightMinVertexCounts, oldToNewPartitionIds);
    long[] effectiveLeftMaxVertexCounts = remapLoads(leftMaxVertexCounts, oldToNewPartitionIds);
    long[] effectiveRightMaxVertexCounts = remapLoads(rightMaxVertexCounts, oldToNewPartitionIds);
    long[] effectiveCandidatePairCounts = remapLoads(
        estimateCibCandidatePairCounts(leftCibStats, rightCibStats, leftCounts, rightCounts),
        oldToNewPartitionIds);
    return new EffectivePlan(
        effectivePartitioner,
        PartProfile.fromPartitioner(effectivePartitioner, effectiveLeftCounts, effectiveRightCounts, effectiveLeftBytes, effectiveRightBytes,
            effectiveLeftComplexities, effectiveRightComplexities,
            effectiveLeftVertexCounts, effectiveRightVertexCounts,
            effectiveLeftMinVertexCounts, effectiveRightMinVertexCounts,
            effectiveLeftMaxVertexCounts, effectiveRightMaxVertexCounts,
            effectiveCandidatePairCounts),
        remapAssignments(leftAssignments, oldToNewPartitionIds),
        remapAssignments(rightAssignments, oldToNewPartitionIds),
        originalPartitionCount,
        nonEmptyCandidatePartitionCount,
        nonEmptyCandidateLoadBytes,
        traditionalEffectivePartitionIds.length,
        Math.max(0, traditionalEffectivePartitionIds.length - effectivePartitionIds.length));
  }

  /**
   * Builds a dense explicit partition view for retained partitions when the concrete partitioner
   * cannot natively renumber the retained subset into {@code [0, n)} partition ids.
   *
   * <p>Grid/STR/KD style partitioners expose a fixed global layout. Once only the effective
   * partitions remain, the later execution pipeline still expects dense integer ids. This helper
   * materializes one compact execution-only wrapper that preserves the retained envelopes and
   * stable human-readable codes while satisfying that dense-id contract.
   */
  protected SpatialPartitioner buildExplicitEffectivePartitioner(int[] retainedPartitionIds) {
    EnvelopeNDLite[] retainedEnvelopes = new EnvelopeNDLite[retainedPartitionIds == null ? 0 : retainedPartitionIds.length];
    String[] retainedCodes = new String[retainedEnvelopes.length];
    for (int newPartitionId = 0; newPartitionId < retainedEnvelopes.length; newPartitionId++) {
      int oldPartitionId = retainedPartitionIds[newPartitionId];
      retainedEnvelopes[newPartitionId] = getPartitionMBR(oldPartitionId);
      retainedCodes[newPartitionId] = getPartitionCodeForPartitionId(oldPartitionId);
    }
    return new ExplicitPartitionListPartitioner(getEnvelope(), retainedEnvelopes, retainedCodes, isDisjoint());
  }

  /**
   * Returns the stable code to use when one retained partition is exposed through a dense
   * execution-only wrapper.
   *
   * <p>Most partitioners use the original numeric id as their stable code. LAHilbert overrides
   * this hook to preserve its hierarchical partition code strings.
   */
  protected String getPartitionCodeForPartitionId(int partitionId) {
    return Integer.toString(partitionId);
  }

  /**
   * Collects one [[AbstractFeature]] per input record while preserving the Spark-assigned stable OID.
   *
   * This is the bridge from the binary feature RDD into the lightweight driver-side
   * structures used by partition construction and effective-partition pruning.
   */
  public static AbstractFeature[] collectAbstractFeatures(JavaRDD<IFeature> features) {
    return collectAbstractFeatures(features, null);
  }

  /**
   * Collects one [[AbstractFeature]] per input record while preserving the Spark-assigned stable OID
   * and using the requested complexity-estimation mode.
   *
   * <p>This is the main bridge from the feature RDD into the driver-side lightweight partitioning
   * structures. Each input feature is converted into one {@link AbstractFeature} carrying only:
   * <ul>
   *   <li>its stable OID,</li>
   *   <li>its envelope and other lightweight geometry metadata,</li>
   *   <li>its memory-load estimate, and</li>
   *   <li>its computation-complexity estimate.</li>
   * </ul>
   *
   * <p>The original full {@link IFeature} objects are not retained here; callers use the returned array
   * only for partition construction and keep the full binary features elsewhere for later reload.
   *
   * @param features the input feature RDD to summarize.
   * @param complexityMode the configured complexity-estimation mode, or {@code null} for the default mode.
   * @return one driver-side array containing one {@link AbstractFeature} per input record.
   */
  public static AbstractFeature[] collectAbstractFeatures(JavaRDD<IFeature> features, String complexityMode) {
    return collectAbstractFeatures(
        features,
        complexityMode,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  public static AbstractFeature[] collectAbstractFeatures(JavaRDD<IFeature> features,
                                                          String complexityMode,
                                                          String loadEstimateMode) {
    if (features == null)
      return new AbstractFeature[0];
    List<AbstractFeature> collected = features.zipWithUniqueId().map(tuple ->
        AbstractFeature.fromFeatureBinary(
            tuple._1,
            tuple._2,
            tuple._1 == null ? 0L : tuple._1.getStorageSize(),
            resolveFeatureLoadBytes(tuple._1, loadEstimateMode),
            complexityMode)
    ).collect();
    return collected.toArray(new AbstractFeature[0]);
  }

  /**
   * Collects one [[AbstractFeature]] per input record from an already materialized OID-to-feature RDD.
   *
   * This overload avoids recomputing {@code zipWithUniqueId()} when the caller has already
   * persisted the stable OID mapping for later partition-aware execution.
   *
   * @param featuresByOid a cached {@code (oid, feature)} RDD prepared earlier in the pipeline.
   * @param complexityMode the configured complexity-estimation mode, or {@code null} for the default mode.
   * @return one driver-side array containing one {@link AbstractFeature} per cached feature.
   */
  public static AbstractFeature[] collectAbstractFeatures(JavaPairRDD<Long, IFeature> featuresByOid,
                                                          String complexityMode) {
    return collectAbstractFeatures(
        featuresByOid,
        complexityMode,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  public static AbstractFeature[] collectAbstractFeatures(JavaPairRDD<Long, IFeature> featuresByOid,
                                                          String complexityMode,
                                                          String loadEstimateMode) {
    if (featuresByOid == null)
      return new AbstractFeature[0];
    List<AbstractFeature> collected = featuresByOid.map(tuple ->
        AbstractFeature.fromFeatureBinary(
            tuple._2,
            tuple._1,
            tuple._2 == null ? 0L : tuple._2.getStorageSize(),
            resolveFeatureLoadBytes(tuple._2, loadEstimateMode),
            complexityMode)
      ).collect();
    return collected.toArray(new AbstractFeature[0]);
  }

  /**
   * Collects one lightweight descriptor per cached feature while simultaneously aggregating the
   * dataset summary and feature count in the same distributed pass.
   *
   * <p>This helper exists for the prepare phase of partition-aware execution. That phase already
   * needs all {@link AbstractFeature}s on the driver, so the method opportunistically computes the
   * summary and count from the exact same binary-metadata scan instead of performing additional
   * full-RDD actions for {@code count()} and {@code SummaryHelper.computeFor(...)}.
   *
   * @param featuresByOid cached {@code (oid, feature)} RDD prepared earlier in the pipeline
   * @param complexityMode configured complexity-estimation mode, or {@code null} for the default
   * @return one immutable bundle containing the collected abstract features, summary, and count
   */
  public static CollectedAbstractFeatures collectAbstractFeaturesWithSummary(JavaPairRDD<Long, IFeature> featuresByOid,
                                                                             String complexityMode) {
    return collectAbstractFeaturesWithSummary(
        featuresByOid,
        complexityMode,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  public static CollectedAbstractFeatures collectAbstractFeaturesWithSummary(JavaPairRDD<Long, IFeature> featuresByOid,
                                                                             String complexityMode,
                                                                             String loadEstimateMode) {
    if (featuresByOid == null)
      return CollectedAbstractFeatures.empty();
    List<AbstractFeaturePartitionBatch> batches = featuresByOid.mapPartitions(iterator -> {
      List<AbstractFeature> abstractFeatures = new ArrayList<>();
      Summary summary = new Summary();
      long featureCount = 0L;
      while (iterator.hasNext()) {
        Tuple2<Long, IFeature> tuple = iterator.next();
        IFeature feature = tuple._2;
        AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
            feature,
            tuple._1,
            feature == null ? 0L : feature.getStorageSize(),
            resolveFeatureLoadBytes(feature, loadEstimateMode),
            complexityMode);
        abstractFeatures.add(abstractFeature);
        summary.expandToAbstractFeature(abstractFeature);
        featureCount++;
      }
      if (abstractFeatures.isEmpty())
        return Collections.<AbstractFeaturePartitionBatch>emptyList().iterator();
        return Collections.singletonList(
            new AbstractFeaturePartitionBatch(
                abstractFeatures.toArray(new AbstractFeature[0]),
                summary,
                featureCount)).iterator();
    }).collect();
    Summary mergedSummary = new Summary();
    long mergedCount = 0L;
    AbstractFeature[] mergedFeatures = new AbstractFeature[1024];
    int offset = 0;
    for (AbstractFeaturePartitionBatch batch : batches) {
      if (batch == null || batch.getAbstractFeatures().length == 0)
        continue;
      mergedSummary.expandToSummary(batch.getSummary());
      mergedCount += Math.max(0L, batch.getFeatureCount());
      int batchLength = batch.getAbstractFeatures().length;
      if (offset + batchLength > mergedFeatures.length) {
        int newLength = mergedFeatures.length;
        while (offset + batchLength > newLength) {
          newLength = Math.max(newLength * 2, offset + batchLength);
        }
        mergedFeatures = Arrays.copyOf(mergedFeatures, newLength);
      }
      System.arraycopy(batch.getAbstractFeatures(), 0, mergedFeatures, offset, batchLength);
      offset += batchLength;
    }
    if (offset == 0)
      return CollectedAbstractFeatures.empty();
    if (offset != mergedFeatures.length)
      mergedFeatures = Arrays.copyOf(mergedFeatures, offset);
    return new CollectedAbstractFeatures(mergedFeatures, mergedSummary, mergedCount);
  }

  /**
   * Collects one {@link AbstractFeature} per input record together with summary and count
   * directly from a feature RDD that has not yet been keyed by stable OID.
   *
   * <p>This helper is intended for lightweight partition-analysis code paths that only need
   * driver-side partition-construction descriptors and do not need to keep a cached
   * {@code (oid, feature)} working set for later execution.
   */
  public static CollectedAbstractFeatures collectAbstractFeaturesWithSummary(JavaRDD<IFeature> features,
                                                                             String complexityMode) {
    return collectAbstractFeaturesWithSummary(
        features,
        complexityMode,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  public static CollectedAbstractFeatures collectAbstractFeaturesWithSummary(JavaRDD<IFeature> features,
                                                                             String complexityMode,
                                                                             String loadEstimateMode) {
    if (features == null)
      return CollectedAbstractFeatures.empty();
    JavaPairRDD<Long, IFeature> featuresByOid =
        features.zipWithUniqueId().mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
    return collectAbstractFeaturesWithSummary(featuresByOid, complexityMode, loadEstimateMode);
  }

  /**
   * Builds one synopsis descriptor per occupied regular-grid cell instead of collecting one
   * {@link AbstractFeature} per raw record.
   *
   * <p>This path is intended for very large partition-analysis runs where collecting all
   * per-record abstract features would put too much pressure on executor and driver memory.
   * Each Spark partition first aggregates features into local grid cells and only emits those
   * cell-level descriptors. Spark then merges cells with the same key across partitions.
   *
   * @param features distributed input features
   * @param referenceEnvelope shared analysis envelope that defines the synopsis grid
   * @param synopsisOrder grid order where {@code 2^order} cells are created per dimension
   * @param complexityMode configured complexity mode, or {@code null} for the default
   * @return one lightweight descriptor per occupied synopsis cell
   */
  public static AbstractFeature[] collectSynopsisFeatures(JavaRDD<IFeature> features,
                                                          EnvelopeNDLite referenceEnvelope,
                                                          int synopsisOrder,
                                                          String complexityMode) {
    return collectSynopsisFeaturesWithSummary(
        features,
        referenceEnvelope,
        synopsisOrder,
        complexityMode,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE)
        .getSynopsisFeatures();
  }

  public static AbstractFeature[] collectSynopsisFeatures(JavaRDD<IFeature> features,
                                                          EnvelopeNDLite referenceEnvelope,
                                                          int synopsisOrder,
                                                          String complexityMode,
                                                          String loadEstimateMode) {
    return collectSynopsisFeaturesWithSummary(
        features,
        referenceEnvelope,
        synopsisOrder,
        complexityMode,
        loadEstimateMode,
        false)
        .getSynopsisFeatures();
  }

  public static AbstractFeature[] collectSynopsisFeatures(JavaRDD<IFeature> features,
                                                          EnvelopeNDLite referenceEnvelope,
                                                          int synopsisOrder,
                                                          String complexityMode,
                                                          String loadEstimateMode,
                                                          boolean trackRepresentativeMoments) {
    return collectSynopsisFeaturesWithSummary(
        features,
        referenceEnvelope,
        synopsisOrder,
        complexityMode,
        loadEstimateMode,
        trackRepresentativeMoments)
        .getSynopsisFeatures();
  }

  /**
   * Collects synopsis features while simultaneously aggregating the exact dataset summary and
   * feature count in the same distributed pass.
   *
   * <p>This helper is intended for the disk-hybrid prepare path where large datasets previously
   * required one full scan for summary and another full scan for synopsis collection. The current
   * implementation keeps the local work partition-parallel, emits one compact batch per Spark
   * partition, and lets the driver merge only the occupied synopsis cells.
   */
  public static CollectedSynopsisFeatures collectSynopsisFeaturesWithSummary(JavaRDD<IFeature> features,
                                                                             EnvelopeNDLite referenceEnvelope,
                                                                             int synopsisOrder,
                                                                             String complexityMode) {
    return collectSynopsisFeaturesWithSummary(
        features,
        referenceEnvelope,
        synopsisOrder,
        complexityMode,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  public static CollectedSynopsisFeatures collectSynopsisFeaturesWithSummary(JavaRDD<IFeature> features,
                                                                             EnvelopeNDLite referenceEnvelope,
                                                                             int synopsisOrder,
                                                                             String complexityMode,
                                                                             String loadEstimateMode) {
    return collectSynopsisFeaturesWithSummary(
        features,
        referenceEnvelope,
        synopsisOrder,
        complexityMode,
        loadEstimateMode,
        false);
  }

  public static CollectedSynopsisFeatures collectSynopsisFeaturesWithSummary(JavaRDD<IFeature> features,
                                                                             EnvelopeNDLite referenceEnvelope,
                                                                             int synopsisOrder,
                                                                             String complexityMode,
                                                                             String loadEstimateMode,
                                                                             boolean trackRepresentativeMoments) {
    if (features == null || referenceEnvelope == null || referenceEnvelope.isEmpty())
      return CollectedSynopsisFeatures.empty();
    final EnvelopeNDLite safeReferenceEnvelope = new EnvelopeNDLite(referenceEnvelope);
    final int sidePartitions = safeSidePartitions(synopsisOrder);
    List<SynopsisPartitionBatch> batches = features.mapPartitions(iterator -> {
      Map<SynopsisCellKey, AbstractFeature> aggregatedCells = new HashMap<>();
      Summary summary = new Summary();
      long featureCount = 0L;
      while (iterator.hasNext()) {
        IFeature feature = iterator.next();
        AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
            feature,
            -1L,
            feature == null ? 0L : feature.getStorageSize(),
            resolveFeatureLoadBytes(feature, loadEstimateMode),
            complexityMode);
        if (abstractFeature == null || abstractFeature.isEmpty() || !abstractFeature.intersectsEnvelope(safeReferenceEnvelope))
          continue;
        if (trackRepresentativeMoments)
          abstractFeature.withExactRepresentativeMoments();
        summary.expandToAbstractFeature(abstractFeature);
        featureCount++;
        SynopsisCellKey cellKey = locateSynopsisCell(abstractFeature, safeReferenceEnvelope, sidePartitions);
        AbstractFeature aggregated = aggregatedCells.get(cellKey);
        if (aggregated == null) {
          aggregated = new AbstractFeature(abstractFeature)
              .withAggregationLevel(Math.max(1, synopsisOrder))
              .withHierarchyKey(cellKey.toHierarchyKey());
          aggregatedCells.put(cellKey, aggregated);
        } else {
          aggregated.mergeInPlace(abstractFeature);
        }
      }
      if (aggregatedCells.isEmpty())
        return Collections.<SynopsisPartitionBatch>emptyList().iterator();
      SynopsisCellAggregate[] aggregates = new SynopsisCellAggregate[aggregatedCells.size()];
      int aggregateIndex = 0;
      for (Map.Entry<SynopsisCellKey, AbstractFeature> entry : aggregatedCells.entrySet()) {
        aggregates[aggregateIndex++] = new SynopsisCellAggregate(entry.getKey(), entry.getValue());
      }
      return Collections.singletonList(new SynopsisPartitionBatch(aggregates, summary, featureCount)).iterator();
    }).collect();
    if (batches == null || batches.isEmpty())
      return CollectedSynopsisFeatures.empty();
    Summary mergedSummary = new Summary();
    long mergedFeatureCount = 0L;
    Map<SynopsisCellKey, AbstractFeature> mergedCells = new HashMap<>();
    for (SynopsisPartitionBatch batch : batches) {
      if (batch == null || batch.getAggregates().length == 0)
        continue;
      mergedSummary.expandToSummary(batch.getSummary());
      mergedFeatureCount += Math.max(0L, batch.getFeatureCount());
      for (SynopsisCellAggregate aggregate : batch.getAggregates()) {
        if (aggregate == null || aggregate.getCellKey() == null || aggregate.getAbstractFeature() == null)
          continue;
        AbstractFeature existing = mergedCells.get(aggregate.getCellKey());
        if (existing == null) {
          mergedCells.put(aggregate.getCellKey(), new AbstractFeature(aggregate.getAbstractFeature()));
        } else {
          existing.mergeInPlace(aggregate.getAbstractFeature());
        }
      }
    }
    if (mergedCells.isEmpty())
      return new CollectedSynopsisFeatures(new AbstractFeature[0], mergedSummary, mergedFeatureCount);
    List<Map.Entry<SynopsisCellKey, AbstractFeature>> ordered = new ArrayList<>(mergedCells.entrySet());
    ordered.sort(Map.Entry.comparingByKey());
    AbstractFeature[] synopsisFeatures = new AbstractFeature[ordered.size()];
    for (int i = 0; i < ordered.size(); i++)
      synopsisFeatures[i] = ordered.get(i).getValue();
    return new CollectedSynopsisFeatures(synopsisFeatures, mergedSummary, mergedFeatureCount);
  }

  /**
   * Builds the regular-grid synopsis directly from disk-backed per-object descriptors.
   *
   * <p>This avoids decoding the original geometry a second time after the descriptors have already
   * been created for exact refinement. Only one local cell map per Spark partition and the merged
   * occupied cells are retained in memory.
   */
  public static AbstractFeature[] collectSynopsisAbstractFeatures(
      JavaRDD<AbstractFeature> features,
      EnvelopeNDLite referenceEnvelope,
      int synopsisOrder,
      boolean trackRepresentativeMoments) {
    if (features == null || referenceEnvelope == null || referenceEnvelope.isEmpty())
      return new AbstractFeature[0];
    final EnvelopeNDLite safeReferenceEnvelope = new EnvelopeNDLite(referenceEnvelope);
    final int sidePartitions = safeSidePartitions(synopsisOrder);
    List<SynopsisCellAggregate[]> batches = features.mapPartitions(iterator -> {
      Map<SynopsisCellKey, AbstractFeature> aggregatedCells = new HashMap<>();
      while (iterator.hasNext()) {
        AbstractFeature abstractFeature = iterator.next();
        if (abstractFeature == null
            || abstractFeature.isEmpty()
            || !abstractFeature.intersectsEnvelope(safeReferenceEnvelope))
          continue;
        if (trackRepresentativeMoments)
          abstractFeature.withExactRepresentativeMoments();
        SynopsisCellKey cellKey = locateSynopsisCell(
            abstractFeature,
            safeReferenceEnvelope,
            sidePartitions);
        AbstractFeature aggregated = aggregatedCells.get(cellKey);
        if (aggregated == null) {
          aggregated = new AbstractFeature(abstractFeature)
              .withAggregationLevel(Math.max(1, synopsisOrder))
              .withHierarchyKey(cellKey.toHierarchyKey());
          aggregatedCells.put(cellKey, aggregated);
        } else {
          aggregated.mergeInPlace(abstractFeature);
        }
      }
      if (aggregatedCells.isEmpty())
        return Collections.<SynopsisCellAggregate[]>emptyList().iterator();
      SynopsisCellAggregate[] aggregates = new SynopsisCellAggregate[aggregatedCells.size()];
      int aggregateIndex = 0;
      for (Map.Entry<SynopsisCellKey, AbstractFeature> entry : aggregatedCells.entrySet()) {
        aggregates[aggregateIndex++] = new SynopsisCellAggregate(entry.getKey(), entry.getValue());
      }
      return Collections.singletonList(aggregates).iterator();
    }).collect();
    if (batches == null || batches.isEmpty())
      return new AbstractFeature[0];

    Map<SynopsisCellKey, AbstractFeature> mergedCells = new HashMap<>();
    for (SynopsisCellAggregate[] batch : batches) {
      if (batch == null)
        continue;
      for (SynopsisCellAggregate aggregate : batch) {
        if (aggregate == null
            || aggregate.getCellKey() == null
            || aggregate.getAbstractFeature() == null)
          continue;
        AbstractFeature existing = mergedCells.get(aggregate.getCellKey());
        if (existing == null) {
          mergedCells.put(
              aggregate.getCellKey(),
              new AbstractFeature(aggregate.getAbstractFeature()));
        } else {
          existing.mergeInPlace(aggregate.getAbstractFeature());
        }
      }
    }
    List<Map.Entry<SynopsisCellKey, AbstractFeature>> ordered =
        new ArrayList<>(mergedCells.entrySet());
    ordered.sort(Map.Entry.comparingByKey());
    AbstractFeature[] synopsisFeatures = new AbstractFeature[ordered.size()];
    for (int i = 0; i < ordered.size(); i++)
      synopsisFeatures[i] = ordered.get(i).getValue();
    return synopsisFeatures;
  }

  /**
   * Collects exact per-partition load statistics by evaluating raw features directly against an
   * already constructed partitioner.
   *
   * <p>This helper is the bridge between a lightweight synopsis-built candidate partition layout
   * and the exact execution-time partition statistics needed later by job planning and disk-hybrid
   * materialization. Unlike {@link #buildEffectivePlan(AbstractFeature[], AbstractFeature[])}, it
   * does not require a full driver-side {@link AbstractFeature} array. Each executor instead
   * expands raw features to every overlapping partition locally, aggregates counts/load/complexity
   * per partition, and only ships those partition-level aggregates back to the driver.
   *
   * @param featuresByOid cached raw {@code (oid, feature)} working set
   * @param partitioner candidate partitioner whose current partition ids are evaluated exactly
   * @param complexityMode configured complexity-estimation mode
   * @return exact per-partition load statistics aligned with the current partitioner ids
   */
  public static PartitionLoadStats collectPartitionLoadStats(JavaPairRDD<Long, IFeature> featuresByOid,
                                                             SpatialPartitioner partitioner,
                                                             String complexityMode) {
    return collectPartitionLoadStats(
        featuresByOid,
        partitioner,
        complexityMode,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  public static PartitionLoadStats collectPartitionLoadStats(JavaPairRDD<Long, IFeature> featuresByOid,
                                                             SpatialPartitioner partitioner,
                                                             String complexityMode,
                                                             String loadEstimateMode) {
    int partitionCount = partitioner == null ? 0 : Math.max(0, partitioner.numPartitions());
    if (featuresByOid == null || partitioner == null || partitionCount == 0)
      return PartitionLoadStats.empty(partitionCount);
    Map<Integer, PartitionLoadAccumulator> aggregated = featuresByOid
        .mapPartitionsToPair(iterator -> {
          Map<Integer, PartitionLoadAccumulator> local = new HashMap<>();
          IntArray matchedPartitions = new IntArray();
          while (iterator.hasNext()) {
            Tuple2<Long, IFeature> tuple = iterator.next();
            if (tuple == null || tuple._2 == null)
              continue;
            IFeature feature = tuple._2;
            AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
                feature,
                tuple._1 == null ? -1L : tuple._1,
                feature.getStorageSize(),
                resolveFeatureLoadBytes(feature, loadEstimateMode),
                complexityMode);
            if (abstractFeature == null || abstractFeature.isEmpty())
              continue;
            partitioner.overlapPartitions(abstractFeature, matchedPartitions);
            for (int i = 0; i < matchedPartitions.size(); i++) {
              int partitionId = matchedPartitions.get(i);
              PartitionLoadAccumulator accumulator = local.computeIfAbsent(
                  partitionId,
                  ignored -> new PartitionLoadAccumulator());
              accumulator.add(abstractFeature, partitioner.getPartitionMBR(partitionId));
            }
          }
          List<Tuple2<Integer, PartitionLoadAccumulator>> outputs = new ArrayList<>(local.size());
          for (Map.Entry<Integer, PartitionLoadAccumulator> entry : local.entrySet())
            outputs.add(new Tuple2<>(entry.getKey(), entry.getValue()));
          return outputs.iterator();
        })
        .reduceByKey((left, right) -> left.mergeInPlace(right))
        .collectAsMap();
    long[] featureCounts = new long[partitionCount];
    long[] totalBytes = new long[partitionCount];
    double[] totalComplexities = new double[partitionCount];
    long[] totalVertexCounts = new long[partitionCount];
    long[] minVertexCounts = new long[partitionCount];
    long[] maxVertexCounts = new long[partitionCount];
    double[] cibCornerCounts = new double[partitionCount];
    double[] cibOverlapRatios = new double[partitionCount];
    double[] cibHorizontalRatios = new double[partitionCount];
    double[] cibVerticalRatios = new double[partitionCount];
    EnvelopeNDLite[] aggregateEnvelopes = new EnvelopeNDLite[partitionCount];
    if (aggregated != null) {
      for (Map.Entry<Integer, PartitionLoadAccumulator> entry : aggregated.entrySet()) {
        if (entry == null)
          continue;
        int partitionId = entry.getKey() == null ? -1 : entry.getKey();
        if (partitionId < 0 || partitionId >= partitionCount)
          continue;
        PartitionLoadAccumulator accumulator = entry.getValue();
        if (accumulator == null)
          continue;
        featureCounts[partitionId] = accumulator.getFeatureCount();
        totalBytes[partitionId] = accumulator.getTotalBytes();
        totalComplexities[partitionId] = accumulator.getTotalComplexity();
        totalVertexCounts[partitionId] = accumulator.getTotalVertexCount();
        minVertexCounts[partitionId] = accumulator.getMinVertexCount();
        maxVertexCounts[partitionId] = accumulator.getMaxVertexCount();
        cibCornerCounts[partitionId] = accumulator.getCibCornerCount();
        cibOverlapRatios[partitionId] = accumulator.getCibOverlapRatio();
        cibHorizontalRatios[partitionId] = accumulator.getCibHorizontalRatio();
        cibVerticalRatios[partitionId] = accumulator.getCibVerticalRatio();
        aggregateEnvelopes[partitionId] = accumulator.getAggregateEnvelope();
      }
    }
    return PartitionLoadStats.of(
        featureCounts,
        totalBytes,
        totalComplexities,
        totalVertexCounts,
        minVertexCounts,
        maxVertexCounts,
        cibCornerCounts,
        cibOverlapRatios,
        cibHorizontalRatios,
        cibVerticalRatios,
        aggregateEnvelopes);
  }

  /**
   * Collects exact per-partition load statistics directly from raw features without requiring a
   * temporary OID mapping.
   *
   * <p>This overload is intended for disk-hybrid preparation paths that materialize final partition
   * files directly from partition geometry and therefore never need replay-by-OID during prepare.
   * The resulting statistics are still exact because each raw feature is expanded against the
   * candidate partitioner on executors before the partition-level aggregates are reduced.
   *
   * @param features raw feature stream for one input side
   * @param partitioner candidate partitioner whose current partition ids are evaluated exactly
   * @param complexityMode configured complexity-estimation mode
   * @return exact per-partition load statistics aligned with the current partitioner ids
   */
  public static PartitionLoadStats collectPartitionLoadStats(JavaRDD<IFeature> features,
                                                             SpatialPartitioner partitioner,
                                                             String complexityMode) {
    return collectPartitionLoadStats(
        features,
        partitioner,
        complexityMode,
        SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  public static PartitionLoadStats collectPartitionLoadStats(JavaRDD<IFeature> features,
                                                             SpatialPartitioner partitioner,
                                                             String complexityMode,
                                                             String loadEstimateMode) {
    int partitionCount = partitioner == null ? 0 : Math.max(0, partitioner.numPartitions());
    if (features == null || partitioner == null || partitionCount == 0)
      return PartitionLoadStats.empty(partitionCount);
    Map<Integer, PartitionLoadAccumulator> aggregated = features
        .mapPartitionsToPair(iterator -> {
          Map<Integer, PartitionLoadAccumulator> local = new HashMap<>();
          IntArray matchedPartitions = new IntArray();
          while (iterator.hasNext()) {
            IFeature feature = iterator.next();
            if (feature == null)
              continue;
            AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
                feature,
                -1L,
                feature.getStorageSize(),
                resolveFeatureLoadBytes(feature, loadEstimateMode),
                complexityMode);
            if (abstractFeature == null || abstractFeature.isEmpty())
              continue;
            partitioner.overlapPartitions(abstractFeature, matchedPartitions);
            for (int i = 0; i < matchedPartitions.size(); i++) {
              int partitionId = matchedPartitions.get(i);
              PartitionLoadAccumulator accumulator = local.computeIfAbsent(
                  partitionId,
                  ignored -> new PartitionLoadAccumulator());
              accumulator.add(abstractFeature, partitioner.getPartitionMBR(partitionId));
            }
          }
          List<Tuple2<Integer, PartitionLoadAccumulator>> outputs = new ArrayList<>(local.size());
          for (Map.Entry<Integer, PartitionLoadAccumulator> entry : local.entrySet())
            outputs.add(new Tuple2<>(entry.getKey(), entry.getValue()));
          return outputs.iterator();
        })
        .reduceByKey((left, right) -> left.mergeInPlace(right))
        .collectAsMap();
    long[] featureCounts = new long[partitionCount];
    long[] totalBytes = new long[partitionCount];
    double[] totalComplexities = new double[partitionCount];
    long[] totalVertexCounts = new long[partitionCount];
    long[] minVertexCounts = new long[partitionCount];
    long[] maxVertexCounts = new long[partitionCount];
    double[] cibCornerCounts = new double[partitionCount];
    double[] cibOverlapRatios = new double[partitionCount];
    double[] cibHorizontalRatios = new double[partitionCount];
    double[] cibVerticalRatios = new double[partitionCount];
    EnvelopeNDLite[] aggregateEnvelopes = new EnvelopeNDLite[partitionCount];
    if (aggregated != null) {
      for (Map.Entry<Integer, PartitionLoadAccumulator> entry : aggregated.entrySet()) {
        if (entry == null)
          continue;
        int partitionId = entry.getKey() == null ? -1 : entry.getKey();
        if (partitionId < 0 || partitionId >= partitionCount)
          continue;
        PartitionLoadAccumulator accumulator = entry.getValue();
        if (accumulator == null)
          continue;
        featureCounts[partitionId] = accumulator.getFeatureCount();
        totalBytes[partitionId] = accumulator.getTotalBytes();
        totalComplexities[partitionId] = accumulator.getTotalComplexity();
        totalVertexCounts[partitionId] = accumulator.getTotalVertexCount();
        minVertexCounts[partitionId] = accumulator.getMinVertexCount();
        maxVertexCounts[partitionId] = accumulator.getMaxVertexCount();
        cibCornerCounts[partitionId] = accumulator.getCibCornerCount();
        cibOverlapRatios[partitionId] = accumulator.getCibOverlapRatio();
        cibHorizontalRatios[partitionId] = accumulator.getCibHorizontalRatio();
        cibVerticalRatios[partitionId] = accumulator.getCibVerticalRatio();
        aggregateEnvelopes[partitionId] = accumulator.getAggregateEnvelope();
      }
    }
    return PartitionLoadStats.of(
        featureCounts,
        totalBytes,
        totalComplexities,
        totalVertexCounts,
        minVertexCounts,
        maxVertexCounts,
        cibCornerCounts,
        cibOverlapRatios,
        cibHorizontalRatios,
        cibVerticalRatios,
        aggregateEnvelopes);
  }

  /**
   * Collects the compact exact statistics needed to feed raw-object distribution back into
   * synopsis-driven recursive refinement.
   *
   * <p>Every source object is converted to a short-lived {@link AbstractFeature} on the executor,
   * replicated to every intersecting current partition, and folded into partition-local counters.
   * Only partition-level counts, byte loads, projected aggregate MBRs, and fixed-width center
   * histograms are returned to the driver. No object-level descriptor array is collected.
   */
  public static PartitionRefinementStats collectPartitionRefinementStats(
      JavaRDD<IFeature> features,
      SpatialPartitioner partitioner,
      String loadEstimateMode,
      int histogramBinCount) {
    int partitionCount = partitioner == null ? 0 : Math.max(0, partitioner.numPartitions());
    int safeBinCount = Math.max(8, histogramBinCount);
    if (features == null || partitioner == null || partitionCount == 0)
      return PartitionRefinementStats.empty(partitionCount, safeBinCount);
    Map<Integer, PartitionRefinementAccumulator> aggregated = features
        .mapPartitionsToPair(iterator -> {
          Map<Integer, PartitionRefinementAccumulator> local = new HashMap<>();
          IntArray matchedPartitions = new IntArray();
          while (iterator.hasNext()) {
            IFeature feature = iterator.next();
            if (feature == null)
              continue;
            AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(
                feature,
                -1L,
                feature.getStorageSize(),
                resolveFeatureLoadBytes(feature, loadEstimateMode),
                null);
            if (abstractFeature == null || abstractFeature.isEmpty())
              continue;
            partitioner.overlapPartitions(abstractFeature, matchedPartitions);
            for (int i = 0; i < matchedPartitions.size(); i++) {
              int partitionId = matchedPartitions.get(i);
              EnvelopeNDLite partitionEnvelope = partitioner.getPartitionMBR(partitionId);
              PartitionRefinementAccumulator accumulator = local.computeIfAbsent(
                  partitionId,
                  ignored -> new PartitionRefinementAccumulator(safeBinCount));
              accumulator.add(abstractFeature, partitionEnvelope);
            }
          }
          List<Tuple2<Integer, PartitionRefinementAccumulator>> outputs =
              new ArrayList<>(local.size());
          for (Map.Entry<Integer, PartitionRefinementAccumulator> entry : local.entrySet())
            outputs.add(new Tuple2<>(entry.getKey(), entry.getValue()));
          return outputs.iterator();
        })
        .reduceByKey((left, right) -> left.mergeInPlace(right))
        .collectAsMap();
    long[] featureCounts = new long[partitionCount];
    long[] totalBytes = new long[partitionCount];
    EnvelopeNDLite[] aggregateEnvelopes = new EnvelopeNDLite[partitionCount];
    long[][][] countHistograms = new long[partitionCount][2][safeBinCount];
    long[][][] byteHistograms = new long[partitionCount][2][safeBinCount];
    if (aggregated != null) {
      for (Map.Entry<Integer, PartitionRefinementAccumulator> entry : aggregated.entrySet()) {
        if (entry == null || entry.getKey() == null)
          continue;
        int partitionId = entry.getKey();
        if (partitionId < 0 || partitionId >= partitionCount || entry.getValue() == null)
          continue;
        PartitionRefinementAccumulator accumulator = entry.getValue();
        featureCounts[partitionId] = accumulator.featureCount;
        totalBytes[partitionId] = accumulator.totalBytes;
        aggregateEnvelopes[partitionId] =
            accumulator.aggregateEnvelope == null
                ? null
                : new EnvelopeNDLite(accumulator.aggregateEnvelope);
        copyHistogram(accumulator.countHistograms, countHistograms[partitionId]);
        copyHistogram(accumulator.byteHistograms, byteHistograms[partitionId]);
      }
    }
    return new PartitionRefinementStats(
        featureCounts,
        totalBytes,
        aggregateEnvelopes,
        countHistograms,
        byteHistograms,
        safeBinCount);
  }

  private static void copyHistogram(long[][] source, long[][] target) {
    if (source == null || target == null)
      return;
    int axisCount = Math.min(source.length, target.length);
    for (int axis = 0; axis < axisCount; axis++) {
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

  /**
   * Builds an exact effective plan from already collected per-partition load statistics.
   *
   * <p>This method is intended for out-of-core partition preparation paths where the geometric
   * partition layout can be built from lightweight synopsis descriptors, but the final effective
   * partition set and execution profiles must still come from an exact distributed statistics pass
   * over all raw features.
   *
   * <p>The returned plan intentionally carries no explicit OID-to-partition assignment lists. It is
   * therefore suitable for execution paths that can materialize the final working set directly from
   * the partitioner geometry, such as the current {@code dual-replication} disk-hybrid path.
   *
   * @param leftStats exact left-side per-partition counts/load/complexity
   * @param rightStats exact right-side per-partition counts/load/complexity
   * @return one exact effective plan aligned with the current partitioner geometry
   */
  public EffectivePlan buildEffectivePlanFromPartitionStats(PartitionLoadStats leftStats,
                                                            PartitionLoadStats rightStats) {
    int originalPartitionCount = Math.max(0, numPartitions());
    long[] leftCounts = alignedLongStats(leftStats == null ? null : leftStats.featureCounts, originalPartitionCount);
    long[] rightCounts = alignedLongStats(rightStats == null ? null : rightStats.featureCounts, originalPartitionCount);
    long[] leftBytes = alignedLongStats(leftStats == null ? null : leftStats.totalBytes, originalPartitionCount);
    long[] rightBytes = alignedLongStats(rightStats == null ? null : rightStats.totalBytes, originalPartitionCount);
    double[] leftComplexities = alignedDoubleStats(leftStats == null ? null : leftStats.totalComplexities, originalPartitionCount);
    double[] rightComplexities = alignedDoubleStats(rightStats == null ? null : rightStats.totalComplexities, originalPartitionCount);
    long[] leftVertexCounts = alignedLongStats(leftStats == null ? null : leftStats.totalVertexCounts, originalPartitionCount);
    long[] rightVertexCounts = alignedLongStats(rightStats == null ? null : rightStats.totalVertexCounts, originalPartitionCount);
    long[] leftMinVertexCounts = alignedLongStats(leftStats == null ? null : leftStats.minVertexCounts, originalPartitionCount);
    long[] rightMinVertexCounts = alignedLongStats(rightStats == null ? null : rightStats.minVertexCounts, originalPartitionCount);
    long[] leftMaxVertexCounts = alignedLongStats(leftStats == null ? null : leftStats.maxVertexCounts, originalPartitionCount);
    long[] rightMaxVertexCounts = alignedLongStats(rightStats == null ? null : rightStats.maxVertexCounts, originalPartitionCount);
    long[] candidatePairCounts = estimateCibCandidatePairCounts(leftStats, rightStats, leftCounts, rightCounts, originalPartitionCount);
    int[] traditionalEffectivePartitionIds = collectEffectivePartitionIds(
        leftCounts,
        rightCounts,
        null,
        null,
        false);
    int nonEmptyCandidatePartitionCount = countNonEmptyPartitions(leftCounts, rightCounts);
    long nonEmptyCandidateLoadBytes = sumNonEmptyCandidateLoadBytes(leftCounts, rightCounts, leftBytes, rightBytes);
    int[] effectivePartitionIds = collectEffectivePartitionIds(leftStats, rightStats, originalPartitionCount);
    int[] oldToNewPartitionIds = retainPartitions(effectivePartitionIds);
    SpatialPartitioner effectivePartitioner = this;
    if (numPartitions() != effectivePartitionIds.length)
      effectivePartitioner = buildExplicitEffectivePartitioner(effectivePartitionIds);
    long[] effectiveLeftCounts = remapLoads(leftCounts, oldToNewPartitionIds);
    long[] effectiveRightCounts = remapLoads(rightCounts, oldToNewPartitionIds);
    long[] effectiveLeftBytes = remapLoads(leftBytes, oldToNewPartitionIds);
    long[] effectiveRightBytes = remapLoads(rightBytes, oldToNewPartitionIds);
    double[] effectiveLeftComplexities = remapLoads(leftComplexities, oldToNewPartitionIds);
    double[] effectiveRightComplexities = remapLoads(rightComplexities, oldToNewPartitionIds);
    long[] effectiveLeftVertexCounts = remapLoads(leftVertexCounts, oldToNewPartitionIds);
    long[] effectiveRightVertexCounts = remapLoads(rightVertexCounts, oldToNewPartitionIds);
    long[] effectiveLeftMinVertexCounts = remapLoads(leftMinVertexCounts, oldToNewPartitionIds);
    long[] effectiveRightMinVertexCounts = remapLoads(rightMinVertexCounts, oldToNewPartitionIds);
    long[] effectiveLeftMaxVertexCounts = remapLoads(leftMaxVertexCounts, oldToNewPartitionIds);
    long[] effectiveRightMaxVertexCounts = remapLoads(rightMaxVertexCounts, oldToNewPartitionIds);
    long[] effectiveCandidatePairCounts = remapLoads(candidatePairCounts, oldToNewPartitionIds);
    return new EffectivePlan(
        effectivePartitioner,
        PartProfile.fromPartitioner(
            effectivePartitioner,
            effectiveLeftCounts,
            effectiveRightCounts,
            effectiveLeftBytes,
            effectiveRightBytes,
            effectiveLeftComplexities,
            effectiveRightComplexities,
            effectiveLeftVertexCounts,
            effectiveRightVertexCounts,
            effectiveLeftMinVertexCounts,
            effectiveRightMinVertexCounts,
            effectiveLeftMaxVertexCounts,
            effectiveRightMaxVertexCounts,
            effectiveCandidatePairCounts),
        Collections.emptyList(),
        Collections.emptyList(),
        originalPartitionCount,
        nonEmptyCandidatePartitionCount,
        nonEmptyCandidateLoadBytes,
        traditionalEffectivePartitionIds.length,
        Math.max(0, traditionalEffectivePartitionIds.length - effectivePartitionIds.length));
  }

  /**
   * Returns the original candidate-partitioner ids that remain effective after exact left/right
   * partition statistics are known.
   *
   * <p>The returned ids are ordered the same way as the dense execution-time partition ids created
   * later by {@link #buildEffectivePlanFromPartitionStats(PartitionLoadStats, PartitionLoadStats)}.
   * This is useful for disk-hybrid preparation paths that first materialize candidate partition
   * files and then need to promote only the retained subset into the final dense-id layout.
   */
  public static int[] collectEffectivePartitionIdsFromStats(PartitionLoadStats leftStats,
                                                            PartitionLoadStats rightStats,
                                                            int partitionCount) {
    return collectEffectivePartitionIds(leftStats, rightStats, partitionCount);
  }

  /**
   * Reloads only the binary features assigned to a target partition set.
   *
   * Later join execution uses this method to bring back only the records belonging
   * to one planned job before any geometry deserialization is triggered.
   */
  public static JavaPairRDD<Integer, IFeature> loadAssignedBinaryFeatures(JavaRDD<IFeature> features,
                                                                          List<FeatureRef> assignments,
                                                                          int[] targetPartitionIds) {
    if (features == null)
      return loadAssignedBinaryFeatures((JavaPairRDD<Long, IFeature>) null, assignments, targetPartitionIds);
    JavaPairRDD<Long, IFeature> featuresByOid = features.zipWithUniqueId().mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
    return loadAssignedBinaryFeatures(featuresByOid, assignments, targetPartitionIds);
  }

  /**
   * Reloads only the binary features assigned to a target partition set from a precomputed OID mapping.
   *
   * This overload is used by the partition-aware execution path after the original feature
   * RDD has been materialized once as {@code (oid, feature)} and cached for reuse across jobs.
   *
   * <p>The method does not redo any spatial overlap checks. Instead, it:
   * <ol>
   *   <li>filters the compact {@link FeatureRef} assignment list down to the requested partition ids,</li>
   *   <li>parallelizes those selected assignments as {@code (oid, partitionId)}, and</li>
   *   <li>joins them with the cached {@code (oid, feature)} RDD to recover
   *       {@code (partitionId, feature)} pairs.</li>
   * </ol>
   *
   * <p>This is the key optimization that reuses the partition-construction result during execution:
   * spatial membership is computed once up front, then later jobs reload by OID only.
   *
   * @param featuresByOid cached {@code (oid, feature)} RDD for one input side.
   * @param assignments full assignment list produced by the effective partition construction phase.
   * @param targetPartitionIds partition ids required by the current job.
   * @return one pair RDD of {@code (partitionId, feature)} containing only the requested partitions.
   */
  public static JavaPairRDD<Integer, IFeature> loadAssignedBinaryFeatures(JavaPairRDD<Long, IFeature> featuresByOid,
                                                                          List<FeatureRef> assignments,
                                                                          int[] targetPartitionIds) {
    if (featuresByOid == null || assignments == null || assignments.isEmpty() ||
        targetPartitionIds == null || targetPartitionIds.length == 0) {
      JavaSparkContext sparkContext = featuresByOid == null
          ? JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate())
          : JavaSparkContext.fromSparkContext(featuresByOid.context());
      return sparkContext.parallelizePairs(new ArrayList<>());
    }
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(featuresByOid.context());
    Set<Integer> targetIds = new HashSet<>();
    for (int targetPartitionId : targetPartitionIds)
      targetIds.add(targetPartitionId);
    List<Tuple2<Long, Integer>> selectedAssignments = new ArrayList<>();
    for (FeatureRef assignment : assignments) {
      if (targetIds.contains(assignment.getPartitionId()))
        selectedAssignments.add(new Tuple2<>(assignment.getOid(), assignment.getPartitionId()));
    }
    if (selectedAssignments.isEmpty())
      return sparkContext.parallelizePairs(new ArrayList<>());
    JavaPairRDD<Long, Integer> assignmentRDD = sparkContext.parallelizePairs(selectedAssignments);
    return featuresByOid.join(assignmentRDD).mapToPair(tuple -> new Tuple2<>(tuple._2._2, tuple._2._1));
  }

  private static long[] alignedLongStats(long[] values, int partitionCount) {
    long[] aligned = new long[Math.max(0, partitionCount)];
    if (values == null || aligned.length == 0)
      return aligned;
    System.arraycopy(values, 0, aligned, 0, Math.min(values.length, aligned.length));
    return aligned;
  }

  private static double[] alignedDoubleStats(double[] values, int partitionCount) {
    double[] aligned = new double[Math.max(0, partitionCount)];
    if (values == null || aligned.length == 0)
      return aligned;
    System.arraycopy(values, 0, aligned, 0, Math.min(values.length, aligned.length));
    return aligned;
  }

  /**
   * Returns a conservative upper bound on the compact broadcast index size for one assignment list.
   *
   * <p>The bound assumes the worst case in which every assignment belongs to a different OID, i.e.
   * the broadcast table needs one {@code long} OID, one {@code int} offset, and one {@code int}
   * partition id per assignment.
   */
  public static long estimateAssignmentLookupTableUpperBoundBytes(int assignmentCount) {
    long safeCount = Math.max(0L, assignmentCount);
    return (Long.BYTES + Integer.BYTES + Integer.BYTES) * safeCount + Integer.BYTES;
  }

  /**
   * Builds a compact OID-to-partition lookup table suitable for short-lived Spark broadcast use.
   *
   * <p>The returned structure stores three primitive arrays:
   * <ul>
   *   <li>{@code oids[i]}: the stable feature OID, sorted ascending</li>
   *   <li>{@code offsets[i]}: start offset of that OID's partition ids in {@code partitionIds}</li>
   *   <li>{@code partitionIds[offsets[i] .. offsets[i+1])}: dense effective partition ids</li>
   * </ul>
   *
   * <p>This representation is materially smaller than the object-heavy {@code List<FeatureRef>} already
   * held by the effective plan and is therefore a good fit for the one-shot final working-set
   * materialization step.
   */
  public static AssignmentLookupTable buildAssignmentLookupTable(List<FeatureRef> assignments) {
    if (assignments == null || assignments.isEmpty())
      return AssignmentLookupTable.empty();
    Map<Long, IntArray> partitionsByOid = new HashMap<>();
    int assignmentCount = 0;
    for (FeatureRef assignment : assignments) {
      if (assignment == null)
        continue;
      IntArray partitionIds = partitionsByOid.computeIfAbsent(assignment.getOid(), key -> new IntArray());
      partitionIds.add(assignment.getPartitionId());
      assignmentCount++;
    }
    if (assignmentCount == 0 || partitionsByOid.isEmpty())
      return AssignmentLookupTable.empty();

    long[] oids = new long[partitionsByOid.size()];
    int oidIndex = 0;
    for (Long oid : partitionsByOid.keySet())
      oids[oidIndex++] = oid;
    Arrays.sort(oids);

    int[] offsets = new int[oids.length + 1];
    int[] partitionIds = new int[assignmentCount];
    int partitionOffset = 0;
    for (int i = 0; i < oids.length; i++) {
      offsets[i] = partitionOffset;
      IntArray partitions = partitionsByOid.get(oids[i]);
      if (partitions == null || partitions.isEmpty())
        continue;
      int size = partitions.size();
      System.arraycopy(partitions.underlyingArray(), 0, partitionIds, partitionOffset, size);
      partitionOffset += size;
    }
    offsets[oids.length] = partitionOffset;
    if (partitionOffset != partitionIds.length)
      partitionIds = Arrays.copyOf(partitionIds, partitionOffset);
    return new AssignmentLookupTable(oids, offsets, partitionIds);
  }

  /**
   * Assigns every lightweight feature to all intersecting partitions and accumulates
   * count/byte statistics for each receiving partition.
   */
  private List<FeatureRef> assign(AbstractFeature[] features, long[] counts, long[] bytes) {
    return assign(features, counts, bytes, null, null, null, null, null, null);
  }

  /**
   * Assigns every lightweight feature to all intersecting partitions and accumulates
   * count/byte/complexity statistics for each receiving partition.
   *
   * <p>This is the core data-distribution routine used during effective partition construction.
   * For each {@link AbstractFeature}, the method:
   * <ol>
   *   <li>finds all partition ids whose partition MBR intersects the feature envelope,</li>
   *   <li>increments the count/load/complexity summaries of those partitions, and</li>
   *   <li>records one compact {@link FeatureRef} assignment per hit.</li>
   * </ol>
   *
   * <p>The returned assignments are later reused by job execution so the project does not need to repeat
   * the same spatial membership checks when reloading partition-local binary features.
   *
   * @param features the lightweight features to distribute.
   * @param counts mutable per-partition feature counters updated in place.
   * @param bytes mutable per-partition memory-load counters updated in place.
   * @param complexities optional mutable per-partition computation-intensity counters updated in place.
   * @return one compact assignment list containing {@code (oid, partitionId)} for all hits.
   */
  private List<FeatureRef> assign(AbstractFeature[] features, long[] counts, long[] bytes, double[] complexities) {
    return assign(features, counts, bytes, complexities, null, null, null, null, null);
  }

  private List<FeatureRef> assign(AbstractFeature[] features,
                                  long[] counts,
                                  long[] bytes,
                                  double[] complexities,
                                  long[] vertexCounts,
                                  long[] minVertexCounts,
                                  long[] maxVertexCounts,
                                  CibHistogramStats[] cibStatsByPartition,
                                  EnvelopeNDLite[] aggregateEnvelopes) {
    List<FeatureRef> assignments = new ArrayList<>();
    if (features == null)
      return assignments;
    IntArray matchedPartitions = new IntArray();
    for (AbstractFeature feature : features) {
      if (feature == null || feature.isEmpty() || numPartitions() == 0)
        continue;
      matchedPartitions.clear();
      overlapPartitions(feature, matchedPartitions);
      for (int i = 0; i < matchedPartitions.size(); i++) {
        int partitionId = matchedPartitions.get(i);
        if (partitionId < 0 || partitionId >= numPartitions())
          continue;
        counts[partitionId] += 1L;
        bytes[partitionId] += Math.max(0L, feature.getLoadWeight());
        if (complexities != null)
          complexities[partitionId] += Math.max(0.0d, feature.getComplexityEstimate());
        accumulateVertexStats(feature, partitionId, vertexCounts, minVertexCounts, maxVertexCounts);
        if (cibStatsByPartition != null && partitionId < cibStatsByPartition.length && cibStatsByPartition[partitionId] != null)
          cibStatsByPartition[partitionId].add(feature, getPartitionMBR(partitionId));
        accumulateProjectedEnvelope(feature, getPartitionMBR(partitionId), aggregateEnvelopes, partitionId);
        assignments.add(new FeatureRef(feature.getOid(), partitionId));
      }
    }
    return assignments;
  }

  private static void accumulateVertexStats(AbstractFeature feature,
                                            int partitionId,
                                            long[] vertexCounts,
                                            long[] minVertexCounts,
                                            long[] maxVertexCounts) {
    if (feature == null || partitionId < 0)
      return;
    long vertexCount = Math.max(0L, feature.getNumPoints());
    if (vertexCounts != null && partitionId < vertexCounts.length)
      vertexCounts[partitionId] += vertexCount;
    if (minVertexCounts != null && partitionId < minVertexCounts.length) {
      long currentMin = minVertexCounts[partitionId];
      if (currentMin <= 0L || vertexCount < currentMin)
        minVertexCounts[partitionId] = vertexCount;
    }
    if (maxVertexCounts != null && partitionId < maxVertexCounts.length)
      maxVertexCounts[partitionId] = Math.max(maxVertexCounts[partitionId], vertexCount);
  }

  /**
   * Returns partitions that receive both input sides and whose side-specific aggregate MBRs overlap.
   *
   * <p>The aggregate MBR of one side is built from feature MBRs projected into the current
   * partition. Disjoint aggregate MBRs prove that the partition cannot contain an intersecting
   * cross-side candidate. Missing aggregate metadata falls back to the historical count-only rule.
   */
  private static int[] collectEffectivePartitionIds(PartitionLoadStats leftStats,
                                                    PartitionLoadStats rightStats,
                                                    int partitionCount) {
    long[] leftCounts = alignedLongStats(leftStats == null ? null : leftStats.featureCounts, partitionCount);
    long[] rightCounts = alignedLongStats(rightStats == null ? null : rightStats.featureCounts, partitionCount);
    boolean aggregateFilterAvailable =
        leftStats != null
            && rightStats != null
            && leftStats.aggregateEnvelopeStatsAvailable
            && rightStats.aggregateEnvelopeStatsAvailable;
    return collectEffectivePartitionIds(
        leftCounts,
        rightCounts,
        leftStats == null ? null : leftStats.aggregateEnvelopes,
        rightStats == null ? null : rightStats.aggregateEnvelopes,
        aggregateFilterAvailable);
  }

  private static int[] collectEffectivePartitionIds(long[] leftCounts,
                                                    long[] rightCounts,
                                                    EnvelopeNDLite[] leftAggregateEnvelopes,
                                                    EnvelopeNDLite[] rightAggregateEnvelopes,
                                                    boolean aggregateFilterAvailable) {
    int partitionCount = Math.min(
        leftCounts == null ? 0 : leftCounts.length,
        rightCounts == null ? 0 : rightCounts.length);
    int count = 0;
    for (int i = 0; i < partitionCount; i++) {
      if (isEffectivePartition(
          i,
          leftCounts,
          rightCounts,
          leftAggregateEnvelopes,
          rightAggregateEnvelopes,
          aggregateFilterAvailable))
        count++;
    }
    int[] effectivePartitionIds = new int[count];
    int offset = 0;
    for (int i = 0; i < partitionCount; i++) {
      if (isEffectivePartition(
          i,
          leftCounts,
          rightCounts,
          leftAggregateEnvelopes,
          rightAggregateEnvelopes,
          aggregateFilterAvailable))
        effectivePartitionIds[offset++] = i;
    }
    return effectivePartitionIds;
  }

  private static int countNonEmptyPartitions(long[] leftCounts, long[] rightCounts) {
    int partitionCount = Math.min(
        leftCounts == null ? 0 : leftCounts.length,
        rightCounts == null ? 0 : rightCounts.length);
    int count = 0;
    for (int i = 0; i < partitionCount; i++) {
      if (leftCounts[i] > 0L || rightCounts[i] > 0L)
        count++;
    }
    return count;
  }

  private static long sumNonEmptyCandidateLoadBytes(long[] leftCounts,
                                                    long[] rightCounts,
                                                    long[] leftBytes,
                                                    long[] rightBytes) {
    int partitionCount = Math.min(
        leftCounts == null ? 0 : leftCounts.length,
        rightCounts == null ? 0 : rightCounts.length);
    long total = 0L;
    for (int i = 0; i < partitionCount; i++) {
      if (leftCounts[i] <= 0L && rightCounts[i] <= 0L)
        continue;
      total = saturatedAdd(total, valueAt(leftBytes, i));
      total = saturatedAdd(total, valueAt(rightBytes, i));
    }
    return total;
  }

  private static boolean isEffectivePartition(int partitionId,
                                              long[] leftCounts,
                                              long[] rightCounts,
                                              EnvelopeNDLite[] leftAggregateEnvelopes,
                                              EnvelopeNDLite[] rightAggregateEnvelopes,
                                              boolean aggregateFilterAvailable) {
    if (leftCounts[partitionId] <= 0L || rightCounts[partitionId] <= 0L)
      return false;
    if (!aggregateFilterAvailable)
      return true;
    EnvelopeNDLite leftEnvelope = envelopeAt(leftAggregateEnvelopes, partitionId);
    EnvelopeNDLite rightEnvelope = envelopeAt(rightAggregateEnvelopes, partitionId);
    if (leftEnvelope == null || leftEnvelope.isEmpty() || rightEnvelope == null || rightEnvelope.isEmpty())
      return true;
    return leftEnvelope.intersectsEnvelopeClosed(rightEnvelope);
  }

  private static EnvelopeNDLite envelopeAt(EnvelopeNDLite[] envelopes, int partitionId) {
    return envelopes == null || partitionId < 0 || partitionId >= envelopes.length
        ? null
        : envelopes[partitionId];
  }

  private static void accumulateProjectedEnvelope(AbstractFeature feature,
                                                  EnvelopeNDLite partitionEnvelope,
                                                  EnvelopeNDLite[] aggregateEnvelopes,
                                                  int partitionId) {
    if (feature == null
        || partitionEnvelope == null
        || aggregateEnvelopes == null
        || partitionId < 0
        || partitionId >= aggregateEnvelopes.length)
      return;
    aggregateEnvelopes[partitionId] = updateProjectedAggregateEnvelope(
        aggregateEnvelopes[partitionId],
        feature,
        partitionEnvelope);
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

  /** Remaps a load array from old partition ids to the retained dense effective ids. */
  private static long[] remapLoads(long[] source, int[] oldToNew) {
    int newLength = 0;
    for (int value : oldToNew) {
      if (value >= 0)
        newLength = Math.max(newLength, value + 1);
    }
    long[] remapped = new long[newLength];
    if (source == null)
      return remapped;
    for (int oldPartitionId = 0; oldPartitionId < source.length && oldPartitionId < oldToNew.length; oldPartitionId++) {
      int newPartitionId = oldToNew[oldPartitionId];
      if (newPartitionId >= 0)
        remapped[newPartitionId] += source[oldPartitionId];
    }
    return remapped;
  }

  /** Remaps a complexity array from old partition ids to retained dense ids. */
  private static double[] remapLoads(double[] source, int[] oldToNew) {
    int newLength = 0;
    for (int value : oldToNew) {
      if (value >= 0)
        newLength = Math.max(newLength, value + 1);
    }
    double[] remapped = new double[newLength];
    if (source == null)
      return remapped;
    for (int oldPartitionId = 0; oldPartitionId < source.length && oldPartitionId < oldToNew.length; oldPartitionId++) {
      int newPartitionId = oldToNew[oldPartitionId];
      if (newPartitionId >= 0)
        remapped[newPartitionId] += source[oldPartitionId];
    }
    return remapped;
  }

  private static CibHistogramStats[] createCibStatsArray(int partitionCount) {
    CibHistogramStats[] stats = new CibHistogramStats[Math.max(0, partitionCount)];
    for (int i = 0; i < stats.length; i++)
      stats[i] = new CibHistogramStats();
    return stats;
  }

  private static long[] estimateCibCandidatePairCounts(CibHistogramStats[] leftStats,
                                                       CibHistogramStats[] rightStats,
                                                       long[] leftCounts,
                                                       long[] rightCounts) {
    int partitionCount = Math.max(
        Math.max(leftCounts == null ? 0 : leftCounts.length, rightCounts == null ? 0 : rightCounts.length),
        Math.max(leftStats == null ? 0 : leftStats.length, rightStats == null ? 0 : rightStats.length));
    long[] candidatePairCounts = new long[partitionCount];
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      long leftCount = valueAt(leftCounts, partitionId);
      long rightCount = valueAt(rightCounts, partitionId);
      candidatePairCounts[partitionId] = estimateCibCandidatePairCount(
          leftStats == null || partitionId >= leftStats.length ? null : leftStats[partitionId],
          rightStats == null || partitionId >= rightStats.length ? null : rightStats[partitionId],
          leftCount,
          rightCount,
          false);
    }
    return candidatePairCounts;
  }

  private static long[] estimateCibCandidatePairCounts(PartitionLoadStats leftStats,
                                                       PartitionLoadStats rightStats,
                                                       long[] leftCounts,
                                                       long[] rightCounts,
                                                       int partitionCount) {
    boolean hasCibStats = leftStats != null && rightStats != null &&
        leftStats.hasCibHistogramStats() && rightStats.hasCibHistogramStats();
    int count = Math.max(0, partitionCount);
    long[] candidatePairCounts = new long[count];
    for (int partitionId = 0; partitionId < count; partitionId++) {
      long leftCount = valueAt(leftCounts, partitionId);
      long rightCount = valueAt(rightCounts, partitionId);
      CibHistogramStats leftCibStats = hasCibStats
          ? CibHistogramStats.fromArrays(
              leftStats.cibCornerCounts,
              leftStats.cibOverlapRatios,
              leftStats.cibHorizontalRatios,
              leftStats.cibVerticalRatios,
              partitionId)
          : null;
      CibHistogramStats rightCibStats = hasCibStats
          ? CibHistogramStats.fromArrays(
              rightStats.cibCornerCounts,
              rightStats.cibOverlapRatios,
              rightStats.cibHorizontalRatios,
              rightStats.cibVerticalRatios,
              partitionId)
          : null;
      candidatePairCounts[partitionId] = estimateCibCandidatePairCount(
          leftCibStats,
          rightCibStats,
          leftCount,
          rightCount,
          !hasCibStats);
    }
    return candidatePairCounts;
  }

  private static long estimateCibCandidatePairCount(CibHistogramStats leftStats,
                                                    CibHistogramStats rightStats,
                                                    long leftCount,
                                                    long rightCount,
                                                    boolean fallbackToProduct) {
    if (leftCount <= 0L || rightCount <= 0L)
      return 0L;
    if (fallbackToProduct || leftStats == null || rightStats == null)
      return saturatedProduct(leftCount, rightCount);
    double estimate =
        (rightStats.cornerCount * leftStats.overlapRatio +
            leftStats.cornerCount * rightStats.overlapRatio +
            rightStats.horizontalRatio * leftStats.verticalRatio +
            leftStats.horizontalRatio * rightStats.verticalRatio) / 4.0d;
    if (!Double.isFinite(estimate) || estimate <= 0.0d)
      return 0L;
    if (estimate >= Long.MAX_VALUE)
      return Long.MAX_VALUE;
    return (long) Math.ceil(estimate);
  }

  private static long saturatedProduct(long left, long right) {
    long safeLeft = Math.max(0L, left);
    long safeRight = Math.max(0L, right);
    if (safeLeft == 0L || safeRight == 0L)
      return 0L;
    if (safeLeft > Long.MAX_VALUE / safeRight)
      return Long.MAX_VALUE;
    return safeLeft * safeRight;
  }

  private static long saturatedAdd(long left, long right) {
    long safeLeft = Math.max(0L, left);
    long safeRight = Math.max(0L, right);
    if (safeLeft > Long.MAX_VALUE - safeRight)
      return Long.MAX_VALUE;
    return safeLeft + safeRight;
  }

  private static long valueAt(long[] values, int index) {
    return values == null || index < 0 || index >= values.length ? 0L : values[index];
  }

  /** Remaps OID-to-partition assignments after effective partitions have been compacted. */
  private static List<FeatureRef> remapAssignments(List<FeatureRef> assignments, int[] oldToNew) {
    List<FeatureRef> remapped = new ArrayList<>();
    if (assignments == null || assignments.isEmpty())
      return remapped;
    for (FeatureRef assignment : assignments) {
      int oldPartitionId = assignment.getPartitionId();
      if (oldPartitionId < 0 || oldPartitionId >= oldToNew.length)
        continue;
      int newPartitionId = oldToNew[oldPartitionId];
      if (newPartitionId >= 0)
        remapped.add(new FeatureRef(assignment.getOid(), newPartitionId));
    }
    return remapped;
  }

  /**
   * Lightweight CIB-style histogram components for one partition cell.
   *
   * <p>For every feature assigned to the partition, we accumulate four MBR/cell
   * summary terms: corner count (C), overlap-area ratio (O), horizontal overlap
   * ratio (H), and vertical overlap ratio (V). The left/right summaries later
   * estimate candidate pairs without enumerating MBR pairs.
   */
  public static final class CibHistogramStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private double cornerCount;
    private double overlapRatio;
    private double horizontalRatio;
    private double verticalRatio;

    public CibHistogramStats add(EnvelopeNDLite featureEnvelope, EnvelopeNDLite partitionEnvelope) {
      if (featureEnvelope == null || partitionEnvelope == null ||
          featureEnvelope.isEmpty() || partitionEnvelope.isEmpty() ||
          featureEnvelope.getCoordinateDimension() < 2 || partitionEnvelope.getCoordinateDimension() < 2) {
        return this;
      }
      double cellMinX = partitionEnvelope.getMinCoord(0);
      double cellMaxX = partitionEnvelope.getMaxCoord(0);
      double cellMinY = partitionEnvelope.getMinCoord(1);
      double cellMaxY = partitionEnvelope.getMaxCoord(1);
      double cellWidth = cellMaxX - cellMinX;
      double cellHeight = cellMaxY - cellMinY;
      if (!Double.isFinite(cellWidth) || !Double.isFinite(cellHeight) || cellWidth <= 0.0d || cellHeight <= 0.0d)
        return this;

      double featureMinX = featureEnvelope.getMinCoord(0);
      double featureMaxX = featureEnvelope.getMaxCoord(0);
      double featureMinY = featureEnvelope.getMinCoord(1);
      double featureMaxY = featureEnvelope.getMaxCoord(1);
      double overlapWidth = Math.min(featureMaxX, cellMaxX) - Math.max(featureMinX, cellMinX);
      double overlapHeight = Math.min(featureMaxY, cellMaxY) - Math.max(featureMinY, cellMinY);
      if (!Double.isFinite(overlapWidth) || !Double.isFinite(overlapHeight) ||
          overlapWidth < 0.0d || overlapHeight < 0.0d) {
        return this;
      }

      cornerCount += countCornersInside(featureMinX, featureMaxX, featureMinY, featureMaxY,
          cellMinX, cellMaxX, cellMinY, cellMaxY);
      horizontalRatio += overlapWidth / cellWidth;
      verticalRatio += overlapHeight / cellHeight;
      overlapRatio += (overlapWidth * overlapHeight) / (cellWidth * cellHeight);
      return this;
    }

    public CibHistogramStats mergeInPlace(CibHistogramStats other) {
      if (other == null)
        return this;
      this.cornerCount += other.cornerCount;
      this.overlapRatio += other.overlapRatio;
      this.horizontalRatio += other.horizontalRatio;
      this.verticalRatio += other.verticalRatio;
      return this;
    }

    public double getCornerCount() {
      return cornerCount;
    }

    public double getOverlapRatio() {
      return overlapRatio;
    }

    public double getHorizontalRatio() {
      return horizontalRatio;
    }

    public double getVerticalRatio() {
      return verticalRatio;
    }

    private static CibHistogramStats fromArrays(double[] cornerCounts,
                                                double[] overlapRatios,
                                                double[] horizontalRatios,
                                                double[] verticalRatios,
                                                int index) {
      CibHistogramStats stats = new CibHistogramStats();
      stats.cornerCount = valueAt(cornerCounts, index);
      stats.overlapRatio = valueAt(overlapRatios, index);
      stats.horizontalRatio = valueAt(horizontalRatios, index);
      stats.verticalRatio = valueAt(verticalRatios, index);
      return stats;
    }

    private static double valueAt(double[] values, int index) {
      return values == null || index < 0 || index >= values.length ? 0.0d : values[index];
    }

    private static int countCornersInside(double featureMinX,
                                          double featureMaxX,
                                          double featureMinY,
                                          double featureMaxY,
                                          double cellMinX,
                                          double cellMaxX,
                                          double cellMinY,
                                          double cellMaxY) {
      int count = 0;
      if (containsClosed(featureMinX, featureMinY, cellMinX, cellMaxX, cellMinY, cellMaxY))
        count++;
      if (containsClosed(featureMinX, featureMaxY, cellMinX, cellMaxX, cellMinY, cellMaxY))
        count++;
      if (containsClosed(featureMaxX, featureMinY, cellMinX, cellMaxX, cellMinY, cellMaxY))
        count++;
      if (containsClosed(featureMaxX, featureMaxY, cellMinX, cellMaxX, cellMinY, cellMaxY))
        count++;
      return count;
    }

    private static boolean containsClosed(double x,
                                          double y,
                                          double minX,
                                          double maxX,
                                          double minY,
                                          double maxY) {
      return x >= minX && x <= maxX && y >= minY && y <= maxY;
    }
  }

  public static final class FeatureRef implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long oid;
    private final int partitionId;

    /** Creates one lightweight OID-to-partition mapping entry. */
    public FeatureRef(long oid, int partitionId) {
      this.oid = oid;
      this.partitionId = partitionId;
    }

    /** Returns the stable feature OID used later during binary reload. */
    public long getOid() {
      return oid;
    }

    /** Returns the dense effective partition id receiving this feature. */
    public int getPartitionId() {
      return partitionId;
    }
  }

  /**
   * Exact per-partition load statistics collected from raw features.
   */
  public static final class PartitionLoadStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long[] featureCounts;
    private final long[] totalBytes;
    private final double[] totalComplexities;
    private final long[] totalVertexCounts;
    private final long[] minVertexCounts;
    private final long[] maxVertexCounts;
    private final double[] cibCornerCounts;
    private final double[] cibOverlapRatios;
    private final double[] cibHorizontalRatios;
    private final double[] cibVerticalRatios;
    private final boolean cibHistogramStatsAvailable;
    private final EnvelopeNDLite[] aggregateEnvelopes;
    private final boolean aggregateEnvelopeStatsAvailable;

    private PartitionLoadStats(long[] featureCounts, long[] totalBytes, double[] totalComplexities) {
      this(
          featureCounts,
          totalBytes,
          totalComplexities,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          false,
          null,
          false);
    }

    private PartitionLoadStats(long[] featureCounts,
                               long[] totalBytes,
                               double[] totalComplexities,
                               long[] totalVertexCounts,
                               long[] minVertexCounts,
                               long[] maxVertexCounts,
                               double[] cibCornerCounts,
                               double[] cibOverlapRatios,
                               double[] cibHorizontalRatios,
                               double[] cibVerticalRatios,
                               boolean cibHistogramStatsAvailable,
                               EnvelopeNDLite[] aggregateEnvelopes,
                               boolean aggregateEnvelopeStatsAvailable) {
      this.featureCounts = featureCounts == null ? new long[0] : featureCounts;
      this.totalBytes = totalBytes == null ? new long[0] : totalBytes;
      this.totalComplexities = totalComplexities == null ? new double[0] : totalComplexities;
      this.totalVertexCounts = totalVertexCounts == null ? new long[0] : totalVertexCounts;
      this.minVertexCounts = minVertexCounts == null ? new long[0] : minVertexCounts;
      this.maxVertexCounts = maxVertexCounts == null ? new long[0] : maxVertexCounts;
      this.cibCornerCounts = cibCornerCounts == null ? new double[0] : cibCornerCounts;
      this.cibOverlapRatios = cibOverlapRatios == null ? new double[0] : cibOverlapRatios;
      this.cibHorizontalRatios = cibHorizontalRatios == null ? new double[0] : cibHorizontalRatios;
      this.cibVerticalRatios = cibVerticalRatios == null ? new double[0] : cibVerticalRatios;
      this.cibHistogramStatsAvailable = cibHistogramStatsAvailable;
      this.aggregateEnvelopes = copyEnvelopes(aggregateEnvelopes);
      this.aggregateEnvelopeStatsAvailable = aggregateEnvelopeStatsAvailable;
    }

    public static PartitionLoadStats empty(int partitionCount) {
      return new PartitionLoadStats(
          new long[Math.max(0, partitionCount)],
          new long[Math.max(0, partitionCount)],
          new double[Math.max(0, partitionCount)],
          new long[Math.max(0, partitionCount)],
          new long[Math.max(0, partitionCount)],
          new long[Math.max(0, partitionCount)],
          new double[Math.max(0, partitionCount)],
          new double[Math.max(0, partitionCount)],
          new double[Math.max(0, partitionCount)],
          new double[Math.max(0, partitionCount)],
          true,
          new EnvelopeNDLite[Math.max(0, partitionCount)],
          true);
    }

    public static PartitionLoadStats of(long[] featureCounts,
                                        long[] totalBytes,
                                        double[] totalComplexities) {
      return new PartitionLoadStats(featureCounts, totalBytes, totalComplexities);
    }

    public static PartitionLoadStats of(long[] featureCounts,
                                        long[] totalBytes,
                                        double[] totalComplexities,
                                        EnvelopeNDLite[] aggregateEnvelopes) {
      return new PartitionLoadStats(
          featureCounts,
          totalBytes,
          totalComplexities,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          false,
          aggregateEnvelopes,
          true);
    }

    public static PartitionLoadStats of(long[] featureCounts,
                                        long[] totalBytes,
                                        double[] totalComplexities,
                                        long[] totalVertexCounts,
                                        long[] minVertexCounts,
                                        long[] maxVertexCounts,
                                        double[] cibCornerCounts,
                                        double[] cibOverlapRatios,
                                        double[] cibHorizontalRatios,
                                        double[] cibVerticalRatios) {
      return new PartitionLoadStats(
          featureCounts,
          totalBytes,
          totalComplexities,
          totalVertexCounts,
          minVertexCounts,
          maxVertexCounts,
          cibCornerCounts,
          cibOverlapRatios,
          cibHorizontalRatios,
          cibVerticalRatios,
          true,
          null,
          false);
    }

    public static PartitionLoadStats of(long[] featureCounts,
                                        long[] totalBytes,
                                        double[] totalComplexities,
                                        long[] totalVertexCounts,
                                        long[] minVertexCounts,
                                        long[] maxVertexCounts,
                                        double[] cibCornerCounts,
                                        double[] cibOverlapRatios,
                                        double[] cibHorizontalRatios,
                                        double[] cibVerticalRatios,
                                        EnvelopeNDLite[] aggregateEnvelopes) {
      return new PartitionLoadStats(
          featureCounts,
          totalBytes,
          totalComplexities,
          totalVertexCounts,
          minVertexCounts,
          maxVertexCounts,
          cibCornerCounts,
          cibOverlapRatios,
          cibHorizontalRatios,
          cibVerticalRatios,
          true,
          aggregateEnvelopes,
          true);
    }

    public long[] getFeatureCounts() {
      return featureCounts;
    }

    public long[] getTotalBytes() {
      return totalBytes;
    }

    public double[] getTotalComplexities() {
      return totalComplexities;
    }

    public long[] getTotalVertexCounts() {
      return totalVertexCounts;
    }

    public long[] getMinVertexCounts() {
      return minVertexCounts;
    }

    public long[] getMaxVertexCounts() {
      return maxVertexCounts;
    }

    public boolean hasCibHistogramStats() {
      return cibHistogramStatsAvailable;
    }

    public double[] getCibCornerCounts() {
      return cibCornerCounts;
    }

    public double[] getCibOverlapRatios() {
      return cibOverlapRatios;
    }

    public double[] getCibHorizontalRatios() {
      return cibHorizontalRatios;
    }

    public double[] getCibVerticalRatios() {
      return cibVerticalRatios;
    }

    public boolean hasAggregateEnvelopeStats() {
      return aggregateEnvelopeStatsAvailable;
    }

    public EnvelopeNDLite[] getAggregateEnvelopes() {
      return copyEnvelopes(aggregateEnvelopes);
    }

    private static EnvelopeNDLite[] copyEnvelopes(EnvelopeNDLite[] envelopes) {
      if (envelopes == null)
        return new EnvelopeNDLite[0];
      EnvelopeNDLite[] copy = new EnvelopeNDLite[envelopes.length];
      for (int i = 0; i < envelopes.length; i++)
        copy[i] = envelopes[i] == null ? null : new EnvelopeNDLite(envelopes[i]);
      return copy;
    }
  }

  /**
   * Exact but compact raw-object statistics used only by iterative synopsis correction.
   */
  public static final class PartitionRefinementStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long[] featureCounts;
    private final long[] totalBytes;
    private final EnvelopeNDLite[] aggregateEnvelopes;
    private final long[][][] countHistograms;
    private final long[][][] byteHistograms;
    private final int histogramBinCount;

    private PartitionRefinementStats(long[] featureCounts,
                                     long[] totalBytes,
                                     EnvelopeNDLite[] aggregateEnvelopes,
                                     long[][][] countHistograms,
                                     long[][][] byteHistograms,
                                     int histogramBinCount) {
      this.featureCounts = featureCounts == null ? new long[0] : featureCounts;
      this.totalBytes = totalBytes == null ? new long[0] : totalBytes;
      this.aggregateEnvelopes = PartitionLoadStats.copyEnvelopes(aggregateEnvelopes);
      this.countHistograms = countHistograms == null ? new long[0][][] : countHistograms;
      this.byteHistograms = byteHistograms == null ? new long[0][][] : byteHistograms;
      this.histogramBinCount = Math.max(8, histogramBinCount);
    }

    public static PartitionRefinementStats empty(int partitionCount, int histogramBinCount) {
      int safePartitionCount = Math.max(0, partitionCount);
      int safeBinCount = Math.max(8, histogramBinCount);
      return new PartitionRefinementStats(
          new long[safePartitionCount],
          new long[safePartitionCount],
          new EnvelopeNDLite[safePartitionCount],
          new long[safePartitionCount][2][safeBinCount],
          new long[safePartitionCount][2][safeBinCount],
          safeBinCount);
    }

    public static PartitionRefinementStats of(long[] featureCounts,
                                              long[] totalBytes,
                                              EnvelopeNDLite[] aggregateEnvelopes,
                                              long[][][] countHistograms,
                                              long[][][] byteHistograms,
                                              int histogramBinCount) {
      return new PartitionRefinementStats(
          featureCounts,
          totalBytes,
          aggregateEnvelopes,
          countHistograms,
          byteHistograms,
          histogramBinCount);
    }

    public long[] getFeatureCounts() {
      return featureCounts;
    }

    public long[] getTotalBytes() {
      return totalBytes;
    }

    public EnvelopeNDLite[] getAggregateEnvelopes() {
      return PartitionLoadStats.copyEnvelopes(aggregateEnvelopes);
    }

    public long[] getCountHistogram(int partitionId, int axis) {
      return copyHistogramSlice(countHistograms, partitionId, axis);
    }

    public long[] getByteHistogram(int partitionId, int axis) {
      return copyHistogramSlice(byteHistograms, partitionId, axis);
    }

    public int getHistogramBinCount() {
      return histogramBinCount;
    }

    private static long[] copyHistogramSlice(long[][][] histograms,
                                             int partitionId,
                                             int axis) {
      if (histograms == null
          || partitionId < 0
          || partitionId >= histograms.length
          || histograms[partitionId] == null
          || axis < 0
          || axis >= histograms[partitionId].length
          || histograms[partitionId][axis] == null)
        return new long[0];
      return Arrays.copyOf(
          histograms[partitionId][axis],
          histograms[partitionId][axis].length);
    }
  }

  /**
   * Mutable per-partition load accumulator used only during distributed exact-statistics collection.
   */
  private static final class PartitionLoadAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private long featureCount;
    private long totalBytes;
    private long totalVertexCount;
    private long minVertexCount;
    private long maxVertexCount;
    private double totalComplexity;
    private final CibHistogramStats cibStats = new CibHistogramStats();
    private EnvelopeNDLite aggregateEnvelope;

    private void add(AbstractFeature feature, EnvelopeNDLite partitionEnvelope) {
      if (feature == null)
        return;
      long vertexCount = Math.max(0L, feature.getNumPoints());
      featureCount++;
      totalBytes += Math.max(0L, feature.getLoadWeight());
      totalVertexCount += vertexCount;
      if (featureCount == 1L || vertexCount < minVertexCount)
        minVertexCount = vertexCount;
      maxVertexCount = Math.max(maxVertexCount, vertexCount);
      totalComplexity += Math.max(0.0d, feature.getComplexityEstimate());
      cibStats.add(feature, partitionEnvelope);
      aggregateEnvelope = updateProjectedAggregateEnvelope(
          aggregateEnvelope,
          feature,
          partitionEnvelope);
    }

    private PartitionLoadAccumulator mergeInPlace(PartitionLoadAccumulator other) {
      if (other == null)
        return this;
      long previousFeatureCount = this.featureCount;
      this.featureCount += other.featureCount;
      this.totalBytes += other.totalBytes;
      this.totalVertexCount += other.totalVertexCount;
      if (other.featureCount > 0L) {
        if (previousFeatureCount <= 0L)
          this.minVertexCount = other.minVertexCount;
        else
          this.minVertexCount = Math.min(this.minVertexCount, other.minVertexCount);
        this.maxVertexCount = Math.max(this.maxVertexCount, other.maxVertexCount);
      }
      this.totalComplexity += other.totalComplexity;
      this.cibStats.mergeInPlace(other.cibStats);
      if (other.aggregateEnvelope != null && !other.aggregateEnvelope.isEmpty()) {
        if (this.aggregateEnvelope == null)
          this.aggregateEnvelope = new EnvelopeNDLite(other.aggregateEnvelope);
        else
          this.aggregateEnvelope.merge(other.aggregateEnvelope);
      }
      return this;
    }

    private long getFeatureCount() {
      return featureCount;
    }

    private long getTotalBytes() {
      return totalBytes;
    }

    private long getTotalVertexCount() {
      return totalVertexCount;
    }

    private long getMinVertexCount() {
      return minVertexCount;
    }

    private long getMaxVertexCount() {
      return maxVertexCount;
    }

    private double getTotalComplexity() {
      return totalComplexity;
    }

    private double getCibCornerCount() {
      return cibStats.getCornerCount();
    }

    private double getCibOverlapRatio() {
      return cibStats.getOverlapRatio();
    }

    private double getCibHorizontalRatio() {
      return cibStats.getHorizontalRatio();
    }

    private double getCibVerticalRatio() {
      return cibStats.getVerticalRatio();
    }

    private EnvelopeNDLite getAggregateEnvelope() {
      return aggregateEnvelope == null ? null : new EnvelopeNDLite(aggregateEnvelope);
    }
  }

  /**
   * Per-partition executor-local state for exact recursive-refinement feedback.
   */
  private static final class PartitionRefinementAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private long featureCount;
    private long totalBytes;
    private EnvelopeNDLite aggregateEnvelope;
    private final long[][] countHistograms;
    private final long[][] byteHistograms;

    private PartitionRefinementAccumulator(int histogramBinCount) {
      int safeBinCount = Math.max(8, histogramBinCount);
      this.countHistograms = new long[2][safeBinCount];
      this.byteHistograms = new long[2][safeBinCount];
    }

    private void add(AbstractFeature feature, EnvelopeNDLite partitionEnvelope) {
      if (feature == null || partitionEnvelope == null)
        return;
      long loadBytes = Math.max(0L, feature.getLoadWeight());
      featureCount++;
      totalBytes += loadBytes;
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
          double center = Math.max(min, Math.min(partitionEnvelope.getMaxCoord(axis), feature.getCenter(axis)));
          double normalized = (center - min) / length;
          bin = (int) Math.floor(normalized * countHistograms[axis].length);
          bin = Math.max(0, Math.min(countHistograms[axis].length - 1, bin));
        }
        countHistograms[axis][bin]++;
        byteHistograms[axis][bin] += loadBytes;
      }
    }

    private PartitionRefinementAccumulator mergeInPlace(
        PartitionRefinementAccumulator other) {
      if (other == null)
        return this;
      featureCount += other.featureCount;
      totalBytes += other.totalBytes;
      if (other.aggregateEnvelope != null && !other.aggregateEnvelope.isEmpty()) {
        if (aggregateEnvelope == null)
          aggregateEnvelope = new EnvelopeNDLite(other.aggregateEnvelope);
        else
          aggregateEnvelope.merge(other.aggregateEnvelope);
      }
      for (int axis = 0; axis < countHistograms.length; axis++) {
        for (int bin = 0; bin < countHistograms[axis].length; bin++) {
          countHistograms[axis][bin] += other.countHistograms[axis][bin];
          byteHistograms[axis][bin] += other.byteHistograms[axis][bin];
        }
      }
      return this;
    }
  }

  /**
   * Result bundle returned by the one-pass abstract-feature collection helper.
   *
   * <p>The prepare phase needs all three views of the same dataset side:
   * driver-side {@link AbstractFeature}s for partition construction, a dataset
   * {@link Summary} for LAHilbert sizing, and the exact feature count for debug output.
   * This wrapper keeps those views together so callers can reuse one distributed pass.
   */
  public static final class CollectedAbstractFeatures implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final CollectedAbstractFeatures EMPTY =
        new CollectedAbstractFeatures(new AbstractFeature[0], new Summary(), 0L);

    private final AbstractFeature[] abstractFeatures;
    private final Summary summary;
    private final long featureCount;

    private CollectedAbstractFeatures(AbstractFeature[] abstractFeatures, Summary summary, long featureCount) {
      this.abstractFeatures = abstractFeatures == null ? new AbstractFeature[0] : abstractFeatures;
      this.summary = summary == null ? new Summary() : summary;
      this.featureCount = Math.max(0L, featureCount);
    }

    public static CollectedAbstractFeatures empty() {
      return EMPTY;
    }

    /** Creates an immutable collection bundle from externally prepared lightweight descriptors. */
    public static CollectedAbstractFeatures of(AbstractFeature[] abstractFeatures, Summary summary, long featureCount) {
      if (abstractFeatures == null || abstractFeatures.length == 0)
        return EMPTY;
      Summary safeSummary = summary == null ? new Summary() : summary;
      return new CollectedAbstractFeatures(
          Arrays.copyOf(abstractFeatures, abstractFeatures.length),
          safeSummary,
          Math.max(0L, featureCount));
    }

    public AbstractFeature[] getAbstractFeatures() {
      return abstractFeatures;
    }

    public Summary getSummary() {
      return summary;
    }

    public long getFeatureCount() {
      return featureCount;
    }
  }

  /** Immutable bundle returned by the one-pass synopsis + summary collection helper. */
  public static final class CollectedSynopsisFeatures implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final CollectedSynopsisFeatures EMPTY =
        new CollectedSynopsisFeatures(new AbstractFeature[0], new Summary(), 0L);

    private final AbstractFeature[] synopsisFeatures;
    private final Summary summary;
    private final long featureCount;

    private CollectedSynopsisFeatures(AbstractFeature[] synopsisFeatures, Summary summary, long featureCount) {
      this.synopsisFeatures = synopsisFeatures == null ? new AbstractFeature[0] : synopsisFeatures;
      this.summary = summary == null ? new Summary() : summary;
      this.featureCount = Math.max(0L, featureCount);
    }

    public static CollectedSynopsisFeatures empty() {
      return EMPTY;
    }

    public AbstractFeature[] getSynopsisFeatures() {
      return synopsisFeatures;
    }

    public Summary getSummary() {
      return summary;
    }

    public long getFeatureCount() {
      return featureCount;
    }
  }

  /** One driver-collected partition batch used only to merge the one-pass abstract-feature scan. */
  private static final class AbstractFeaturePartitionBatch implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AbstractFeature[] abstractFeatures;
    private final Summary summary;
    private final long featureCount;

    private AbstractFeaturePartitionBatch(AbstractFeature[] abstractFeatures, Summary summary, long featureCount) {
      this.abstractFeatures = abstractFeatures == null ? new AbstractFeature[0] : abstractFeatures;
      this.summary = summary == null ? new Summary() : summary;
      this.featureCount = Math.max(0L, featureCount);
    }

    private AbstractFeature[] getAbstractFeatures() {
      return abstractFeatures;
    }

    private Summary getSummary() {
      return summary;
    }

    private long getFeatureCount() {
      return featureCount;
    }
  }

  /** One aggregated synopsis cell emitted from one Spark partition during one-pass collection. */
  private static final class SynopsisCellAggregate implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SynopsisCellKey cellKey;
    private final AbstractFeature abstractFeature;

    private SynopsisCellAggregate(SynopsisCellKey cellKey, AbstractFeature abstractFeature) {
      this.cellKey = cellKey;
      this.abstractFeature = abstractFeature;
    }

    private SynopsisCellKey getCellKey() {
      return cellKey;
    }

    private AbstractFeature getAbstractFeature() {
      return abstractFeature;
    }
  }

  /** One driver-collected batch containing local synopsis cells plus local summary/count. */
  private static final class SynopsisPartitionBatch implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SynopsisCellAggregate[] aggregates;
    private final Summary summary;
    private final long featureCount;

    private SynopsisPartitionBatch(SynopsisCellAggregate[] aggregates, Summary summary, long featureCount) {
      this.aggregates = aggregates == null ? new SynopsisCellAggregate[0] : aggregates;
      this.summary = summary == null ? new Summary() : summary;
      this.featureCount = Math.max(0L, featureCount);
    }

    private SynopsisCellAggregate[] getAggregates() {
      return aggregates;
    }

    private Summary getSummary() {
      return summary;
    }

    private long getFeatureCount() {
      return featureCount;
    }
  }

  private static int safeSidePartitions(int synopsisOrder) {
    if (synopsisOrder <= 0)
      return 1;
    if (synopsisOrder >= 30)
      return 1 << 29;
    return 1 << synopsisOrder;
  }

  private static SynopsisCellKey locateSynopsisCell(AbstractFeature feature,
                                                    EnvelopeNDLite referenceEnvelope,
                                                    int sidePartitions) {
    int coordinateDimension = referenceEnvelope.getCoordinateDimension();
    int[] coords = new int[coordinateDimension];
    for (int d = 0; d < coordinateDimension; d++) {
      coords[d] = locateSynopsisCellCoordinate(feature.getCenter(d), referenceEnvelope.getMinCoord(d), referenceEnvelope.getMaxCoord(d), sidePartitions);
    }
    return new SynopsisCellKey(coords);
  }

  private static int locateSynopsisCellCoordinate(double coordinate,
                                                  double minCoord,
                                                  double maxCoord,
                                                  int sidePartitions) {
    if (sidePartitions <= 1 || !(maxCoord > minCoord) || Double.isNaN(coordinate))
      return 0;
    double normalized = (coordinate - minCoord) / (maxCoord - minCoord);
    int cell = (int) Math.floor(normalized * sidePartitions);
    if (cell < 0)
      return 0;
    if (cell >= sidePartitions)
      return sidePartitions - 1;
    return cell;
  }

  private static long resolveFeatureLoadBytes(IFeature feature, String loadEstimateMode) {
    return feature == null ? 0L : Math.max(0L, feature.getExecutionLoadEstimateBytes(loadEstimateMode));
  }

  private static final class SynopsisCellKey implements Serializable, Comparable<SynopsisCellKey> {
    private static final long serialVersionUID = 1L;
    private final int[] coords;
    private final int hashCode;

    private SynopsisCellKey(int[] coords) {
      this.coords = coords == null ? new int[0] : Arrays.copyOf(coords, coords.length);
      this.hashCode = Arrays.hashCode(this.coords);
    }

    private String toHierarchyKey() {
      if (coords.length == 0)
        return "synopsis:root";
      StringBuilder builder = new StringBuilder("synopsis");
      for (int coord : coords)
        builder.append(':').append(coord);
      return builder.toString();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (!(other instanceof SynopsisCellKey))
        return false;
      SynopsisCellKey that = (SynopsisCellKey) other;
      return Arrays.equals(this.coords, that.coords);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public int compareTo(SynopsisCellKey other) {
      if (other == null)
        return 1;
      int commonLength = Math.min(this.coords.length, other.coords.length);
      for (int i = 0; i < commonLength; i++) {
        int compare = Integer.compare(this.coords[i], other.coords[i]);
        if (compare != 0)
          return compare;
      }
      return Integer.compare(this.coords.length, other.coords.length);
    }
  }

  /**
   * Compact OID-to-partition lookup table used only during the one-shot final working-set materialization.
   *
   * <p>The table is optimized for:
   * <ul>
   *   <li>small serialized footprint when broadcast to executors</li>
   *   <li>fast binary-search lookup by stable OID</li>
   *   <li>zero per-entry object allocation in the executor hot path</li>
   * </ul>
   */
  public static final class AssignmentLookupTable implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final AssignmentLookupTable EMPTY =
        new AssignmentLookupTable(new long[0], new int[] {0}, new int[0]);

    private final long[] oids;
    private final int[] offsets;
    private final int[] partitionIds;
    private final long estimatedSizeInBytes;

    private AssignmentLookupTable(long[] oids, int[] offsets, int[] partitionIds) {
      this.oids = oids == null ? new long[0] : oids;
      this.offsets = offsets == null ? new int[] {0} : offsets;
      this.partitionIds = partitionIds == null ? new int[0] : partitionIds;
      this.estimatedSizeInBytes =
          (long) Long.BYTES * this.oids.length
              + (long) Integer.BYTES * this.offsets.length
              + (long) Integer.BYTES * this.partitionIds.length;
    }

    public static AssignmentLookupTable empty() {
      return EMPTY;
    }

    /** Returns whether the lookup table contains any OID-to-partition mapping. */
    public boolean isEmpty() {
      return oids.length == 0 || partitionIds.length == 0;
    }

    /** Returns the estimated serialized payload size of the primitive-array table. */
    public long getEstimatedSizeInBytes() {
      return estimatedSizeInBytes;
    }

    /** Binary-searches the sorted OID table and returns its index, or {@code -1} when absent. */
    public int findOidIndex(long oid) {
      int index = Arrays.binarySearch(oids, oid);
      return index < 0 ? -1 : index;
    }

    /** Returns the inclusive start offset of the partition-id slice for one OID index. */
    public int getPartitionStartOffset(int oidIndex) {
      if (oidIndex < 0 || oidIndex >= oids.length)
        return -1;
      return offsets[oidIndex];
    }

    /** Returns the exclusive end offset of the partition-id slice for one OID index. */
    public int getPartitionEndOffset(int oidIndex) {
      if (oidIndex < 0 || oidIndex >= oids.length)
        return -1;
      return offsets[oidIndex + 1];
    }

    /** Returns one partition id by flat offset inside the compact partition-id array. */
    public int getPartitionIdAt(int partitionOffset) {
      return partitionIds[partitionOffset];
    }
  }

  public static final class EffectivePlan implements Serializable {
    private static final long serialVersionUID = 1L;
    private final SpatialPartitioner partitioner;
    private final List<PartProfile> partitionProfiles;
    private final List<FeatureRef> leftAssignments;
    private final List<FeatureRef> rightAssignments;
    private final int originalPartitionCount;
    private final int nonEmptyCandidatePartitionCount;
    private final long nonEmptyCandidateLoadBytes;
    private final int traditionalEffectivePartitionCount;
    private final int aggregateMbrPrunedPartitionCount;

    /** Creates the immutable effective-partition result after pruning one-sided partitions. */
    public EffectivePlan(SpatialPartitioner partitioner,
                         List<PartProfile> partitionProfiles,
                         List<FeatureRef> leftAssignments,
                         List<FeatureRef> rightAssignments) {
      this(
          partitioner,
          partitionProfiles,
          leftAssignments,
          rightAssignments,
          partitioner == null ? 0 : Math.max(0, partitioner.numPartitions()),
          partitionProfiles == null ? 0 : partitionProfiles.size(),
          sumProfileLoadBytes(partitionProfiles),
          partitionProfiles == null ? 0 : partitionProfiles.size(),
          0);
    }

    /** Creates the immutable effective-partition result with explicit pruning diagnostics. */
    public EffectivePlan(SpatialPartitioner partitioner,
                         List<PartProfile> partitionProfiles,
                         List<FeatureRef> leftAssignments,
                         List<FeatureRef> rightAssignments,
                         int originalPartitionCount,
                         int traditionalEffectivePartitionCount,
                         int aggregateMbrPrunedPartitionCount) {
      this(
          partitioner,
          partitionProfiles,
          leftAssignments,
          rightAssignments,
          originalPartitionCount,
          traditionalEffectivePartitionCount,
          sumProfileLoadBytes(partitionProfiles),
          traditionalEffectivePartitionCount,
          aggregateMbrPrunedPartitionCount);
    }

    /** Creates the immutable effective-partition result with explicit pruning diagnostics. */
    public EffectivePlan(SpatialPartitioner partitioner,
                         List<PartProfile> partitionProfiles,
                         List<FeatureRef> leftAssignments,
                         List<FeatureRef> rightAssignments,
                         int originalPartitionCount,
                         int nonEmptyCandidatePartitionCount,
                         long nonEmptyCandidateLoadBytes,
                         int traditionalEffectivePartitionCount,
                         int aggregateMbrPrunedPartitionCount) {
      this.partitioner = partitioner;
      this.partitionProfiles = immutableCopy(partitionProfiles);
      this.leftAssignments = immutableCopy(leftAssignments);
      this.rightAssignments = immutableCopy(rightAssignments);
      this.originalPartitionCount = Math.max(0, originalPartitionCount);
      this.nonEmptyCandidatePartitionCount = Math.max(0, nonEmptyCandidatePartitionCount);
      this.nonEmptyCandidateLoadBytes = Math.max(0L, nonEmptyCandidateLoadBytes);
      this.traditionalEffectivePartitionCount = Math.max(0, traditionalEffectivePartitionCount);
      this.aggregateMbrPrunedPartitionCount = Math.max(0, aggregateMbrPrunedPartitionCount);
    }

    /** Returns the already-pruned partitioner that contains only effective partitions. */
    public SpatialPartitioner getPartitioner() {
      return partitioner;
    }

    /** Returns one load descriptor per effective partition. */
    public List<PartProfile> getPartitionProfiles() {
      return partitionProfiles;
    }

    /** @deprecated Use {@link #getPartitionProfiles()} for the effective-partition profile view. */
    @Deprecated
    public List<PartProfile> getPartitionLoads() {
      return getPartitionProfiles();
    }

    /** Returns the compact left-side OID-to-partition assignments. */
    public List<FeatureRef> getLeftAssignments() {
      return leftAssignments;
    }

    /** Returns the compact right-side OID-to-partition assignments. */
    public List<FeatureRef> getRightAssignments() {
      return rightAssignments;
    }

    /** Returns the candidate partition count before effective-partition pruning. */
    public int getOriginalPartitionCount() {
      return originalPartitionCount;
    }

    /** Returns the candidate partitions that receive at least one side before final pruning. */
    public int getNonEmptyCandidatePartitionCount() {
      return nonEmptyCandidatePartitionCount;
    }

    /** Returns total left+right load bytes for non-empty candidate partitions before final pruning. */
    public long getNonEmptyCandidateLoadBytes() {
      return nonEmptyCandidateLoadBytes;
    }

    /** Returns the count-only effective partition count before aggregate-MBR pruning. */
    public int getTraditionalEffectivePartitionCount() {
      return traditionalEffectivePartitionCount;
    }

    /** Returns how many count-effective partitions were removed by aggregate-MBR pruning. */
    public int getAggregateMbrPrunedPartitionCount() {
      return aggregateMbrPrunedPartitionCount;
    }

    /** Returns true when no effective partitions remain after pruning. */
    public boolean isEmpty() {
      return partitionProfiles.isEmpty();
    }

    private static <T> List<T> immutableCopy(List<T> values) {
      if (values == null || values.isEmpty())
        return Collections.emptyList();
      return Collections.unmodifiableList(new ArrayList<>(values));
    }

    private static long sumProfileLoadBytes(List<PartProfile> profiles) {
      if (profiles == null || profiles.isEmpty())
        return 0L;
      long total = 0L;
      for (PartProfile profile : profiles) {
        if (profile == null)
          continue;
        total = saturatedAdd(total, profile.getLeftBytes());
        total = saturatedAdd(total, profile.getRightBytes());
      }
      return total;
    }
  }

  /**
   * Find all the overlapping partitions in an envelope (MBR).
   * @param mbr the MBR to find overlapping partitions
   * @param matchedPartitions an array to use for matched partitions
   */
  public abstract void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions);

  /** Convenience overload that accepts the mutable ND envelope wrapper. */
  public void overlapPartitions(EnvelopeND mbr, IntArray matchedPartitions) {
    overlapPartitions(new EnvelopeNDLite(mbr), matchedPartitions);
  }
  
  /**
   * Returns the single partition that contains the center of the given envelope.
   * This method should return a value in the range [0, {@link #numPartitions()}[
   *
   * @param mbr the minimum bounding rectangle to find a partition
   * @return the ID of the best partition that the given MBR should be assigned to.
   */
  public int overlapPartition(EnvelopeND mbr) {
    return overlapPartition(new EnvelopeNDLite(mbr));
  }

  /**
   * Resolves a Spark partition key from several supported spatial key shapes.
   *
   * Spark's `Partitioner` API routes generic `Object` keys, so this helper bridges
   * the partitioner with integer partition ids, full features, raw JTS geometries,
   * and lightweight envelopes.
   */
  public int getPartition(Object key) {
    if (key instanceof Integer)
      return (Integer) key;
    if (key instanceof IFeature)
      return overlapPartition(new EnvelopeNDLite().merge(((IFeature)key).getGeometry()));
    if (key instanceof Geometry)
      return overlapPartition(new EnvelopeNDLite().merge((Geometry)key));
    if (key instanceof EnvelopeNDLite)
      return overlapPartition((EnvelopeNDLite) key);
    else
      throw new RuntimeException("Unsupported key type "+key.getClass());
  }

  public abstract int overlapPartition(EnvelopeNDLite mbr);
  
  /**
   * Returns the minimum bounding rectangle (MBR) of the given partition.
   * @param partitionID the ID of the partition to get its MBR.
   * @param mbr output parameter for the MBR to avoid allocation of objects
   */
  public abstract void getPartitionMBR(int partitionID, EnvelopeNDLite mbr);

  /** Convenience overload that allocates and returns the partition envelope. */
  public EnvelopeNDLite getPartitionMBR(int partitionID) {
    EnvelopeNDLite mbr = new EnvelopeNDLite(this.getCoordinateDimension());
    getPartitionMBR(partitionID, mbr);
    return mbr;
  }
  
  /**
   * Returns total number of partitions
   * @return the total number of partitions
   */
  public abstract int numPartitions();

  /**
   * Whether this partitioner is configured to produce disjoint partitions
   * @return {@code true} if this partitions will produce spatially disjoint partitions
   */
  public abstract boolean isDisjoint();

  /**
   * Number of dimensions for this partitioner
   * @return the dimensions of the partitioner
   */
  public abstract int getCoordinateDimension();

  /** Returns the static metadata annotation declared on the concrete partitioner class. */
  public Metadata getMetadata() {
    return this.getClass().getAnnotation(Metadata.class);
  }

  /**
   * Returns the MBR of the underlying dataset on which the partitioner was created.
   * @return the envelope of the entire partitioner
   */
  public abstract EnvelopeNDLite getEnvelope();

  /** Iterates over partition envelopes in partition-id order. */
  @Override
  public Iterator<EnvelopeNDLite> iterator() {
    return new EnvelopeIterator(this);
  }

  /**
   * Print all the partition boundaries to a tab-separated file.
   * @param out the output to write to
   */
  public void toWKT(PrintStream out) {
    out.println("PartitionID\tMBR");
    EnvelopeNDLite mbr = new EnvelopeNDLite(this.getCoordinateDimension());
    for (int i = 0; i < numPartitions(); i++) {
      out.print(i);
      out.print('\t');
      this.getPartitionMBR(i, mbr);
      out.println(mbr.toWKT(new StringBuilder()).toString());
    }
  }

  /**
   * A class to iterate over all partitions
   */
  public class EnvelopeIterator implements Iterator<EnvelopeNDLite> {
    /**The spatial partitioner that this iterate goes over*/
    private final SpatialPartitioner spatialPartitioner;

    /**The partitioner that will be returned when next is called*/
    private int iPartition = 0;

    /**The partition that is returned when next is called*/
    private final EnvelopeNDLite partitionMBR = new EnvelopeNDLite();

    /** Creates an iterator over all partition envelopes of the given partitioner. */
    public EnvelopeIterator(SpatialPartitioner p) {
      this.spatialPartitioner = p;
    }

    /** Returns whether another partition envelope is still available. */
    @Override
    public boolean hasNext() {
      return iPartition < spatialPartitioner.numPartitions();
    }

    /** Materializes the next partition envelope into a reusable object. */
    @Override
    public EnvelopeNDLite next() {
      spatialPartitioner.getPartitionMBR(iPartition, partitionMBR);
      iPartition++;
      return partitionMBR;
    }
  }
}
