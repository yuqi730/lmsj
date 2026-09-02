package cn.edu.whu.spatialJoin.core.partitioning.helper;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.core.partitioning.SpatialPartitioner;
import cn.edu.whu.spatialJoin.cost.PartProfile;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.utils.IntArray;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds strategy-specific effective partition assignments on top of the already constructed
 * resource-aware partition layout.
 *
 * <p>The current project keeps one important invariant:
 * the geometric partition boundaries, load-aware split decisions, and dynamic refinement logic
 * still come from the existing dual-replication LAHilbert construction path. This helper only
 * rewrites the final feature-to-partition assignments after that partition layout has already
 * been decided.
 *
 * <p>That separation keeps the current resource-aware partitioning logic stable while still
 * allowing experiments with different assignment semantics:
 * <ul>
 *   <li>{@code dual-replication}: current project behavior, both sides replicate to every
 *       intersecting final effective partition.</li>
 *   <li>{@code anchor-replication-filter}: one heavier side still uses the current replication
 *       behavior, but the other side is filtered by aggregate anchor-side MBRs before assignment.</li>
 * </ul>
 *
 * <p>All strategies returned by this helper produce a fresh immutable
 * {@link SpatialPartitioner.EffectivePlan}. The returned plan can therefore be used by the
 * execution layer without mutating the baseline dual-replication plan that was originally
 * created by the partitioner.
 */
public final class PartitionAssignmentStrategyHelper {

  /** Marker returned when no anchor side is needed, i.e. for the current dual-replication mode. */
  public static final String ANCHOR_SIDE_NONE = "none";
  /** Marker returned when the left input is chosen as the anchor side. */
  public static final String ANCHOR_SIDE_LEFT = "left";
  /** Marker returned when the right input is chosen as the anchor side. */
  public static final String ANCHOR_SIDE_RIGHT = "right";

  private PartitionAssignmentStrategyHelper() {
  }

  /**
   * Immutable result of applying one assignment strategy to an already constructed partition layout.
   *
   * <p>The result exposes:
   * <ul>
   *   <li>the normalized strategy name that actually took effect,</li>
   *   <li>the selected anchor side when the strategy needs one, and</li>
   *   <li>the final effective plan consumed by later job planning and execution.</li>
   * </ul>
   */
  public static final class StrategyPlanResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String strategy;
    private final String anchorSide;
    private final SpatialPartitioner.EffectivePlan effectivePlan;

    public StrategyPlanResult(String strategy,
                              String anchorSide,
                              SpatialPartitioner.EffectivePlan effectivePlan) {
      this.strategy = strategy == null
          ? SpatialJoinConstants.Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY
          : strategy;
      this.anchorSide = anchorSide == null ? ANCHOR_SIDE_NONE : anchorSide;
      this.effectivePlan = effectivePlan;
    }

    /** Returns the normalized strategy name that was actually applied. */
    public String getStrategy() {
      return strategy;
    }

    /** Returns which side was chosen as anchor, or {@value #ANCHOR_SIDE_NONE} when not needed. */
    public String getAnchorSide() {
      return anchorSide;
    }

    /** Returns the strategy-specific effective partition plan used by later execution stages. */
    public SpatialPartitioner.EffectivePlan getEffectivePlan() {
      return effectivePlan;
    }
  }

  /**
   * Applies one of the supported assignment strategies to the current final partition layout.
   *
   * <p>The input {@code basePlan} is expected to be the current project's baseline
   * dual-replication effective plan. The helper then rewrites only the assignments and
   * per-partition execution profiles while preserving the already chosen final partition
   * boundaries and refinement structure.
   *
   * @param basePlan baseline dual-replication effective plan whose partition layout should be reused.
   * @param leftFeatures full left-side lightweight features. They provide envelopes, OIDs, byte load,
   *                     and complexity for the later strategy-specific remapping.
   * @param rightFeatures full right-side lightweight features. They provide envelopes, OIDs, byte load,
   *                      and complexity for the later strategy-specific remapping.
   * @param configuredStrategy user-provided strategy option. Null/blank/unknown values fall back to
   *                           {@code dual-replication}.
   * @return one immutable strategy result carrying the normalized strategy, the chosen anchor side,
   *         and the remapped effective plan.
   */
  public static StrategyPlanResult applyStrategy(SpatialPartitioner.EffectivePlan basePlan,
                                                 AbstractFeature[] leftFeatures,
                                                 AbstractFeature[] rightFeatures,
                                                 String configuredStrategy) {
    String strategy = normalizeStrategy(configuredStrategy);
    if (basePlan == null) {
      return new StrategyPlanResult(strategy, ANCHOR_SIDE_NONE, null);
    }
    if (SpatialJoinConstants.Execution.PARTITION_ASSIGNMENT_STRATEGY_DUAL_REPLICATION.equals(strategy)) {
      return new StrategyPlanResult(strategy, ANCHOR_SIDE_NONE, basePlan);
    }
    String anchorSide = chooseAnchorSide(leftFeatures, rightFeatures);
    SpatialPartitioner.EffectivePlan plan =
        buildAnchorReplicationFilterPlan(basePlan, leftFeatures, rightFeatures, anchorSide);
    return new StrategyPlanResult(strategy, anchorSide, plan);
  }

  /**
   * Normalizes a user-facing strategy string to one of the supported canonical values.
   *
   * <p>The helper accepts null/blank/unknown values and maps them to the project's default
   * strategy so callers can safely pass raw option values without pre-validation.
   *
   * @param configuredStrategy raw strategy string from options or debug input.
   * @return one of the canonical values defined in {@link SpatialJoinConstants.Execution}.
   */
  public static String normalizeStrategy(String configuredStrategy) {
    if (configuredStrategy == null || configuredStrategy.trim().isEmpty()) {
      return SpatialJoinConstants.Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY;
    }
    String normalized = configuredStrategy.trim().toLowerCase();
    if (SpatialJoinConstants.Execution.PARTITION_ASSIGNMENT_STRATEGY_ANCHOR_REPLICATION_FILTER.equals(normalized)) {
      return normalized;
    }
    if (SpatialJoinConstants.Execution.PARTITION_ASSIGNMENT_STRATEGY_DUAL_REPLICATION.equals(normalized)) {
      return normalized;
    }
    return SpatialJoinConstants.Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY;
  }

  /**
   * Chooses which side should act as the anchor side for asymmetric assignment strategies.
   *
   * <p>The current heuristic intentionally uses only stable metadata that already exists on
   * {@link AbstractFeature}:
   * <ol>
   *   <li>prefer the side with larger total memory load,</li>
   *   <li>break ties by feature count,</li>
   *   <li>break remaining ties in favor of the left side for deterministic behavior.</li>
   * </ol>
   *
   * @param leftFeatures left-side lightweight features.
   * @param rightFeatures right-side lightweight features.
   * @return {@value #ANCHOR_SIDE_LEFT} or {@value #ANCHOR_SIDE_RIGHT}.
   */
  public static String chooseAnchorSide(AbstractFeature[] leftFeatures,
                                        AbstractFeature[] rightFeatures) {
    long leftBytes = totalLoad(leftFeatures);
    long rightBytes = totalLoad(rightFeatures);
    if (leftBytes > rightBytes) {
      return ANCHOR_SIDE_LEFT;
    }
    if (rightBytes > leftBytes) {
      return ANCHOR_SIDE_RIGHT;
    }
    int leftCount = leftFeatures == null ? 0 : leftFeatures.length;
    int rightCount = rightFeatures == null ? 0 : rightFeatures.length;
    return leftCount >= rightCount ? ANCHOR_SIDE_LEFT : ANCHOR_SIDE_RIGHT;
  }

  /**
   * Builds the anchor-replication-filter strategy on top of the current final partition layout.
   *
   * <p>The anchor side keeps the current project's dual-replication semantics: every feature is assigned
   * to every final partition whose envelope intersects the feature MBR. The difference is applied only to
   * the opposite side:
   * <ul>
   *   <li>while assigning the anchor side, each partition accumulates the union of the anchor features'
   *       projected MBRs inside that partition,</li>
   *   <li>the other side is then assigned only to partitions where its MBR intersects that aggregate
   *       anchor projection.</li>
   * </ul>
   *
   * <p>This strategy keeps the current duplicate-avoidance rule intact because anchor features still
   * replicate to every intersecting partition just like the baseline dual-replication path.
   */
  private static SpatialPartitioner.EffectivePlan buildAnchorReplicationFilterPlan(SpatialPartitioner.EffectivePlan basePlan,
                                                                                   AbstractFeature[] leftFeatures,
                                                                                   AbstractFeature[] rightFeatures,
                                                                                   String anchorSide) {
    SpatialPartitioner partitioner = basePlan.getPartitioner();
    int partitionCount = partitioner == null ? 0 : partitioner.numPartitions();
    long[] leftCounts = new long[partitionCount];
    long[] rightCounts = new long[partitionCount];
    long[] leftBytes = new long[partitionCount];
    long[] rightBytes = new long[partitionCount];
    double[] leftComplexities = new double[partitionCount];
    double[] rightComplexities = new double[partitionCount];
    long[] leftVertexCounts = new long[partitionCount];
    long[] rightVertexCounts = new long[partitionCount];
    long[] leftMinVertexCounts = new long[partitionCount];
    long[] rightMinVertexCounts = new long[partitionCount];
    long[] leftMaxVertexCounts = new long[partitionCount];
    long[] rightMaxVertexCounts = new long[partitionCount];
    List<SpatialPartitioner.FeatureRef> leftAssignments = new ArrayList<>();
    List<SpatialPartitioner.FeatureRef> rightAssignments = new ArrayList<>();
    EnvelopeNDLite[] anchorAggregates = new EnvelopeNDLite[partitionCount];

    if (ANCHOR_SIDE_LEFT.equals(anchorSide)) {
      assignReplicatedAnchorFeatures(
          leftFeatures, partitioner, leftAssignments, leftCounts, leftBytes, leftComplexities,
          leftVertexCounts, leftMinVertexCounts, leftMaxVertexCounts, anchorAggregates);
      assignSelectiveFeatures(
          rightFeatures, partitioner, rightAssignments, rightCounts, rightBytes, rightComplexities,
          rightVertexCounts, rightMinVertexCounts, rightMaxVertexCounts, anchorAggregates);
    } else {
      assignReplicatedAnchorFeatures(
          rightFeatures, partitioner, rightAssignments, rightCounts, rightBytes, rightComplexities,
          rightVertexCounts, rightMinVertexCounts, rightMaxVertexCounts, anchorAggregates);
      assignSelectiveFeatures(
          leftFeatures, partitioner, leftAssignments, leftCounts, leftBytes, leftComplexities,
          leftVertexCounts, leftMinVertexCounts, leftMaxVertexCounts, anchorAggregates);
    }
    return finalizePlan(basePlan, leftCounts, rightCounts, leftBytes, rightBytes, leftComplexities, rightComplexities,
        leftVertexCounts, rightVertexCounts, leftMinVertexCounts, rightMinVertexCounts, leftMaxVertexCounts, rightMaxVertexCounts,
        leftAssignments, rightAssignments);
  }

  /**
   * Assigns the anchor side in anchor-replication-filter mode.
   *
   * <p>The anchor side here keeps the current project's replication behavior:
   * a feature is assigned to every final partition whose envelope intersects the feature MBR. The aggregate
   * MBR recorded for each partition is the projected overlap between the feature MBR and the partition MBR,
   * which makes the later selective filtering of the other side tighter than a full-MBR union.
   *
   * @param features anchor-side lightweight features.
   * @param partitioner final partitioner reused from the baseline plan.
   * @param assignments output assignment list receiving one entry per intersecting partition.
   * @param counts output per-partition feature counts for the current side.
   * @param bytes output per-partition byte loads for the current side.
   * @param complexities output per-partition complexity sums for the current side.
   * @param aggregateEnvelopes mutable per-partition aggregate MBRs updated with the projected feature envelopes.
   */
  private static void assignReplicatedAnchorFeatures(AbstractFeature[] features,
                                                     SpatialPartitioner partitioner,
                                                     List<SpatialPartitioner.FeatureRef> assignments,
                                                     long[] counts,
                                                     long[] bytes,
                                                     double[] complexities,
                                                     long[] vertexCounts,
                                                     long[] minVertexCounts,
                                                     long[] maxVertexCounts,
                                                     EnvelopeNDLite[] aggregateEnvelopes) {
    if (features == null || partitioner == null) {
      return;
    }
    IntArray matchedPartitions = new IntArray();
    for (int featureIndex = 0; featureIndex < features.length; featureIndex++) {
      AbstractFeature feature = features[featureIndex];
      if (!isUsableFeature(feature)) {
        continue;
      }
      partitioner.overlapPartitions(feature, matchedPartitions);
      for (int i = 0; i < matchedPartitions.size(); i++) {
        int partitionId = matchedPartitions.get(i);
        appendAssignment(feature, partitionId, assignments, counts, bytes, complexities, vertexCounts, minVertexCounts, maxVertexCounts);
        EnvelopeNDLite projectedEnvelope = feature.intersectionEnvelope(partitioner.getPartitionMBR(partitionId));
        if (projectedEnvelope != null && !projectedEnvelope.isEmpty()) {
          mergeAggregateEnvelope(aggregateEnvelopes, partitionId, projectedEnvelope);
        }
      }
    }
  }

  /**
   * Selectively assigns the non-anchor side based on precomputed anchor aggregate MBRs.
   *
   * <p>The method starts from the current final partition layout and enumerates only those partitions whose
   * envelopes intersect the feature MBR. From that candidate set it keeps only partitions whose anchor-side
   * aggregate MBR is non-empty and intersects the feature MBR. Features never get projected or clipped here;
   * the later local join still receives the full original feature.
   *
   * @param features non-anchor-side lightweight features.
   * @param partitioner final partitioner reused from the baseline plan.
   * @param assignments output assignment list receiving one entry per surviving partition.
   * @param counts output per-partition feature counts for the current side.
   * @param bytes output per-partition byte loads for the current side.
   * @param complexities output per-partition complexity sums for the current side.
   * @param aggregateEnvelopes anchor-side aggregate MBRs built earlier for each partition.
   */
  private static void assignSelectiveFeatures(AbstractFeature[] features,
                                              SpatialPartitioner partitioner,
                                              List<SpatialPartitioner.FeatureRef> assignments,
                                              long[] counts,
                                              long[] bytes,
                                              double[] complexities,
                                              long[] vertexCounts,
                                              long[] minVertexCounts,
                                              long[] maxVertexCounts,
                                              EnvelopeNDLite[] aggregateEnvelopes) {
    if (features == null || partitioner == null || aggregateEnvelopes == null) {
      return;
    }
    IntArray matchedPartitions = new IntArray();
    for (int featureIndex = 0; featureIndex < features.length; featureIndex++) {
      AbstractFeature feature = features[featureIndex];
      if (!isUsableFeature(feature)) {
        continue;
      }
      partitioner.overlapPartitions(feature, matchedPartitions);
      for (int i = 0; i < matchedPartitions.size(); i++) {
        int partitionId = matchedPartitions.get(i);
        EnvelopeNDLite aggregateEnvelope = aggregateEnvelopes[partitionId];
        if (aggregateEnvelope == null || aggregateEnvelope.isEmpty() || !aggregateEnvelope.intersectsEnvelopeClosed(feature)) {
          continue;
        }
        appendAssignment(feature, partitionId, assignments, counts, bytes, complexities, vertexCounts, minVertexCounts, maxVertexCounts);
      }
    }
  }

  /**
   * Turns strategy-specific raw assignment arrays into a dense effective plan.
   *
   * <p>The method performs four finalization steps:
   * <ol>
   *   <li>collect the partitions that still receive both sides after the strategy-specific remapping,</li>
   *   <li>clone the baseline partitioner so pruning does not mutate the original baseline plan,</li>
   *   <li>retain only the surviving partitions on that clone and remap assignments to dense ids, and</li>
   *   <li>build the final {@link PartProfile} list used by later grouping and job planning.</li>
   * </ol>
   */
  private static SpatialPartitioner.EffectivePlan finalizePlan(SpatialPartitioner.EffectivePlan basePlan,
                                                               long[] leftCounts,
                                                               long[] rightCounts,
                                                               long[] leftBytes,
                                                               long[] rightBytes,
                                                               double[] leftComplexities,
                                                               double[] rightComplexities,
                                                               long[] leftVertexCounts,
                                                               long[] rightVertexCounts,
                                                               long[] leftMinVertexCounts,
                                                               long[] rightMinVertexCounts,
                                                               long[] leftMaxVertexCounts,
                                                               long[] rightMaxVertexCounts,
                                                               List<SpatialPartitioner.FeatureRef> leftAssignments,
                                                               List<SpatialPartitioner.FeatureRef> rightAssignments) {
    int[] retainedPartitionIds = collectRetainedPartitionIds(leftCounts, rightCounts);
    SpatialPartitioner copiedPartitioner = copyPartitioner(basePlan.getPartitioner());
    int[] oldToNewPartitionIds = copiedPartitioner.retainPartitions(retainedPartitionIds);
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
    return new SpatialPartitioner.EffectivePlan(
        copiedPartitioner,
        PartProfile.fromPartitioner(
            copiedPartitioner,
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
            null),
        remapAssignments(leftAssignments, oldToNewPartitionIds),
        remapAssignments(rightAssignments, oldToNewPartitionIds));
  }

  /** Appends one strategy-specific assignment and updates the receiving partition statistics. */
  private static void appendAssignment(AbstractFeature feature,
                                       int partitionId,
                                       List<SpatialPartitioner.FeatureRef> assignments,
                                       long[] counts,
                                       long[] bytes,
                                       double[] complexities,
                                       long[] vertexCounts,
                                       long[] minVertexCounts,
                                       long[] maxVertexCounts) {
    assignments.add(new SpatialPartitioner.FeatureRef(feature.getOid(), partitionId));
    counts[partitionId]++;
    bytes[partitionId] += Math.max(0L, feature.getLoadWeight());
    complexities[partitionId] += Math.max(0.0d, feature.getComplexityEstimate());
    long vertexCount = Math.max(0L, feature.getNumPoints());
    vertexCounts[partitionId] += vertexCount;
    if (counts[partitionId] == 1L || vertexCount < minVertexCounts[partitionId]) {
      minVertexCounts[partitionId] = vertexCount;
    }
    maxVertexCounts[partitionId] = Math.max(maxVertexCounts[partitionId], vertexCount);
  }

  /** Merges one feature or projected feature envelope into the aggregate MBR of a partition. */
  private static void mergeAggregateEnvelope(EnvelopeNDLite[] aggregateEnvelopes,
                                             int partitionId,
                                             EnvelopeNDLite contribution) {
    if (aggregateEnvelopes == null || contribution == null || contribution.isEmpty()) {
      return;
    }
    if (aggregateEnvelopes[partitionId] == null || aggregateEnvelopes[partitionId].isEmpty()) {
      aggregateEnvelopes[partitionId] = new EnvelopeNDLite(contribution);
    } else {
      aggregateEnvelopes[partitionId].merge(contribution);
    }
  }

  /** Returns true when the feature can participate in assignment and later local join execution. */
  private static boolean isUsableFeature(AbstractFeature feature) {
    return feature != null && !feature.isEmpty();
  }

  /** Computes the total memory-load proxy of one side to drive anchor-side selection. */
  private static long totalLoad(AbstractFeature[] features) {
    long total = 0L;
    if (features == null) {
      return total;
    }
    for (AbstractFeature feature : features) {
      if (feature != null) {
        total += Math.max(0L, feature.getLoadWeight());
      }
    }
    return total;
  }

  /** Collects the dense partition ids that remain effective after strategy-specific remapping. */
  private static int[] collectRetainedPartitionIds(long[] leftCounts, long[] rightCounts) {
    IntArray retained = new IntArray();
    int partitionCount = Math.min(leftCounts == null ? 0 : leftCounts.length, rightCounts == null ? 0 : rightCounts.length);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      if (leftCounts[partitionId] > 0L && rightCounts[partitionId] > 0L) {
        retained.add(partitionId);
      }
    }
    return retained.toArray();
  }

  /** Remaps one per-partition long array after the partitioner retains only surviving partitions. */
  private static long[] remapLoads(long[] values, int[] oldToNewPartitionIds) {
    if (values == null || oldToNewPartitionIds == null || oldToNewPartitionIds.length == 0) {
      return new long[0];
    }
    int newPartitionCount = Arrays.stream(oldToNewPartitionIds).max().orElse(-1) + 1;
    long[] remapped = new long[Math.max(0, newPartitionCount)];
    for (int oldPartitionId = 0; oldPartitionId < Math.min(values.length, oldToNewPartitionIds.length); oldPartitionId++) {
      int newPartitionId = oldToNewPartitionIds[oldPartitionId];
      if (newPartitionId >= 0) {
        remapped[newPartitionId] = values[oldPartitionId];
      }
    }
    return remapped;
  }

  /** Remaps one per-partition double array after the partitioner retains only surviving partitions. */
  private static double[] remapLoads(double[] values, int[] oldToNewPartitionIds) {
    if (values == null || oldToNewPartitionIds == null || oldToNewPartitionIds.length == 0) {
      return new double[0];
    }
    int newPartitionCount = Arrays.stream(oldToNewPartitionIds).max().orElse(-1) + 1;
    double[] remapped = new double[Math.max(0, newPartitionCount)];
    for (int oldPartitionId = 0; oldPartitionId < Math.min(values.length, oldToNewPartitionIds.length); oldPartitionId++) {
      int newPartitionId = oldToNewPartitionIds[oldPartitionId];
      if (newPartitionId >= 0) {
        remapped[newPartitionId] = values[oldPartitionId];
      }
    }
    return remapped;
  }

  /** Remaps compact OID-to-partition assignments after the partition ids become dense again. */
  private static List<SpatialPartitioner.FeatureRef> remapAssignments(List<SpatialPartitioner.FeatureRef> assignments,
                                                                      int[] oldToNewPartitionIds) {
    if (assignments == null || assignments.isEmpty() || oldToNewPartitionIds == null || oldToNewPartitionIds.length == 0) {
      return new ArrayList<>();
    }
    List<SpatialPartitioner.FeatureRef> remapped = new ArrayList<>(assignments.size());
    for (SpatialPartitioner.FeatureRef assignment : assignments) {
      if (assignment == null) {
        continue;
      }
      int oldPartitionId = assignment.getPartitionId();
      if (oldPartitionId < 0 || oldPartitionId >= oldToNewPartitionIds.length) {
        continue;
      }
      int newPartitionId = oldToNewPartitionIds[oldPartitionId];
      if (newPartitionId >= 0) {
        remapped.add(new SpatialPartitioner.FeatureRef(assignment.getOid(), newPartitionId));
      }
    }
    return remapped;
  }

  /**
   * Creates a deep copy of the partitioner before strategy-specific pruning mutates its final partition set.
   *
   * <p>The baseline effective plan must remain reusable for comparisons and later fallback behavior, so the
   * helper clones the partitioner by Java serialization before calling {@link SpatialPartitioner#retainPartitions(int[])}
   * on the copy.
   */
  private static SpatialPartitioner copyPartitioner(SpatialPartitioner partitioner) {
    if (partitioner == null) {
      return null;
    }
    try {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      try (ObjectOutputStream outputStream = new ObjectOutputStream(byteStream)) {
        outputStream.writeObject(partitioner);
      }
      try (ObjectInputStream inputStream =
               new ObjectInputStream(new ByteArrayInputStream(byteStream.toByteArray()))) {
        return (SpatialPartitioner) inputStream.readObject();
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Failed to clone partitioner for strategy-specific assignment planning", e);
    }
  }
}
