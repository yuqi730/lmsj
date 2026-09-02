package cn.edu.whu.spatialJoin.cost;

import cn.edu.whu.spatialJoin.core.partitioning.LAHilbertPartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.ExplicitPartitionListPartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.SpatialPartitioner;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight per-partition execution profile used by later grouping and job planning.
 */
public final class PartProfile implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int partitionId;
  private final String partitionCode;
  private final EnvelopeNDLite envelope;
  private final long leftFeatureCount;
  private final long rightFeatureCount;
  private final long leftBytes;
  private final long rightBytes;
  private final double leftComplexity;
  private final double rightComplexity;
  private final long leftVertexCount;
  private final long rightVertexCount;
  private final long leftMinVertexCount;
  private final long rightMinVertexCount;
  private final long leftMaxVertexCount;
  private final long rightMaxVertexCount;
  private final long candidatePairCount;
  private final long cibCandidatePairCount;
  private final double partitionIntensity;

  public PartProfile(int partitionId,
                     String partitionCode,
                     EnvelopeNDLite envelope,
                     long leftFeatureCount,
                     long rightFeatureCount,
                     long leftBytes,
                     long rightBytes) {
    this(partitionId, partitionCode, envelope, leftFeatureCount, rightFeatureCount, leftBytes, rightBytes, 0.0d, 0.0d);
  }

  public PartProfile(int partitionId,
                     String partitionCode,
                     EnvelopeNDLite envelope,
                     long leftFeatureCount,
                     long rightFeatureCount,
                     long leftBytes,
                     long rightBytes,
                     double leftComplexity,
                     double rightComplexity) {
    this(partitionId, partitionCode, envelope, leftFeatureCount, rightFeatureCount, leftBytes, rightBytes,
        leftComplexity, rightComplexity,
        saturatedProduct(leftFeatureCount, rightFeatureCount),
        saturatedProduct(leftFeatureCount, rightFeatureCount),
        Math.max(0.0d, leftComplexity) + Math.max(0.0d, rightComplexity));
  }

  public PartProfile(int partitionId,
                     String partitionCode,
                     EnvelopeNDLite envelope,
                     long leftFeatureCount,
                     long rightFeatureCount,
                     long leftBytes,
                     long rightBytes,
                     double leftComplexity,
                     double rightComplexity,
                     long leftVertexCount,
                     long rightVertexCount,
                     long leftMinVertexCount,
                     long rightMinVertexCount,
                     long leftMaxVertexCount,
                     long rightMaxVertexCount) {
    this(partitionId, partitionCode, envelope, leftFeatureCount, rightFeatureCount, leftBytes, rightBytes,
        leftComplexity, rightComplexity,
        leftVertexCount, rightVertexCount,
        leftMinVertexCount, rightMinVertexCount,
        leftMaxVertexCount, rightMaxVertexCount,
        saturatedProduct(leftFeatureCount, rightFeatureCount),
        saturatedProduct(leftFeatureCount, rightFeatureCount),
        Math.max(0.0d, leftComplexity) + Math.max(0.0d, rightComplexity));
  }

  private PartProfile(int partitionId,
                      String partitionCode,
                      EnvelopeNDLite envelope,
                      long leftFeatureCount,
                      long rightFeatureCount,
                     long leftBytes,
                     long rightBytes,
                     double leftComplexity,
                     double rightComplexity,
                     long candidatePairCount,
                     long cibCandidatePairCount,
                     double partitionIntensity) {
    this(partitionId, partitionCode, envelope, leftFeatureCount, rightFeatureCount, leftBytes, rightBytes,
        leftComplexity, rightComplexity,
        0L, 0L, 0L, 0L, 0L, 0L,
        candidatePairCount, cibCandidatePairCount, partitionIntensity);
  }

  private PartProfile(int partitionId,
                      String partitionCode,
                      EnvelopeNDLite envelope,
                      long leftFeatureCount,
                      long rightFeatureCount,
                      long leftBytes,
                      long rightBytes,
                      double leftComplexity,
                      double rightComplexity,
                      long leftVertexCount,
                      long rightVertexCount,
                      long leftMinVertexCount,
                      long rightMinVertexCount,
                      long leftMaxVertexCount,
                      long rightMaxVertexCount,
                      long candidatePairCount,
                      long cibCandidatePairCount,
                      double partitionIntensity) {
    this.partitionId = partitionId;
    this.partitionCode = partitionCode == null ? Integer.toString(partitionId) : partitionCode;
    this.envelope = envelope == null ? new EnvelopeNDLite(0) : new EnvelopeNDLite(envelope);
    this.leftFeatureCount = Math.max(0L, leftFeatureCount);
    this.rightFeatureCount = Math.max(0L, rightFeatureCount);
    this.leftBytes = Math.max(0L, leftBytes);
    this.rightBytes = Math.max(0L, rightBytes);
    this.leftComplexity = Math.max(0.0d, leftComplexity);
    this.rightComplexity = Math.max(0.0d, rightComplexity);
    this.leftVertexCount = Math.max(0L, leftVertexCount);
    this.rightVertexCount = Math.max(0L, rightVertexCount);
    this.leftMinVertexCount = Math.max(0L, leftMinVertexCount);
    this.rightMinVertexCount = Math.max(0L, rightMinVertexCount);
    this.leftMaxVertexCount = Math.max(0L, leftMaxVertexCount);
    this.rightMaxVertexCount = Math.max(0L, rightMaxVertexCount);
    this.candidatePairCount = Math.max(0L, candidatePairCount);
    this.cibCandidatePairCount = Math.max(0L, cibCandidatePairCount);
    this.partitionIntensity = Math.max(0.0d, partitionIntensity);
  }

  /** Returns the dense effective partition id used during later job execution. */
  public int getPartitionId() {
    return partitionId;
  }

  /** Returns the stable partition code, e.g. the LAHilbert hierarchical code. */
  public String getPartitionCode() {
    return partitionCode;
  }

  /** Returns the spatial coverage envelope of this effective partition. */
  public EnvelopeNDLite getEnvelope() {
    return new EnvelopeNDLite(envelope);
  }

  /** Returns the mapped left-side abstract-feature count. */
  public long getLeftFeatureCount() {
    return leftFeatureCount;
  }

  /** Returns the mapped right-side abstract-feature count. */
  public long getRightFeatureCount() {
    return rightFeatureCount;
  }

  /** Returns the mapped left-side byte load. */
  public long getLeftBytes() {
    return leftBytes;
  }

  /** Returns the mapped right-side byte load. */
  public long getRightBytes() {
    return rightBytes;
  }

  /** Returns the mapped left-side computation complexity. */
  public double getLeftComplexity() {
    return leftComplexity;
  }

  /** Returns the mapped right-side computation complexity. */
  public double getRightComplexity() {
    return rightComplexity;
  }

  /** Returns the mapped left-side total vertex count. */
  public long getLeftVertexCount() {
    return leftVertexCount;
  }

  /** Returns the mapped right-side total vertex count. */
  public long getRightVertexCount() {
    return rightVertexCount;
  }

  /** Returns the smallest left-side feature vertex count mapped to this partition. */
  public long getLeftMinVertexCount() {
    return leftMinVertexCount;
  }

  /** Returns the smallest right-side feature vertex count mapped to this partition. */
  public long getRightMinVertexCount() {
    return rightMinVertexCount;
  }

  /** Returns the largest left-side feature vertex count mapped to this partition. */
  public long getLeftMaxVertexCount() {
    return leftMaxVertexCount;
  }

  /** Returns the largest right-side feature vertex count mapped to this partition. */
  public long getRightMaxVertexCount() {
    return rightMaxVertexCount;
  }

  /** Returns the combined left-plus-right feature count. */
  public long getTotalFeatureCount() {
    return leftFeatureCount + rightFeatureCount;
  }

  /** Returns the combined left-plus-right vertex count. */
  public long getTotalVertexCount() {
    return leftVertexCount + rightVertexCount;
  }

  /** Returns the combined left-plus-right byte load. */
  public long getTotalBytes() {
    return leftBytes + rightBytes;
  }

  /** Returns the total memory load in bytes carried by this effective partition. */
  public long getTotalMemoryLoadBytes() {
    return getTotalBytes();
  }

  /** Returns the partition computation intensity as the sum of mapped feature complexities. */
  public double getPartitionIntensity() {
    return partitionIntensity;
  }

  /** @deprecated Use {@link #getTotalMemoryLoadBytes()} for the execution memory-load view. */
  @Deprecated
  public long getActualLoadBytes() {
    return getTotalMemoryLoadBytes();
  }

  /** @deprecated Use {@link #getPartitionIntensity()} for the partition-intensity view. */
  @Deprecated
  public double getTotalComplexity() {
    return getPartitionIntensity();
  }

  /** Returns the historical candidate-pair proxy: left feature count times right feature count. */
  public long getCandidatePairCount() {
    return candidatePairCount;
  }

  /** Returns the CIB-style candidate-pair estimate, when available, otherwise the historical proxy. */
  public long getCibCandidatePairCount() {
    return cibCandidatePairCount;
  }

  /** Returns a copy with an explicitly derived CIB candidate-pair estimate and partition intensity. */
  public PartProfile withCandidatePairAwareIntensity(long cibCandidatePairCount, double partitionIntensity) {
    return new PartProfile(
        partitionId,
        partitionCode,
        envelope,
        leftFeatureCount,
        rightFeatureCount,
        leftBytes,
        rightBytes,
        leftComplexity,
        rightComplexity,
        leftVertexCount,
        rightVertexCount,
        leftMinVertexCount,
        rightMinVertexCount,
        leftMaxVertexCount,
        rightMaxVertexCount,
        candidatePairCount,
        cibCandidatePairCount,
        partitionIntensity);
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

  /**
   * Creates one profile per effective partition from the current partitioner and
   * the already-collected left/right statistics.
   */
  public static List<PartProfile> fromPartitioner(SpatialPartitioner partitioner,
                                                  long[] leftFeatureCounts,
                                                  long[] rightFeatureCounts,
                                                  long[] leftBytes,
                                                  long[] rightBytes) {
    return fromPartitioner(partitioner, leftFeatureCounts, rightFeatureCounts, leftBytes, rightBytes, null, null);
  }

  /**
   * Creates one execution profile per effective partition from the current partitioner,
   * load arrays, and pre-aggregated left/right complexity sums.
   */
  public static List<PartProfile> fromPartitioner(SpatialPartitioner partitioner,
                                                  long[] leftFeatureCounts,
                                                  long[] rightFeatureCounts,
                                                  long[] leftBytes,
                                                  long[] rightBytes,
                                                  double[] leftComplexities,
                                                  double[] rightComplexities) {
    return fromPartitioner(
        partitioner,
        leftFeatureCounts,
        rightFeatureCounts,
        leftBytes,
        rightBytes,
        leftComplexities,
        rightComplexities,
        null);
  }

  /**
   * Creates one execution profile per effective partition and carries an optional
   * CIB candidate-pair estimate supplied by the partition statistics layer.
   */
  public static List<PartProfile> fromPartitioner(SpatialPartitioner partitioner,
                                                  long[] leftFeatureCounts,
                                                  long[] rightFeatureCounts,
                                                  long[] leftBytes,
                                                  long[] rightBytes,
                                                  double[] leftComplexities,
                                                  double[] rightComplexities,
                                                  long[] cibCandidatePairCounts) {
    return fromPartitioner(
        partitioner,
        leftFeatureCounts,
        rightFeatureCounts,
        leftBytes,
        rightBytes,
        leftComplexities,
        rightComplexities,
        null,
        null,
        null,
        null,
        null,
        null,
        cibCandidatePairCounts);
  }

  /**
   * Creates one execution profile per effective partition and carries vertex statistics needed by
   * the feature-count/candidate-average-vertex CI metric.
   */
  public static List<PartProfile> fromPartitioner(SpatialPartitioner partitioner,
                                                  long[] leftFeatureCounts,
                                                  long[] rightFeatureCounts,
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
                                                  long[] cibCandidatePairCounts) {
    List<PartProfile> inputs = new ArrayList<>();
    if (partitioner == null) {
      return inputs;
    }
    int partitionCount = partitioner.numPartitions();
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      String partitionCode = partitioner instanceof LAHilbertPartitioner
          ? ((LAHilbertPartitioner) partitioner).getPartitionCode(partitionId)
          : partitioner instanceof ExplicitPartitionListPartitioner
              ? ((ExplicitPartitionListPartitioner) partitioner).getPartitionCode(partitionId)
          : Integer.toString(partitionId);
      inputs.add(new PartProfile(
          partitionId,
          partitionCode,
          partitioner.getPartitionMBR(partitionId),
          valueAt(leftFeatureCounts, partitionId),
          valueAt(rightFeatureCounts, partitionId),
          valueAt(leftBytes, partitionId),
          valueAt(rightBytes, partitionId),
          valueAt(leftComplexities, partitionId),
          valueAt(rightComplexities, partitionId),
          valueAt(leftVertexCounts, partitionId),
          valueAt(rightVertexCounts, partitionId),
          valueAt(leftMinVertexCounts, partitionId),
          valueAt(rightMinVertexCounts, partitionId),
          valueAt(leftMaxVertexCounts, partitionId),
          valueAt(rightMaxVertexCounts, partitionId),
          saturatedProduct(valueAt(leftFeatureCounts, partitionId), valueAt(rightFeatureCounts, partitionId)),
          cibCandidatePairCounts == null
              ? saturatedProduct(valueAt(leftFeatureCounts, partitionId), valueAt(rightFeatureCounts, partitionId))
              : valueAt(cibCandidatePairCounts, partitionId),
          valueAt(leftComplexities, partitionId) + valueAt(rightComplexities, partitionId)));
    }
    return inputs;
  }

  private static long valueAt(long[] values, int index) {
    return values == null || index < 0 || index >= values.length ? 0L : values[index];
  }

  private static double valueAt(double[] values, int index) {
    return values == null || index < 0 || index >= values.length ? 0.0d : values[index];
  }
}
