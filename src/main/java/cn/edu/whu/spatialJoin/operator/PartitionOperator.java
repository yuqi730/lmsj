package cn.edu.whu.spatialJoin.operator;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.common.Summary;
import cn.edu.whu.spatialJoin.core.partitioning.LAHilbertPartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeature;
import cn.edu.whu.spatialJoin.core.partitioning.SpatialPartitioner;
import cn.edu.whu.spatialJoin.enums.PartitionerType;
import cn.edu.whu.spatialJoin.synopses.AbstractHistogram;

import java.io.Serializable;

/**
 * Partition-focused operator wrapper.
 *
 * <p>The operator translates high-level configuration into a concrete partitioner instance,
 * initializes it and optionally constructs partitions when summary plus lightweight feature inputs are available.
 */
public final class PartitionOperator implements Serializable {

  private static final long serialVersionUID = 1L;

  private final SpatialJoinOptions options;

  PartitionOperator(SpatialJoinOptions options) {
    this.options = new SpatialJoinOptions(options);
  }

  /** Returns a defensive copy of the normalized partition options. */
  public SpatialJoinOptions getOptions() {
    return new SpatialJoinOptions(options);
  }

  /** Resolves the configured partitioner type enum from the current options. */
  public PartitionerType getPartitionerType() {
    String configuredType = options.getString(
        SpatialJoinConstants.Operator.PARTITIONER_TYPE,
        SpatialJoinConstants.Operator.DEFAULT_PARTITIONER_TYPE);
    PartitionerType type = PartitionerType.fromName(configuredType);
    if (type == null) {
      throw new IllegalArgumentException("Unsupported partitioner type: " + configuredType);
    }
    return type;
  }

  /** Returns the requested partition count hint used during partitioner construction. */
  public int getNumPartitions() {
    return Math.max(1, options.getInt(
        SpatialJoinConstants.Operator.NUM_PARTITIONS,
        SpatialJoinConstants.Operator.DEFAULT_NUM_PARTITIONS));
  }

  /** Returns whether the caller requested disjoint partitions. */
  public boolean isDisjoint() {
    return options.getBoolean(
        SpatialJoinConstants.Operator.DISJOINT,
        SpatialJoinConstants.Operator.DEFAULT_DISJOINT);
  }

  /**
   * Instantiates and initializes the configured partitioner.
   *
   * The returned object is only configured at this stage; it does not contain any
   * partition boundaries until one of the `construct` methods is called.
   */
  public SpatialPartitioner createPartitioner() {
    SpatialPartitioner partitioner = SpatialPartitionerFactory.create(getPartitionerType());
    partitioner.setup(options, isDisjoint());
    return partitioner;
  }

  /**
   * Legacy compatibility entry that builds a partitioner from representative points.
   *
   * <p>The current project prefers the {@link AbstractFeature[]} overload so partition
   * construction is based on the full lightweight dataset rather than on sampled points.
   *
   * @param summary dataset summary
   * @param sample representative points arranged by dimension
   * @param histogram optional histogram
   * @return constructed partitioner
   */
  public SpatialPartitioner construct(Summary summary, double[][] sample, AbstractHistogram histogram) {
    SpatialPartitioner partitioner = createPartitioner();
    partitioner.construct(summary, sample, histogram, getNumPartitions());
    return partitioner;
  }

  /**
   * Builds a partitioner using the full set of lightweight per-feature partition descriptors.
   *
   * <p>This is the preferred construction path in the current project because partition generation
   * should be based on all input features rather than on a sampled subset.
   *
   * @param summary dataset summary
   * @param features one lightweight descriptor per input feature
   * @param histogram optional histogram
   * @return constructed partitioner
   */
  public SpatialPartitioner construct(Summary summary, AbstractFeature[] features, AbstractHistogram histogram) {
    SpatialPartitioner partitioner = createPartitioner();
    partitioner.construct(summary, features, histogram, getNumPartitions());
    return partitioner;
  }

  /**
   * Builds the normal geometric partition layout while preserving left/right feature identity only
   * for LA-Hilbert's optional aggregate-MBR refinement pruning.
   */
  public SpatialPartitioner constructWithSideAwareRefinement(Summary summary,
                                                             AbstractFeature[] leftFeatures,
                                                             AbstractFeature[] rightFeatures,
                                                             AbstractHistogram histogram) {
    SpatialPartitioner partitioner = createPartitioner();
    if (partitioner instanceof LAHilbertPartitioner) {
      ((LAHilbertPartitioner) partitioner).constructWithSideAwareRefinement(
          summary,
          leftFeatures,
          rightFeatures,
          histogram,
          getNumPartitions());
      return partitioner;
    }
    int leftLength = leftFeatures == null ? 0 : leftFeatures.length;
    int rightLength = rightFeatures == null ? 0 : rightFeatures.length;
    AbstractFeature[] combined = new AbstractFeature[leftLength + rightLength];
    if (leftLength > 0)
      System.arraycopy(leftFeatures, 0, combined, 0, leftLength);
    if (rightLength > 0)
      System.arraycopy(rightFeatures, 0, combined, leftLength, rightLength);
    partitioner.construct(summary, combined, histogram, getNumPartitions());
    return partitioner;
  }

  /**
   * Builds the effective partition view for two datasets in one step.
   *
   * <p>This is the preferred entry for partition-aware spatial join preparation because
   * the partitioner can use left/right lightweight descriptors while deciding which
   * partitions are actually effective.
   *
   * @param summary combined dataset summary
   * @param leftFeatures left-side lightweight descriptors
   * @param rightFeatures right-side lightweight descriptors
   * @param histogram optional histogram
   * @return effective partitioning result
   */
  public SpatialPartitioner.EffectivePlan constructEffective(Summary summary,
                                                             AbstractFeature[] leftFeatures,
                                                             AbstractFeature[] rightFeatures,
                                                             AbstractHistogram histogram) {
    SpatialPartitioner partitioner = createPartitioner();
    return partitioner.constructEffective(summary, leftFeatures, rightFeatures, histogram, getNumPartitions());
  }
}
