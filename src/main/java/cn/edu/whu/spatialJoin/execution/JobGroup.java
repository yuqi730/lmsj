package cn.edu.whu.spatialJoin.execution;

import cn.edu.whu.spatialJoin.cost.PartCost;
import cn.edu.whu.spatialJoin.enums.PartitionComplexityType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A group of partition computations that should be submitted as one Spark job.
 *
 * <p>The current framework treats one group as one future job unit. Several
 * groups together form a queue that can then be dispatched with bounded
 * concurrency.
 */
public final class JobGroup implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int jobId;
  private final PartitionComplexityType complexityType;
  private final List<PartCost> partitions;
  private final double totalPartitionIntensity;
  private final long totalMemoryLoadBytes;

  public JobGroup(int jobId,
                  PartitionComplexityType complexityType,
                  List<PartCost> partitions,
                  long totalMemoryLoadBytes) {
    this.jobId = jobId;
    this.complexityType = complexityType == null ? PartitionComplexityType.NORMAL : complexityType;
    this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
    double sum = 0.0d;
    for (PartCost partition : partitions) {
      sum += partition.getScore();
    }
    this.totalPartitionIntensity = sum;
    this.totalMemoryLoadBytes = Math.max(0L, totalMemoryLoadBytes);
  }

  /** Returns the sequential id assigned by the job planner. */
  public int getJobId() {
    return jobId;
  }

  /** Returns the overall complexity label of this job group. */
  public PartitionComplexityType getComplexityType() {
    return complexityType;
  }

  /** Returns the per-partition cost descriptors packed into this job. */
  public List<PartCost> getPartitions() {
    return partitions;
  }

  /** Returns the sum of all partition intensities in this job. */
  public double getTotalPartitionIntensity() {
    return totalPartitionIntensity;
  }

  /** Returns the total memory load in bytes carried by this job. */
  public long getTotalMemoryLoadBytes() {
    return totalMemoryLoadBytes;
  }

  /** @deprecated Use {@link #getTotalPartitionIntensity()} for the execution-planning intensity view. */
  @Deprecated
  public double getTotalIntensity() {
    return getTotalPartitionIntensity();
  }

  /** @deprecated Use {@link #getTotalMemoryLoadBytes()} for the execution memory-load view. */
  @Deprecated
  public long getTotalActualLoadBytes() {
    return getTotalMemoryLoadBytes();
  }

  /** Returns the dense effective partition ids assigned to this job. */
  public int[] getPartitionIds() {
    int[] partitionIds = new int[partitions.size()];
    for (int i = 0; i < partitions.size(); i++) {
      partitionIds[i] = partitions.get(i).getProfile().getPartitionId();
    }
    return partitionIds;
  }

  /** Returns how many effective partitions were packed into this job. */
  public int size() {
    return partitions.size();
  }
}
