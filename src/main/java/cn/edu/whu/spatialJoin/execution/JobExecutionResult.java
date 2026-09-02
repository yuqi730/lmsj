package cn.edu.whu.spatialJoin.execution;

import cn.edu.whu.spatialJoin.enums.PartitionComplexityType;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Execution summary for one partition-level join job group.
 */
public final class JobExecutionResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int jobId;
  private final PartitionComplexityType complexityType;
  private final int[] partitionIds;
  private final long leftFeatureCount;
  private final long rightFeatureCount;
  private final long totalMemoryLoadBytes;
  private final long matchCount;
  private final long localJoinNanos;
  private final long[] partitionLocalJoinNanos;
  private final long planeSweepXAxisCount;
  private final long planeSweepYAxisCount;
  private final long planeSweepDiagonalAxisCount;
  private final String objectPairOutputPath;

  public JobExecutionResult(int jobId,
                            PartitionComplexityType complexityType,
                            int[] partitionIds,
                            long leftFeatureCount,
                            long rightFeatureCount,
                            long totalMemoryLoadBytes,
                            long matchCount) {
    this(
        jobId,
        complexityType,
        partitionIds,
        leftFeatureCount,
        rightFeatureCount,
        totalMemoryLoadBytes,
        matchCount,
        0L,
        null,
        0L,
        0L,
        0L,
        null);
  }

  /**
   * Creates one per-job execution summary, including which effective plane-sweep axis was
   * ultimately used by each partition-local join call inside this job.
   */
  public JobExecutionResult(int jobId,
                            PartitionComplexityType complexityType,
                            int[] partitionIds,
                            long leftFeatureCount,
                            long rightFeatureCount,
                            long totalMemoryLoadBytes,
                            long matchCount,
                            long planeSweepXAxisCount,
                            long planeSweepYAxisCount,
                            long planeSweepDiagonalAxisCount) {
    this(
        jobId,
        complexityType,
        partitionIds,
        leftFeatureCount,
        rightFeatureCount,
        totalMemoryLoadBytes,
        matchCount,
        0L,
        null,
        planeSweepXAxisCount,
        planeSweepYAxisCount,
        planeSweepDiagonalAxisCount,
        null);
  }

  /**
   * Creates one per-job execution summary, including the accumulated partition-local join
   * time measured inside executor tasks. This excludes Spark job submission, scheduling,
   * shuffle grouping and partition-file reload time.
   */
  public JobExecutionResult(int jobId,
                            PartitionComplexityType complexityType,
                            int[] partitionIds,
                            long leftFeatureCount,
                            long rightFeatureCount,
                            long totalMemoryLoadBytes,
                            long matchCount,
                            long localJoinNanos,
                            long planeSweepXAxisCount,
                            long planeSweepYAxisCount,
                            long planeSweepDiagonalAxisCount) {
    this(
        jobId,
        complexityType,
        partitionIds,
        leftFeatureCount,
        rightFeatureCount,
        totalMemoryLoadBytes,
        matchCount,
        localJoinNanos,
        null,
        planeSweepXAxisCount,
        planeSweepYAxisCount,
        planeSweepDiagonalAxisCount,
        null);
  }

  public JobExecutionResult(int jobId,
                            PartitionComplexityType complexityType,
                            int[] partitionIds,
                            long leftFeatureCount,
                            long rightFeatureCount,
                            long totalMemoryLoadBytes,
                            long matchCount,
                            long localJoinNanos,
                            long planeSweepXAxisCount,
                            long planeSweepYAxisCount,
                            long planeSweepDiagonalAxisCount,
                            String objectPairOutputPath) {
    this(
        jobId,
        complexityType,
        partitionIds,
        leftFeatureCount,
        rightFeatureCount,
        totalMemoryLoadBytes,
        matchCount,
        localJoinNanos,
        null,
        planeSweepXAxisCount,
        planeSweepYAxisCount,
        planeSweepDiagonalAxisCount,
        objectPairOutputPath);
  }

  public JobExecutionResult(int jobId,
                            PartitionComplexityType complexityType,
                            int[] partitionIds,
                            long leftFeatureCount,
                            long rightFeatureCount,
                            long totalMemoryLoadBytes,
                            long matchCount,
                            long localJoinNanos,
                            long[] partitionLocalJoinNanos,
                            long planeSweepXAxisCount,
                            long planeSweepYAxisCount,
                            long planeSweepDiagonalAxisCount,
                            String objectPairOutputPath) {
    this.jobId = jobId;
    this.complexityType = complexityType == null ? PartitionComplexityType.NORMAL : complexityType;
    this.partitionIds = partitionIds == null ? new int[0] : Arrays.copyOf(partitionIds, partitionIds.length);
    this.leftFeatureCount = Math.max(0L, leftFeatureCount);
    this.rightFeatureCount = Math.max(0L, rightFeatureCount);
    this.totalMemoryLoadBytes = Math.max(0L, totalMemoryLoadBytes);
    this.matchCount = Math.max(0L, matchCount);
    this.localJoinNanos = Math.max(0L, localJoinNanos);
    this.partitionLocalJoinNanos =
        partitionLocalJoinNanos == null ? new long[0] : Arrays.copyOf(partitionLocalJoinNanos, partitionLocalJoinNanos.length);
    this.planeSweepXAxisCount = Math.max(0L, planeSweepXAxisCount);
    this.planeSweepYAxisCount = Math.max(0L, planeSweepYAxisCount);
    this.planeSweepDiagonalAxisCount = Math.max(0L, planeSweepDiagonalAxisCount);
    this.objectPairOutputPath =
        objectPairOutputPath == null || objectPairOutputPath.trim().isEmpty() ? null : objectPairOutputPath.trim();
  }

  /** Returns the planned job id that produced this execution summary. */
  public int getJobId() {
    return jobId;
  }

  /** Returns whether this job was planned as HIGH or NORMAL complexity. */
  public PartitionComplexityType getComplexityType() {
    return complexityType;
  }

  /** Returns the effective partition ids covered by this job. */
  public int[] getPartitionIds() {
    return Arrays.copyOf(partitionIds, partitionIds.length);
  }

  /** Returns the left-side feature count reloaded for this job. */
  public long getLeftFeatureCount() {
    return leftFeatureCount;
  }

  /** Returns the right-side feature count reloaded for this job. */
  public long getRightFeatureCount() {
    return rightFeatureCount;
  }

  /** Returns the total memory load in bytes assigned to this job. */
  public long getTotalMemoryLoadBytes() {
    return totalMemoryLoadBytes;
  }

  /** Returns the number of join matches produced by this job. */
  public long getMatchCount() {
    return matchCount;
  }

  /**
   * Returns the accumulated time spent inside partition-local join kernels, in nanoseconds.
   * This intentionally excludes Spark job submission, task scheduling, partition reloads and
   * shuffle/grouping overhead.
   */
  public long getLocalJoinNanos() {
    return localJoinNanos;
  }

  /**
   * Returns per-partition local join kernel times, in nanoseconds, when a diagnostic
   * execution path collected them. Normal execution paths return an empty array.
   */
  public long[] getPartitionLocalJoinNanos() {
    return Arrays.copyOf(partitionLocalJoinNanos, partitionLocalJoinNanos.length);
  }

  /** Returns how many partition-local joins in this job effectively swept along the x axis. */
  public long getPlaneSweepXAxisCount() {
    return planeSweepXAxisCount;
  }

  /** Returns how many partition-local joins in this job effectively swept along the y axis. */
  public long getPlaneSweepYAxisCount() {
    return planeSweepYAxisCount;
  }

  /** Returns how many partition-local joins in this job effectively swept along the diagonal axis. */
  public long getPlaneSweepDiagonalAxisCount() {
    return planeSweepDiagonalAxisCount;
  }

  /** Returns the external pair-file directory written by object-level recovery mode, if any. */
  public String getObjectPairOutputPath() {
    return objectPairOutputPath;
  }

  /** @deprecated Use {@link #getTotalMemoryLoadBytes()} for the execution memory-load view. */
  @Deprecated
  public long getActualLoadBytes() {
    return getTotalMemoryLoadBytes();
  }
}
