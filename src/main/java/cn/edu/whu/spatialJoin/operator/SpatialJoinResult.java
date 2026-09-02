package cn.edu.whu.spatialJoin.operator;

import cn.edu.whu.spatialJoin.execution.JobExecutionResult;
import cn.edu.whu.spatialJoin.execution.JobQueueExecutionPlan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unified result object for spatial join execution.
 *
 * <p>Read and partition are treated as internal preparation steps in the current
 * project, so only the join phase exposes a public result object. Local preview
 * execution and partition-aware execution both report through this same type.
 */
public final class SpatialJoinResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String leftPath;
  private final String rightPath;
  private final String leftFormat;
  private final String rightFormat;
  private final String predicate;
  private final String algorithm;
  private final long leftCount;
  private final long rightCount;
  private final long matchCount;
  private final int effectivePartitionCount;
  private final int plannedJobCount;
  private final List<JobExecutionResult> jobResults;

  public SpatialJoinResult(String leftPath,
                           String rightPath,
                           String leftFormat,
                           String rightFormat,
                           String predicate,
                           String algorithm,
                           long leftCount,
                           long rightCount,
                           long matchCount,
                           int effectivePartitionCount,
                           int plannedJobCount,
                           List<JobExecutionResult> jobResults) {
    this.leftPath = leftPath;
    this.rightPath = rightPath;
    this.leftFormat = leftFormat;
    this.rightFormat = rightFormat;
    this.predicate = predicate;
    this.algorithm = algorithm;
    this.leftCount = leftCount;
    this.rightCount = rightCount;
    this.matchCount = matchCount;
    this.effectivePartitionCount = Math.max(0, effectivePartitionCount);
    this.plannedJobCount = Math.max(0, plannedJobCount);
    this.jobResults = jobResults == null
        ? Collections.emptyList()
        : Collections.unmodifiableList(new ArrayList<>(jobResults));
  }

  /** Returns the configured left input path. */
  public String getLeftPath() {
    return leftPath;
  }

  /** Returns the configured right input path. */
  public String getRightPath() {
    return rightPath;
  }

  /** Returns the configured left input format. */
  public String getLeftFormat() {
    return leftFormat;
  }

  /** Returns the configured right input format. */
  public String getRightFormat() {
    return rightFormat;
  }

  /** Returns the join predicate executed by this result. */
  public String getPredicate() {
    return predicate;
  }

  /** Returns the join algorithm label executed by this result. */
  public String getAlgorithm() {
    return algorithm;
  }

  /** Returns the left-side feature count observed during execution. */
  public long getLeftCount() {
    return leftCount;
  }

  /** Returns the right-side feature count observed during execution. */
  public long getRightCount() {
    return rightCount;
  }

  /** Returns the total match count produced by the join. */
  public long getMatchCount() {
    return matchCount;
  }

  /** Returns how many effective partitions were finally involved in execution. */
  public int getEffectivePartitionCount() {
    return effectivePartitionCount;
  }

  /** Returns how many partition-aware jobs were planned. */
  public int getPlannedJobCount() {
    return plannedJobCount;
  }

  /** Returns the per-job execution summaries for partition-aware runs. */
  public List<JobExecutionResult> getJobResults() {
    return jobResults;
  }

  /** Returns whether this result came from the partition-aware execution path. */
  public boolean isPartitionAware() {
    return effectivePartitionCount > 0 || plannedJobCount > 0 || !jobResults.isEmpty();
  }

  public static SpatialJoinResult basic(String leftPath,
                                        String rightPath,
                                        String leftFormat,
                                        String rightFormat,
                                        String predicate,
                                        String algorithm,
                                        long leftCount,
                                        long rightCount,
                                        long matchCount) {
    return new SpatialJoinResult(
        leftPath,
        rightPath,
        leftFormat,
        rightFormat,
        predicate,
        algorithm,
        leftCount,
        rightCount,
        matchCount,
        0,
        0,
        Collections.emptyList());
  }

  public static SpatialJoinResult fromPlan(String leftPath,
                                           String rightPath,
                                           String leftFormat,
                                           String rightFormat,
                                           String predicate,
                                           String algorithm,
                                           long leftCount,
                                           long rightCount,
                                           long matchCount,
                                           JobQueueExecutionPlan plan,
                                           List<JobExecutionResult> jobResults) {
    int effectivePartitionCount = 0;
    if (jobResults != null) {
      for (JobExecutionResult jobResult : jobResults)
        effectivePartitionCount += jobResult.getPartitionIds().length;
    }
    return new SpatialJoinResult(
        leftPath,
        rightPath,
        leftFormat,
        rightFormat,
        predicate,
        algorithm,
        leftCount,
        rightCount,
        matchCount,
        effectivePartitionCount,
        plan == null ? 0 : plan.size(),
        jobResults);
  }
}
