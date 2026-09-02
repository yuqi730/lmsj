package cn.edu.whu.spatialJoin.execution;

import cn.edu.whu.spatialJoin.cost.CostClass;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A planned queue of partition-level Spark jobs.
 *
 * <p>This object is intentionally read-only. Once built, it can be handed to a
 * queue/dispatcher layer or simply inspected for debugging and tuning.
 */
public final class JobQueueExecutionPlan implements Serializable {

  private static final long serialVersionUID = 1L;

  private final CostClass classification;
  private final List<JobGroup> highComplexityJobGroups;
  private final List<JobGroup> normalComplexityJobGroups;
  private final List<JobGroup> jobGroups;

  public JobQueueExecutionPlan(CostClass classification,
                               List<JobGroup> highComplexityJobGroups,
                               List<JobGroup> normalComplexityJobGroups) {
    this.classification = classification;
    this.highComplexityJobGroups = Collections.unmodifiableList(new ArrayList<>(highComplexityJobGroups));
    this.normalComplexityJobGroups = Collections.unmodifiableList(new ArrayList<>(normalComplexityJobGroups));
    this.jobGroups = Collections.unmodifiableList(interleave(this.highComplexityJobGroups, this.normalComplexityJobGroups));
  }

  /** Returns the final dispatch order after high/normal groups are interleaved. */
  public List<JobGroup> getJobGroups() {
    return jobGroups;
  }

  /** Returns the high-vs-normal partition classification used to build the plan. */
  public CostClass getClassification() {
    return classification;
  }

  /** Returns the job groups built from high-complexity partitions. */
  public List<JobGroup> getHighComplexityJobGroups() {
    return highComplexityJobGroups;
  }

  /** Returns the job groups built from normal-complexity partitions. */
  public List<JobGroup> getNormalComplexityJobGroups() {
    return normalComplexityJobGroups;
  }

  /** Returns the total number of planned jobs in dispatch order. */
  public int size() {
    return jobGroups.size();
  }

  /** Interleaves high-complexity and normal-complexity jobs into one queue order. */
  private static List<JobGroup> interleave(List<JobGroup> highJobs,
                                           List<JobGroup> normalJobs) {
    List<JobGroup> ordered = new ArrayList<>(highJobs.size() + normalJobs.size());
    int highIndex = 0;
    int normalIndex = 0;
    while (highIndex < highJobs.size() || normalIndex < normalJobs.size()) {
      if (highIndex < highJobs.size()) {
        ordered.add(highJobs.get(highIndex++));
      }
      if (normalIndex < normalJobs.size()) {
        ordered.add(normalJobs.get(normalIndex++));
      }
    }
    return ordered;
  }
}
