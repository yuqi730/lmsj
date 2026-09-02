package cn.edu.whu.spatialJoin.execution;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.cost.CostClass;
import cn.edu.whu.spatialJoin.cost.PartCost;
import cn.edu.whu.spatialJoin.cost.PartProfile;
import cn.edu.whu.spatialJoin.enums.PartitionComplexityType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Planner that converts effective-partition load statistics into a queue of future jobs.
 *
 * <p>The current implementation follows the design's partition-intensity path:
 * 1. compute one scalar partition intensity from the sum of feature complexities
 * 2. split partitions into HIGH/NORMAL groups
 * 3. pack partitions into memory-feasible jobs while minimizing deviation
 */
public final class JobPlanner implements Serializable {

  private static final long serialVersionUID = 1L;

  private final double highComplexityExpansionFactor;
  private final double highComplexityQuantile;
  private final double highComplexityGrayZoneRatio;
  private final long jobMemoryBudgetBytes;
  private final long dispatchLoadBudgetBytes;
  private final int maxPartitionsPerJob;
  private final double maxAverageDeviationWeight;
  private final double varianceDeviationWeight;

  public JobPlanner(double highComplexityExpansionFactor,
                    double highComplexityQuantile,
                    double highComplexityGrayZoneRatio,
                    long jobMemoryBudgetBytes,
                    long dispatchLoadBudgetBytes,
                    int maxPartitionsPerJob,
                    double maxAverageDeviationWeight,
                    double varianceDeviationWeight) {
    this.highComplexityExpansionFactor = Math.max(0.0d, highComplexityExpansionFactor);
    this.highComplexityQuantile = Math.max(0.0d, Math.min(1.0d, highComplexityQuantile));
    this.highComplexityGrayZoneRatio = Math.max(0.0d, Math.min(1.0d, highComplexityGrayZoneRatio));
    this.jobMemoryBudgetBytes = Math.max(1L, jobMemoryBudgetBytes);
    this.dispatchLoadBudgetBytes = Math.max(1L, dispatchLoadBudgetBytes);
    this.maxPartitionsPerJob = Math.max(1, maxPartitionsPerJob);
    this.maxAverageDeviationWeight = Math.max(0.0d, maxAverageDeviationWeight);
    this.varianceDeviationWeight = Math.max(0.0d, varianceDeviationWeight);
  }

  /**
   * Creates a planner from the execution-related options.
   *
   * <p>Partition intensity is currently defined as the sum of feature complexities,
   * so only the classification and job-packing parameters are read here.
   */
  public static JobPlanner fromOptions(SpatialJoinOptions options) {
    if (options == null) {
      BudgetDerivation budgets = deriveBudgets(null);
      return new JobPlanner(
          SpatialJoinConstants.Execution.DEFAULT_HIGH_COMPLEXITY_EXPANSION_FACTOR,
          SpatialJoinConstants.Execution.DEFAULT_HIGH_COMPLEXITY_QUANTILE,
          SpatialJoinConstants.Execution.DEFAULT_HIGH_COMPLEXITY_GRAY_ZONE_RATIO,
          budgets.jobMemoryBudgetBytes,
          budgets.dispatchLoadBudgetBytes,
          deriveMaxPartitionsPerJob(null),
          SpatialJoinConstants.Execution.DEFAULT_DEVIATION_MAX_AVG_WEIGHT,
          SpatialJoinConstants.Execution.DEFAULT_DEVIATION_VARIANCE_WEIGHT);
    }
    BudgetDerivation budgets = deriveBudgets(options);
    return new JobPlanner(
        options.getDouble(
            SpatialJoinConstants.Execution.HIGH_COMPLEXITY_EXPANSION_FACTOR,
            SpatialJoinConstants.Execution.DEFAULT_HIGH_COMPLEXITY_EXPANSION_FACTOR),
        options.getDouble(
            SpatialJoinConstants.Execution.HIGH_COMPLEXITY_QUANTILE,
            SpatialJoinConstants.Execution.DEFAULT_HIGH_COMPLEXITY_QUANTILE),
        options.getDouble(
            SpatialJoinConstants.Execution.HIGH_COMPLEXITY_GRAY_ZONE_RATIO,
            SpatialJoinConstants.Execution.DEFAULT_HIGH_COMPLEXITY_GRAY_ZONE_RATIO),
        budgets.jobMemoryBudgetBytes,
        budgets.dispatchLoadBudgetBytes,
        deriveMaxPartitionsPerJob(options),
        options.getDouble(
            SpatialJoinConstants.Execution.DEVIATION_MAX_AVG_WEIGHT,
            SpatialJoinConstants.Execution.DEFAULT_DEVIATION_MAX_AVG_WEIGHT),
        options.getDouble(
            SpatialJoinConstants.Execution.DEVIATION_VARIANCE_WEIGHT,
            SpatialJoinConstants.Execution.DEFAULT_DEVIATION_VARIANCE_WEIGHT));
  }

  /**
   * Returns the effective job-memory budget used to pack partitions into one Spark job.
   *
   * <p>Unless overridden explicitly through {@code sj.execution.job.memoryBudget},
   * the budget follows the derived global execution-memory budget. If
   * {@code sj.execution.job.memoryBudgetRatio} is set, only job packing uses
   * {@code globalExecutionBudget * ratio}; active-job dispatch still uses the full global budget.
   */
  public long getJobMemoryBudgetBytes() {
    return jobMemoryBudgetBytes;
  }

  /**
   * Returns the active-load budget used by the resource-aware dispatcher.
   *
   * <p>This is normally the same as {@link #getJobMemoryBudgetBytes()}. When a
   * job-memory-budget ratio is configured, this remains the full global execution budget so the
   * ratio only controls task-unit granularity rather than suppressing overall concurrency.
   */
  public long getDispatchLoadBudgetBytes() {
    return dispatchLoadBudgetBytes;
  }

  /**
   * Converts one load descriptor per effective partition into a scalar cost object.
   *
   * <p>The scalar intensity is already materialized in each {@link PartProfile}. Different
   * execution experiments may populate it with the original size-function complexity, a
   * candidate-average complexity score, or the CI hybrid score.
   */
  public List<PartCost> estimatePartitionCosts(List<PartProfile> inputs) {
    List<PartCost> intensities = new ArrayList<>();
    if (inputs == null) {
      return intensities;
    }
    for (PartProfile input : inputs) {
      intensities.add(estimateIntensity(input));
    }
    return intensities;
  }

  /**
   * Splits partitions into HIGH and NORMAL complexity groups using the current threshold rule.
   *
   * <p>The threshold is {@code max(lambda * median, quantile)} as described by the
   * detailed design. To avoid unstable flips caused by tiny score differences around
   * that boundary, the planner also supports a small gray zone immediately below the
   * threshold. Scores in {@code [threshold * (1 - grayZoneRatio), threshold)} are
   * conservatively promoted to {@code HIGH}.
   *
   * <p>The method first extracts one scalar intensity score from every partition profile, then computes:
   * <ul>
   *   <li>the median intensity of all partitions,</li>
   *   <li>the configured quantile intensity, and</li>
   *   <li>the final split threshold as {@code max(lambda * median, quantileValue)}, and</li>
   *   <li>the gray-zone lower bound as {@code threshold * (1 - grayZoneRatio)}.</li>
   * </ul>
   *
   * <p>Every partition whose score is greater than or equal to the threshold becomes {@code HIGH}.
   * Partitions that fall just below the threshold but remain within the configured gray zone are
   * also labeled {@code HIGH} so near-identical costs do not get split into different groups due
   * to floating-point noise or tiny score gaps. The returned {@link CostClass} keeps both the
   * numeric threshold summary and the typed partition lists for later job packing.
   *
   * @param intensities one scalar cost object per effective partition.
   * @return classification summary containing threshold statistics plus HIGH/NORMAL partition lists.
   */
  public CostClass classifyPartitionCosts(List<PartCost> intensities) {
    if (intensities == null || intensities.isEmpty()) {
      return new CostClass(0.0d, 0.0d, 0.0d, Collections.emptyList(), Collections.emptyList());
    }
    List<Double> ascendingScores = new ArrayList<>(intensities.size());
    for (PartCost intensity : intensities) {
      ascendingScores.add(intensity.getScore());
    }
    Collections.sort(ascendingScores);
    double median = percentile(ascendingScores, 0.5d);
    double quantileValue = percentile(ascendingScores, highComplexityQuantile);
    double threshold = Math.max(highComplexityExpansionFactor * median, quantileValue);
    double grayZoneLowerBound = threshold * Math.max(0.0d, 1.0d - highComplexityGrayZoneRatio);

    List<PartCost> high = new ArrayList<>();
    List<PartCost> normal = new ArrayList<>();
    for (PartCost intensity : intensities) {
      double score = intensity.getScore();
      PartitionComplexityType type =
          score >= grayZoneLowerBound
              ? PartitionComplexityType.HIGH
              : PartitionComplexityType.NORMAL;
      PartCost typed = intensity.withComplexityType(type);
      if (type == PartitionComplexityType.HIGH) {
        high.add(typed);
      } else {
        normal.add(typed);
      }
    }
    Collections.sort(high);
    Collections.sort(normal);
    return new CostClass(median, quantileValue, threshold, high, normal);
  }

  /**
   * Builds the final job-queue plan from one load descriptor per effective partition.
   *
   * <p>The method performs three steps in order: estimate partition costs,
   * classify them into HIGH/NORMAL complexity, then pack them into memory-feasible jobs.
   *
   * @param inputs one execution profile per effective partition.
   * @return a complete job-queue plan containing the classification summary and the frozen job groups.
   */
  public JobQueueExecutionPlan buildJobPlan(List<PartProfile> inputs) {
    return buildJobPlanFromCosts(classifyPartitionCosts(estimatePartitionCosts(inputs)));
  }

  /**
   * Builds the final job queue after partition costs have already been estimated and classified.
   *
   * <p>The method first lets HIGH partitions seed the candidate job list, then tries to place NORMAL
   * partitions into the existing candidates before creating new NORMAL jobs. This keeps the behavior
   * aligned with the detailed design where high-intensity work forms the skeleton of the plan and
   * normal partitions are woven into memory-feasible jobs afterward.
   *
   * @param classification precomputed HIGH/NORMAL partition classification.
   * @return immutable job plan that can be handed directly to the dispatcher or sequential executor.
   */
  public JobQueueExecutionPlan buildJobPlanFromCosts(CostClass classification) {
    List<MutablePartitionJobGroup> highJobs = new ArrayList<>();
    List<MutablePartitionJobGroup> normalJobs = new ArrayList<>();
    List<MutablePartitionJobGroup> allCandidates = new ArrayList<>();
    assignPartitions(classification.getHighCosts(), highJobs, allCandidates, PartitionComplexityType.HIGH);
    assignPartitions(classification.getNormalCosts(), normalJobs, allCandidates, PartitionComplexityType.NORMAL);
    return new JobQueueExecutionPlan(classification, freeze(highJobs), freeze(normalJobs));
  }

  /** Converts one partition execution profile into a scalar computation-intensity score. */
  private PartCost estimateIntensity(PartProfile input) {
    double score = input == null ? 0.0d : input.getPartitionIntensity();
    return new PartCost(input, score);
  }

  /**
   * Packs partitions into future jobs while respecting memory budget and job-size limits.
   *
   * Existing candidate jobs are scored by deviation and the partition is appended to
   * the best feasible one; otherwise a new job group is created.
   *
   * <p>For each partition in the given order, the method:
   * <ol>
   *   <li>examines all existing candidate jobs,</li>
   *   <li>keeps only those that can still satisfy the memory budget and maximum partition count,</li>
   *   <li>scores every feasible candidate by the configured deviation formula, and</li>
   *   <li>either appends the partition to the best candidate or creates a new job bucket.</li>
   * </ol>
   *
   * @param partitions the ordered partitions to place into jobs.
   * @param mutableOwnedJobs the list that owns newly created jobs of the current default type.
   * @param mutableCandidateJobs all jobs currently eligible to receive more partitions.
   * @param defaultJobType the complexity label assigned to any newly created job bucket.
   */
  private void assignPartitions(List<PartCost> partitions,
                                List<MutablePartitionJobGroup> mutableOwnedJobs,
                                List<MutablePartitionJobGroup> mutableCandidateJobs,
                                PartitionComplexityType defaultJobType) {
    for (PartCost partition : partitions) {
      MutablePartitionJobGroup bestCandidate = null;
      double bestDeviation = Double.POSITIVE_INFINITY;
      for (MutablePartitionJobGroup candidate : mutableCandidateJobs) {
        if (!candidate.canAccept(partition, jobMemoryBudgetBytes, maxPartitionsPerJob)) {
          continue;
        }
        double deviation = candidate.previewDeviation(partition, maxAverageDeviationWeight, varianceDeviationWeight);
        if (deviation < bestDeviation) {
          bestDeviation = deviation;
          bestCandidate = candidate;
        }
      }
      if (bestCandidate == null) {
        MutablePartitionJobGroup newJob = new MutablePartitionJobGroup(mutableCandidateJobs.size(), defaultJobType);
        newJob.add(partition);
        mutableOwnedJobs.add(newJob);
        mutableCandidateJobs.add(newJob);
      } else {
        bestCandidate.add(partition);
      }
    }
  }

  /** Returns the requested percentile value from an ascending numeric list. */
  private static double percentile(List<Double> ascendingValues, double ratio) {
    if (ascendingValues.isEmpty()) {
      return 0.0d;
    }
    if (ascendingValues.size() == 1) {
      return ascendingValues.get(0);
    }
    double boundedRatio = Math.max(0.0d, Math.min(1.0d, ratio));
    double position = boundedRatio * (ascendingValues.size() - 1);
    int lower = (int) Math.floor(position);
    int upper = (int) Math.ceil(position);
    if (lower == upper) {
      return ascendingValues.get(lower);
    }
    double fraction = position - lower;
    return ascendingValues.get(lower) * (1.0d - fraction) + ascendingValues.get(upper) * fraction;
  }

  /** Freezes mutable planning groups into immutable externally visible job groups. */
  private static List<JobGroup> freeze(List<MutablePartitionJobGroup> groups) {
    List<JobGroup> immutableGroups = new ArrayList<>(groups.size());
    for (MutablePartitionJobGroup group : groups) {
      immutableGroups.add(group.toImmutable());
    }
    return immutableGroups;
  }

  /**
   * Derives both job-packing and dispatch budgets from Spark executor resources.
   *
   * <p>The global dispatch budget is {@code M * (1 - alpha)}, where:
   * <ul>
   *   <li>{@code M} is the sum of all executor-memory allocations</li>
   *   <li>{@code alpha} is {@code spark.memory.storageFraction}</li>
   * </ul>
   *
   * <p>If {@code sj.execution.job.memoryBudgetRatio} is positive, the job-packing budget becomes
   * {@code globalBudget * ratio}. An explicit {@code sj.execution.job.memoryBudget} keeps the legacy
   * behavior and uses the same fixed value for both job packing and active dispatch.
   */
  private static BudgetDerivation deriveBudgets(SpatialJoinOptions options) {
    if (options != null && options.contains(SpatialJoinConstants.Execution.JOB_MEMORY_BUDGET)) {
      long explicitBudget = Math.max(1L, options.getSizeAsBytes(
          SpatialJoinConstants.Execution.JOB_MEMORY_BUDGET,
          SpatialJoinConstants.Execution.DEFAULT_JOB_MEMORY_BUDGET_BYTES));
      return new BudgetDerivation(explicitBudget, explicitBudget);
    }

    long executorMemoryBytes = resolveExecutionMemoryBytes(options);
    int executorInstances = resolveExecutorInstances(options);
    double storageFraction = options == null
        ? SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION
        : options.getDouble(
            SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION_KEY,
            SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION);
    long totalExecutorMemoryBytes = Math.max(1L, executorMemoryBytes) * Math.max(1, executorInstances);
    long derivedBudget = (long) (totalExecutorMemoryBytes * Math.max(0.0d, 1.0d - storageFraction));
    long globalBudget = Math.max(1L, derivedBudget);
    double ratio = options == null
        ? SpatialJoinConstants.Execution.DEFAULT_JOB_MEMORY_BUDGET_RATIO
        : options.getDouble(
            SpatialJoinConstants.Execution.JOB_MEMORY_BUDGET_RATIO,
            SpatialJoinConstants.Execution.DEFAULT_JOB_MEMORY_BUDGET_RATIO);
    double boundedRatio = Math.max(0.0d, Math.min(1.0d, ratio));
    long jobBudget = boundedRatio > 0.0d
        ? Math.max(1L, (long) (globalBudget * boundedRatio))
        : globalBudget;
    return new BudgetDerivation(jobBudget, globalBudget);
  }

  /**
   * Derives the per-job partition-count cap.
   *
   * <p>The precedence is:
   * <ol>
   *   <li>explicit {@code sj.execution.maxPartitionsPerJob}</li>
   *   <li>{@code spark.default.parallelism}</li>
   *   <li>{@code spark.executor.instances * spark.executor.cores}</li>
   *   <li>the built-in fallback constant</li>
   * </ol>
   */
  private static int deriveMaxPartitionsPerJob(SpatialJoinOptions options) {
    if (options != null && options.contains(SpatialJoinConstants.Execution.MAX_PARTITIONS_PER_JOB)) {
      return Math.max(1, options.getInt(
          SpatialJoinConstants.Execution.MAX_PARTITIONS_PER_JOB,
          SpatialJoinConstants.Execution.DEFAULT_MAX_PARTITIONS_PER_JOB));
    }
    if (options != null && options.contains("spark.default.parallelism")) {
      return Math.max(1, options.getInt(
          "spark.default.parallelism",
          SpatialJoinConstants.Execution.DEFAULT_MAX_PARTITIONS_PER_JOB));
    }
    if (options != null) {
      int executorCores = Math.max(1, options.getInt(
          SpatialJoinConstants.SparkDefaults.EXECUTOR_CORES_KEY,
          SpatialJoinConstants.SparkDefaults.EXECUTOR_CORES));
      int executorInstances = resolveExecutorInstances(options);
      return Math.max(1, executorCores * executorInstances);
    }
    return SpatialJoinConstants.Execution.DEFAULT_MAX_PARTITIONS_PER_JOB;
  }

  /** Resolves the number of executors contributing to the total Spark memory budget. */
  private static int resolveExecutorInstances(SpatialJoinOptions options) {
    if (isLocalMaster(options)) {
      return 1;
    }
    if (options == null) {
      return SpatialJoinConstants.SparkDefaults.EXECUTOR_INSTANCES;
    }
    if (options.contains(SpatialJoinConstants.SparkDefaults.EXECUTOR_INSTANCES_KEY)) {
      return Math.max(1, options.getInt(
          SpatialJoinConstants.SparkDefaults.EXECUTOR_INSTANCES_KEY,
          SpatialJoinConstants.SparkDefaults.EXECUTOR_INSTANCES));
    }
    if (options.contains(SpatialJoinConstants.SparkDefaults.DYNAMIC_ALLOCATION_MAX_EXECUTORS_KEY)) {
      return Math.max(1, options.getInt(
          SpatialJoinConstants.SparkDefaults.DYNAMIC_ALLOCATION_MAX_EXECUTORS_KEY,
          SpatialJoinConstants.SparkDefaults.EXECUTOR_INSTANCES));
    }
    return SpatialJoinConstants.SparkDefaults.EXECUTOR_INSTANCES;
  }

  /**
   * Resolves the memory budget source used by job planning.
   *
   * <p>Cluster mode uses executor memory. Local mode has no separate executors, so the
   * planner treats driver memory as the effective worker-memory budget.
   */
  private static long resolveExecutionMemoryBytes(SpatialJoinOptions options) {
    if (isLocalMaster(options)) {
      if (options != null && options.contains(SpatialJoinConstants.SparkDefaults.DRIVER_MEMORY_KEY)) {
        return options.getSizeAsBytes(
            SpatialJoinConstants.SparkDefaults.DRIVER_MEMORY_KEY,
            SpatialJoinConstants.SparkDefaults.DRIVER_MEMORY);
      }
      if (options != null && options.contains(SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY_KEY)) {
        return options.getSizeAsBytes(
            SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY_KEY,
            SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY);
      }
      return parseBytes(SpatialJoinConstants.SparkDefaults.DRIVER_MEMORY);
    }
    return options == null
        ? parseBytes(SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY)
        : options.getSizeAsBytes(
            SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY_KEY,
            SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY);
  }

  /** Returns whether the current execution master is a Spark local mode such as {@code local[*]}. */
  private static boolean isLocalMaster(SpatialJoinOptions options) {
    if (options == null) {
      return false;
    }
    String master = options.getString("spark.master", "");
    return master != null && master.trim().toLowerCase().startsWith("local");
  }

  /** Parses a Spark-style size string such as {@code 1g}. */
  private static long parseBytes(String value) {
    return org.apache.spark.network.util.JavaUtils.byteStringAsBytes(value);
  }

  private static final class BudgetDerivation {
    private final long jobMemoryBudgetBytes;
    private final long dispatchLoadBudgetBytes;

    private BudgetDerivation(long jobMemoryBudgetBytes, long dispatchLoadBudgetBytes) {
      this.jobMemoryBudgetBytes = Math.max(1L, jobMemoryBudgetBytes);
      this.dispatchLoadBudgetBytes = Math.max(1L, dispatchLoadBudgetBytes);
    }
  }

  private static final class MutablePartitionJobGroup {
    private final int jobId;
    private PartitionComplexityType complexityType;
    private final List<PartCost> partitions = new ArrayList<>();
    private long totalMemoryLoadBytes;

    /** Creates one mutable job bucket during the planning phase. */
    private MutablePartitionJobGroup(int jobId, PartitionComplexityType complexityType) {
      this.jobId = jobId;
      this.complexityType = complexityType;
    }

    /** Checks whether a partition can be appended without violating planner limits. */
    private boolean canAccept(PartCost partition, long budgetBytes, int maxPartitionsPerJob) {
      return partitions.size() < maxPartitionsPerJob &&
          totalMemoryLoadBytes + partition.getProfile().getTotalMemoryLoadBytes() <= budgetBytes;
    }

    /** Appends one partition to this in-progress job and updates summary metrics. */
    private void add(PartCost partition) {
      partitions.add(partition);
      totalMemoryLoadBytes += partition.getProfile().getTotalMemoryLoadBytes();
      if (partition.getComplexityType() == PartitionComplexityType.HIGH) {
        complexityType = PartitionComplexityType.HIGH;
      }
    }

    /** Estimates the deviation score after tentatively adding one more partition. */
    private double previewDeviation(PartCost next,
                                    double maxAverageDeviationWeight,
                                    double varianceDeviationWeight) {
      List<Double> scores = new ArrayList<>(partitions.size() + 1);
      for (PartCost partition : partitions) {
        scores.add(partition.getScore());
      }
      scores.add(next.getScore());
      double max = Double.NEGATIVE_INFINITY;
      double sum = 0.0d;
      for (double score : scores) {
        max = Math.max(max, score);
        sum += score;
      }
      double average = scores.isEmpty() ? 0.0d : sum / scores.size();
      double variance = 0.0d;
      for (double score : scores) {
        double diff = score - average;
        variance += diff * diff;
      }
      if (!scores.isEmpty()) {
        variance /= scores.size();
      }
      return maxAverageDeviationWeight * (max - average) + varianceDeviationWeight * variance;
    }

    /** Converts the mutable planning bucket into the immutable public job view. */
    private JobGroup toImmutable() {
      return new JobGroup(jobId, complexityType, partitions, totalMemoryLoadBytes);
    }
  }
}
