package cn.edu.whu.spatialJoin.execution;

import cn.edu.whu.spatialJoin.cost.CostClass;
import cn.edu.whu.spatialJoin.cost.PartCost;
import cn.edu.whu.spatialJoin.cost.PartProfile;
import cn.edu.whu.spatialJoin.enums.PartitionComplexityType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds intensity-contiguous task units with a capacity-first dynamic program.
 *
 * <p>The lexicographic objective first minimizes the number of task units and then minimizes
 * the total within-unit intensity SSE. Every unit must satisfy both the configured partition-count
 * cap and the per-job memory budget.
 */
final class CapacityFirstSseJobPlanner {

  private static final double SSE_EPSILON = 1e-12;

  private CapacityFirstSseJobPlanner() {
  }

  static JobQueueExecutionPlan build(List<PartProfile> profiles, int capacity, long budget) {
    if (profiles == null || capacity <= 0 || budget <= 0) {
      throw new IllegalArgumentException("Profiles and positive capacity/memory budget required");
    }
    List<PartCost> costs = validatedSortedCosts(profiles, budget);
    int size = costs.size();
    if (size == 0) {
      return emptyPlan(costs);
    }

    int[] minimumGroups = new int[size + 1];
    double[] minimumSse = new double[size + 1];
    int[] previous = new int[size + 1];
    Arrays.fill(minimumGroups, Integer.MAX_VALUE);
    Arrays.fill(minimumSse, Double.POSITIVE_INFINITY);
    Arrays.fill(previous, -1);
    minimumGroups[0] = 0;
    minimumSse[0] = 0.0d;

    for (int end = 1; end <= size; end++) {
      long bytes = 0L;
      double mean = 0.0d;
      double segmentSse = 0.0d;
      int segmentSize = 0;
      for (int start = end - 1; start >= 0 && segmentSize < capacity; start--) {
        PartCost cost = costs.get(start);
        long partBytes = cost.getProfile().getTotalMemoryLoadBytes();
        if (bytes > budget - partBytes) {
          break;
        }
        bytes += partBytes;
        segmentSize++;

        // Welford's update keeps the segment SSE stable while the left boundary moves.
        double delta = cost.getScore() - mean;
        mean += delta / segmentSize;
        segmentSse += delta * (cost.getScore() - mean);

        if (minimumGroups[start] == Integer.MAX_VALUE) {
          continue;
        }
        int candidateGroups = minimumGroups[start] + 1;
        double candidateSse = minimumSse[start] + Math.max(0.0d, segmentSse);
        if (candidateGroups < minimumGroups[end]
            || (candidateGroups == minimumGroups[end]
                && (candidateSse < minimumSse[end] - SSE_EPSILON
                    || (Math.abs(candidateSse - minimumSse[end]) <= SSE_EPSILON
                        && start < previous[end])))) {
          minimumGroups[end] = candidateGroups;
          minimumSse[end] = candidateSse;
          previous[end] = start;
        }
      }
    }
    if (previous[size] < 0) {
      throw new IllegalStateException("No feasible capacity-first partitioning");
    }

    List<int[]> ranges = new ArrayList<>();
    for (int end = size; end > 0; end = previous[end]) {
      ranges.add(new int[]{previous[end], end});
    }
    Collections.reverse(ranges);

    List<JobGroup> jobs = new ArrayList<>();
    for (int[] range : ranges) {
      long bytes = 0L;
      for (int index = range[0]; index < range[1]; index++) {
        bytes += costs.get(index).getProfile().getTotalMemoryLoadBytes();
      }
      jobs.add(new JobGroup(
          jobs.size(),
          PartitionComplexityType.NORMAL,
          costs.subList(range[0], range[1]),
          bytes));
    }

    CostClass classification = new CostClass(
        0.0d,
        0.0d,
        Double.POSITIVE_INFINITY,
        Collections.emptyList(),
        costs);
    return new JobQueueExecutionPlan(classification, Collections.emptyList(), jobs);
  }

  private static List<PartCost> validatedSortedCosts(List<PartProfile> profiles, long budget) {
    List<PartCost> costs = new ArrayList<>();
    Set<Integer> ids = new HashSet<>();
    for (PartProfile profile : profiles) {
      if (profile == null || !ids.add(profile.getPartitionId())) {
        throw new IllegalArgumentException("Null or duplicate partition profile");
      }
      double score = profile.getPartitionIntensity();
      long bytes = profile.getTotalMemoryLoadBytes();
      if (!Double.isFinite(score) || score < 0.0d || bytes < 0L || bytes > budget) {
        throw new IllegalArgumentException(
            "Invalid intensity or infeasible partition: " + profile.getPartitionId());
      }
      costs.add(new PartCost(profile, score));
    }
    Collections.sort(costs);
    return costs;
  }

  private static JobQueueExecutionPlan emptyPlan(List<PartCost> costs) {
    CostClass classification = new CostClass(
        0.0d,
        0.0d,
        Double.POSITIVE_INFINITY,
        Collections.emptyList(),
        costs);
    return new JobQueueExecutionPlan(
        classification,
        Collections.emptyList(),
        Collections.emptyList());
  }
}
