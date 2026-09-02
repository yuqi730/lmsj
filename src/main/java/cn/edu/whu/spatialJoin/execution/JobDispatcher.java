package cn.edu.whu.spatialJoin.execution;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

/**
 * Thread-pool based dispatcher for future partition-level Spark jobs.
 *
 * <p>The dispatcher does not submit Spark jobs by itself yet. Instead, it
 * accepts a caller-provided runner function so the outer execution layer can
 * decide whether each queued item is submitted through Spark actions,
 * SparkLauncher, or another orchestration path.
 */
public final class JobDispatcher implements Closeable, Serializable {

  private static final long serialVersionUID = 1L;

  private final int threadPoolSize;
  private final int maxConcurrentJobs;
  private transient ExecutorService executorService;

  /** Resolved dispatch concurrency derived from explicit options or thread-pool capacity. */
  public static final class ResolvedConcurrency implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int threadPoolSize;
    private final int maxConcurrentJobs;
    private final String maxConcurrentJobsSource;

    private ResolvedConcurrency(int threadPoolSize, int maxConcurrentJobs, String maxConcurrentJobsSource) {
      this.threadPoolSize = Math.max(1, threadPoolSize);
      this.maxConcurrentJobs = Math.max(1, maxConcurrentJobs);
      this.maxConcurrentJobsSource = maxConcurrentJobsSource == null ? "unknown" : maxConcurrentJobsSource;
    }

    public int getThreadPoolSize() {
      return threadPoolSize;
    }

    public int getMaxConcurrentJobs() {
      return maxConcurrentJobs;
    }

    public String getMaxConcurrentJobsSource() {
      return maxConcurrentJobsSource;
    }
  }

  /** Optional trace container for resource-aware dispatch debugging. */
  public static final class DispatchTrace implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<DispatchEvent> events = Collections.synchronizedList(new ArrayList<>());

    public List<DispatchEvent> getEvents() {
      synchronized (events) {
        return new ArrayList<>(events);
      }
    }

    private void add(DispatchEvent event) {
      if (event != null) {
        events.add(event);
      }
    }
  }

  /** One submit/complete event captured from the resource-aware dispatch loop. */
  public static final class DispatchEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String eventType;
    private final int jobId;
    private final long jobLoadBytes;
    private final int activeJobsAfterEvent;
    private final long activeLoadBytesAfterEvent;

    private DispatchEvent(String eventType,
                          int jobId,
                          long jobLoadBytes,
                          int activeJobsAfterEvent,
                          long activeLoadBytesAfterEvent) {
      this.eventType = eventType;
      this.jobId = jobId;
      this.jobLoadBytes = Math.max(0L, jobLoadBytes);
      this.activeJobsAfterEvent = Math.max(0, activeJobsAfterEvent);
      this.activeLoadBytesAfterEvent = Math.max(0L, activeLoadBytesAfterEvent);
    }

    public String getEventType() {
      return eventType;
    }

    public int getJobId() {
      return jobId;
    }

    public long getJobLoadBytes() {
      return jobLoadBytes;
    }

    public int getActiveJobsAfterEvent() {
      return activeJobsAfterEvent;
    }

    public long getActiveLoadBytesAfterEvent() {
      return activeLoadBytesAfterEvent;
    }
  }

  public JobDispatcher(int threadPoolSize, int maxConcurrentJobs) {
    this.threadPoolSize = Math.max(1, threadPoolSize);
    this.maxConcurrentJobs = Math.max(1, maxConcurrentJobs);
    this.executorService = Executors.newFixedThreadPool(this.threadPoolSize);
  }

  /** Creates a dispatcher from the execution-related options in [[SpatialJoinOptions]]. */
  public static JobDispatcher fromOptions(SpatialJoinOptions options) {
    ResolvedConcurrency resolved = resolveConcurrency(options);
    return new JobDispatcher(resolved.getThreadPoolSize(), resolved.getMaxConcurrentJobs());
  }

  /** Resolves the effective dispatcher concurrency using thread-pool size plus an optional safety ceiling. */
  public static ResolvedConcurrency resolveConcurrency(SpatialJoinOptions options) {
    if (options == null) {
      return new ResolvedConcurrency(
          SpatialJoinConstants.Execution.DEFAULT_THREAD_POOL_SIZE,
          SpatialJoinConstants.Execution.DEFAULT_THREAD_POOL_SIZE,
          "auto(threadPoolSize)");
    }
    int threadPoolSize = Math.max(
        1,
        options.getInt(
            SpatialJoinConstants.Execution.THREAD_POOL_SIZE,
            SpatialJoinConstants.Execution.DEFAULT_THREAD_POOL_SIZE));
    if (options.contains(SpatialJoinConstants.Execution.MAX_CONCURRENT_JOBS)) {
      int explicitLimit = Math.max(
          1,
          options.getInt(
              SpatialJoinConstants.Execution.MAX_CONCURRENT_JOBS,
              SpatialJoinConstants.Execution.DEFAULT_MAX_CONCURRENT_JOBS));
      return new ResolvedConcurrency(
          threadPoolSize,
          Math.min(threadPoolSize, explicitLimit),
          "explicit");
    }
    return new ResolvedConcurrency(threadPoolSize, threadPoolSize, "auto(threadPoolSize)");
  }

  /** Returns the backing thread-pool size used for dispatch coordination. */
  public int getThreadPoolSize() {
    return threadPoolSize;
  }

  /** Returns the maximum number of jobs allowed to run concurrently. */
  public int getMaxConcurrentJobs() {
    return maxConcurrentJobs;
  }

  /**
   * Dispatches the planned job groups using the configured thread pool.
   *
   * <p>The semaphore models the resource-aware cap on how many Spark jobs are
   * allowed to be pushed concurrently.
   *
   * @param jobGroups planned job groups
   * @param runner caller-provided job runner
   * @param <T> runner result type
   * @return futures for all submitted job groups
   */
  public <T> List<Future<T>> dispatch(Collection<JobGroup> jobGroups, Function<JobGroup, T> runner) {
    ensureExecutor();
    List<Future<T>> futures = new ArrayList<>();
    if (jobGroups == null || runner == null) {
      return futures;
    }
    final Semaphore concurrencyGate = new Semaphore(maxConcurrentJobs);
    for (JobGroup next : jobGroups) {
      if (next == null) {
        continue;
      }
      final JobGroup jobGroup = next;
      futures.add(executorService.submit(new Callable<T>() {
        @Override
        public T call() throws Exception {
          concurrencyGate.acquire();
          try {
            return runner.apply(jobGroup);
          } finally {
            concurrencyGate.release();
          }
        }
      }));
    }
    return futures;
  }

  /**
   * Dispatches planned jobs while respecting both:
   * <ul>
   *   <li>the configured maximum number of concurrent jobs, and</li>
   *   <li>an upper bound on the total memory load of all currently active jobs.</li>
   * </ul>
   *
   * <p>Jobs are considered in the given dispatch order. The dispatcher pushes the next job only when:
   * <ul>
   *   <li>the number of active jobs is still below {@link #maxConcurrentJobs}, and</li>
   *   <li>adding the next job would not make the active total load exceed {@code maxActiveLoadBytes}.</li>
   * </ul>
   *
   * <p>If the queue head by itself is already larger than the load budget, it is still allowed to run alone
   * so the dispatcher never deadlocks on one oversized-but-planned job.
   *
   * @param jobGroups planned jobs in dispatch order
   * @param maxActiveLoadBytes maximum total memory load allowed across currently active jobs
   * @param runner caller-provided job runner
   * @param <T> runner result type
   * @return results in the same order as the input job queue
   */
  public <T> List<T> dispatchResourceAware(Collection<JobGroup> jobGroups,
                                           long maxActiveLoadBytes,
                                           Function<JobGroup, T> runner) {
    return dispatchResourceAware(jobGroups, maxActiveLoadBytes, null, runner);
  }

  /**
   * Resource-aware dispatch variant that optionally records the actual submit/complete trace.
   */
  public <T> List<T> dispatchResourceAware(Collection<JobGroup> jobGroups,
                                           long maxActiveLoadBytes,
                                           DispatchTrace trace,
                                           Function<JobGroup, T> runner) {
    ensureExecutor();
    List<T> orderedResults = new ArrayList<>();
    if (jobGroups == null || runner == null) {
      return orderedResults;
    }
    List<JobGroup> orderedJobs = new ArrayList<>();
    for (JobGroup jobGroup : jobGroups) {
      if (jobGroup != null) {
        orderedJobs.add(jobGroup);
        orderedResults.add(null);
      }
    }
    if (orderedJobs.isEmpty()) {
      return orderedResults;
    }

    long loadBudgetBytes = Math.max(1L, maxActiveLoadBytes);
    ExecutorCompletionService<IndexedJobResult<T>> completionService =
        new ExecutorCompletionService<>(executorService);

    boolean[] submitted = new boolean[orderedJobs.size()];
    int submittedJobs = 0;
    int activeJobs = 0;
    int completedJobs = 0;
    long activeLoadBytes = 0L;

    while (completedJobs < orderedJobs.size()) {
      while (submittedJobs < orderedJobs.size() && activeJobs < maxConcurrentJobs) {
        int nextJobIndex = findNextAdmissibleJobIndex(
            orderedJobs,
            submitted,
            activeJobs,
            activeLoadBytes,
            loadBudgetBytes);
        if (nextJobIndex < 0) {
          break;
        }
        JobGroup nextJob = orderedJobs.get(nextJobIndex);
        long nextJobLoadBytes = Math.max(0L, nextJob.getTotalMemoryLoadBytes());
        final int jobIndex = nextJobIndex;
        completionService.submit(new Callable<IndexedJobResult<T>>() {
          @Override
          public IndexedJobResult<T> call() {
            return new IndexedJobResult<>(
                jobIndex,
                nextJob.getJobId(),
                nextJobLoadBytes,
                runner.apply(nextJob));
          }
        });
        submitted[jobIndex] = true;
        submittedJobs++;
        activeJobs++;
        activeLoadBytes += nextJobLoadBytes;
        recordDispatchEvent(trace, "submit", nextJob.getJobId(), nextJobLoadBytes, activeJobs, activeLoadBytes);
      }

      if (activeJobs == 0 && submittedJobs < orderedJobs.size()) {
        int nextJobIndex = findFirstPendingJobIndex(submitted);
        if (nextJobIndex < 0) {
          break;
        }
        JobGroup nextJob = orderedJobs.get(nextJobIndex);
        long nextJobLoadBytes = Math.max(0L, nextJob.getTotalMemoryLoadBytes());
        final int jobIndex = nextJobIndex;
        completionService.submit(new Callable<IndexedJobResult<T>>() {
          @Override
          public IndexedJobResult<T> call() {
            return new IndexedJobResult<>(
                jobIndex,
                nextJob.getJobId(),
                nextJobLoadBytes,
                runner.apply(nextJob));
          }
        });
        submitted[jobIndex] = true;
        submittedJobs++;
        activeJobs++;
        activeLoadBytes += nextJobLoadBytes;
        recordDispatchEvent(trace, "submit", nextJob.getJobId(), nextJobLoadBytes, activeJobs, activeLoadBytes);
      }

      if (activeJobs == 0) {
        break;
      }

      try {
        IndexedJobResult<T> completed = completionService.take().get();
        orderedResults.set(completed.index, completed.value);
        activeJobs--;
        completedJobs++;
        activeLoadBytes = Math.max(0L, activeLoadBytes - completed.jobLoadBytes);
        recordDispatchEvent(trace, "complete", completed.jobId, completed.jobLoadBytes, activeJobs, activeLoadBytes);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while dispatching resource-aware partition jobs", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Failed to execute resource-aware partition job", e.getCause() == null ? e : e.getCause());
      }
    }
    return orderedResults;
  }

  private static int findNextAdmissibleJobIndex(List<JobGroup> orderedJobs,
                                                boolean[] submitted,
                                                int activeJobs,
                                                long activeLoadBytes,
                                                long loadBudgetBytes) {
    if (orderedJobs == null || submitted == null) {
      return -1;
    }
    for (int i = 0; i < orderedJobs.size(); i++) {
      if (i < submitted.length && submitted[i]) {
        continue;
      }
      JobGroup nextJob = orderedJobs.get(i);
      if (nextJob == null) {
        continue;
      }
      long nextJobLoadBytes = Math.max(0L, nextJob.getTotalMemoryLoadBytes());
      boolean canSubmitNow =
          activeJobs == 0 || activeLoadBytes + nextJobLoadBytes <= loadBudgetBytes;
      if (canSubmitNow) {
        return i;
      }
    }
    return -1;
  }

  private static int findFirstPendingJobIndex(boolean[] submitted) {
    if (submitted == null) {
      return -1;
    }
    for (int i = 0; i < submitted.length; i++) {
      if (!submitted[i]) {
        return i;
      }
    }
    return -1;
  }

  @Override
  /** Shuts down the internal thread pool when the dispatcher is no longer needed. */
  public void close() {
    if (executorService != null) {
      executorService.shutdown();
      executorService = null;
    }
  }

  /** Lazily recreates the executor after deserialization or an earlier close. */
  private void ensureExecutor() {
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(threadPoolSize);
    }
  }

  /** Internal completion payload used by the resource-aware dispatch loop. */
  private static final class IndexedJobResult<T> {
    private final int index;
    private final int jobId;
    private final long jobLoadBytes;
    private final T value;

    private IndexedJobResult(int index, int jobId, long jobLoadBytes, T value) {
      this.index = index;
      this.jobId = jobId;
      this.jobLoadBytes = Math.max(0L, jobLoadBytes);
      this.value = value;
    }
  }

  private static void recordDispatchEvent(DispatchTrace trace,
                                          String eventType,
                                          int jobId,
                                          long jobLoadBytes,
                                          int activeJobsAfterEvent,
                                          long activeLoadBytesAfterEvent) {
    if (trace == null) {
      return;
    }
    trace.add(new DispatchEvent(
        eventType,
        jobId,
        jobLoadBytes,
        activeJobsAfterEvent,
        activeLoadBytesAfterEvent));
  }
}
