package cn.edu.whu.spatialJoin.common;

public final class SpatialJoinConstants {

  private SpatialJoinConstants() {
  }

  public static final class SparkDefaults {
    /** Executor memory used when no explicit Spark setting is provided. */
    public static final String EXECUTOR_MEMORY_KEY = "spark.executor.memory";
    public static final String EXECUTOR_MEMORY = "1g";
    /** Driver memory used by local-mode execution where no separate executors exist. */
    public static final String DRIVER_MEMORY_KEY = "spark.driver.memory";
    public static final String DRIVER_MEMORY = "1g";
    /** Number of executors contributing to the total execution-memory budget M. */
    public static final String EXECUTOR_INSTANCES_KEY = "spark.executor.instances";
    public static final int EXECUTOR_INSTANCES = 1;
    /** Dynamic-allocation fallback for estimating how many executors may run concurrently. */
    public static final String DYNAMIC_ALLOCATION_MAX_EXECUTORS_KEY = "spark.dynamicAllocation.maxExecutors";
    /** Number of cores per executor, used when deriving one-partition memory targets. */
    public static final String EXECUTOR_CORES_KEY = "spark.executor.cores";
    public static final int EXECUTOR_CORES = 1;
    /** Number of CPU cores reserved by each Spark task. */
    public static final String TASK_CPUS_KEY = "spark.task.cpus";
    public static final int TASK_CPUS = 1;
    /** Spark execution-memory fraction used by LAHilbert when deriving per-partition thresholds. */
    public static final String MEMORY_FRACTION_KEY = "spark.memory.fraction";
    public static final double MEMORY_FRACTION = 0.6d;
    /** Spark storage fraction alpha; execution budget is derived from M * (1 - alpha). */
    public static final String STORAGE_FRACTION_KEY = "spark.memory.storageFraction";
    public static final double STORAGE_FRACTION = 0.5d;

    private SparkDefaults() {
    }
  }

  public static final class LAHilbert {
    /** Explicit global overlap-domain minimum coordinates; overrides summary-derived overlap when present. */
    public static final String GLOBAL_MIN_COORDS = "lahilbert.global.min";
    /** Explicit global overlap-domain maximum coordinates; overrides summary-derived overlap when present. */
    public static final String GLOBAL_MAX_COORDS = "lahilbert.global.max";
    /** Left dataset summary MBR minimum coordinates used to derive the overlap domain. */
    public static final String LEFT_MIN_COORDS = "lahilbert.left.min";
    /** Left dataset summary MBR maximum coordinates used to derive the overlap domain. */
    public static final String LEFT_MAX_COORDS = "lahilbert.left.max";
    /** Right dataset summary MBR minimum coordinates used to derive the overlap domain. */
    public static final String RIGHT_MIN_COORDS = "lahilbert.right.min";
    /** Right dataset summary MBR maximum coordinates used to derive the overlap domain. */
    public static final String RIGHT_MAX_COORDS = "lahilbert.right.max";
    /** Left dataset total memory-load size used when estimating the initial Hilbert order. */
    public static final String LEFT_SUMMARY_SIZE = "lahilbert.left.size";
    /** Right dataset total memory-load size used when estimating the initial Hilbert order. */
    public static final String RIGHT_SUMMARY_SIZE = "lahilbert.right.size";
    /** Left dataset total feature count used by count-based recursive refinement. */
    public static final String LEFT_SUMMARY_FEATURES = "lahilbert.left.numFeatures";
    /** Right dataset total feature count used by count-based recursive refinement. */
    public static final String RIGHT_SUMMARY_FEATURES = "lahilbert.right.numFeatures";
    /** Optional initial Hilbert order l; when absent the partitioner derives it from dataset summaries. */
    public static final String INITIAL_ORDER = "lahilbert.initial.order";
    /** Target per-partition memory-load threshold that triggers LAHilbert refinement. */
    public static final String PARTITION_BYTES = "lahilbert.partition.bytes";
    /** Safety coefficient applied when deriving per-partition memory budget from Spark resources. */
    public static final String SAFETY_FRACTION = "lahilbert.memory.safetyFraction";
    /** Additional scaling factor applied to estimated partition load before refinement decisions. */
    public static final String LOAD_SCALE_FACTOR = "lahilbert.load.scaleFactor";
    /** Minimum branching factor when one overloaded partition is refined into child partitions. */
    public static final String REFINE_FANOUT = "lahilbert.refine.fanout";
      /** Maximum recursive refinement depth allowed for one LAHilbert partition path; use -1 for unlimited. */
      public static final String MAX_REFINEMENT_DEPTH = "lahilbert.refine.maxDepth";
    /**
     * Recursive refinement strategy used after the initial Hilbert cells have been built.
     *
     * <p>The default keeps the project's original adaptive-axis + memory-load-driven refinement.
     * The optional object-center-offset variant keeps the same load-driven refinement but chooses
     * its split axis from object MBR-center offsets relative to the current partition.
     */
    public static final String REFINE_STRATEGY = "lahilbert.refine.strategy";
    /** Current project baseline: split along the longest side and keep refining while partition load exceeds the byte threshold. */
    public static final String REFINE_STRATEGY_ADAPTIVE_AXIS_LOAD = "adaptive-axis-load";
    /** One-axis load refinement driven by object MBR-center offsets from the partition lower-left corner. */
    public static final String REFINE_STRATEGY_OBJECT_CENTER_OFFSET_AXIS_LOAD =
        "object-center-offset-axis-load";
    /** Equal four-way quadtree-style split refined by a feature-count threshold derived from average feature bytes. */
    public static final String REFINE_STRATEGY_EQUAL_QUADTREE_COUNT = "equal-quadtree-count";
    /** Equal four-way quadtree-style split refined directly by partition memory load. */
    public static final String REFINE_STRATEGY_EQUAL_QUADTREE_LOAD = "equal-quadtree-load";
    /**
     * Whether join-oriented recursive refinement should prune a partition when the projected
     * aggregate MBRs of its left and right feature sets are disjoint.
     */
    public static final String REFINE_AGGREGATE_MBR_PRUNING_ENABLED =
        "lahilbert.refine.aggregateMbrPruning.enabled";
    /**
     * Whether exact recursive refinement may remove one-sided or aggregate-MBR-disjoint
     * candidate leaves before the final effective-partition plan is built.
     */
    public static final String REFINE_EFFECTIVE_PRUNING_ENABLED =
        "lahilbert.refine.effectivePruning.enabled";
    /**
     * Whether one parent partition may greedily merge adjacent effective child partitions again
     * after a split when the merged unique load still fits inside the same target threshold.
     */
    public static final String COALESCE_SMALL_SIBLINGS_ENABLED = "lahilbert.refine.coalesceSmallSiblings.enabled";
    /** Object-to-leaf routing implementation used after partition envelopes are fixed. */
    public static final String PARTITION_ROUTING_MODE = "lahilbert.partitionRouting.mode";
    /** Historical implementation that tests every leaf envelope for every object. */
    public static final String PARTITION_ROUTING_MODE_LINEAR = "linear";
    /** Direct initial-grid lookup followed by checks of leaves belonging to the touched grid cells. */
    public static final String PARTITION_ROUTING_MODE_GRID_DIRECTORY = "grid-directory";
    /** Runs both routing implementations and fails immediately if their partition IDs differ. */
    public static final String PARTITION_ROUTING_MODE_VALIDATE = "validate";
    /** Human-readable explicit partition threshold used by local debug apps and manual examples. */
    public static final String DEFAULT_PARTITION_SIZE = "32m";
    /** Numeric companion of {@link #DEFAULT_PARTITION_SIZE}; used only as a byte-level fallback. */
    public static final long DEFAULT_PARTITION_BYTES = 32L * 1024L * 1024L;
    public static final double DEFAULT_SAFETY_FRACTION = 0.8d;
    public static final double DEFAULT_LOAD_SCALE_FACTOR = 1.0d;
    public static final int DEFAULT_REFINE_FANOUT = 2;
    public static final int DEFAULT_MAX_REFINEMENT_DEPTH = 5;
    public static final String DEFAULT_REFINE_STRATEGY = REFINE_STRATEGY_ADAPTIVE_AXIS_LOAD;
    public static final boolean DEFAULT_REFINE_AGGREGATE_MBR_PRUNING_ENABLED = false;
    public static final boolean DEFAULT_REFINE_EFFECTIVE_PRUNING_ENABLED = true;
    public static final boolean DEFAULT_COALESCE_SMALL_SIBLINGS_ENABLED = false;
    public static final String DEFAULT_PARTITION_ROUTING_MODE = PARTITION_ROUTING_MODE_GRID_DIRECTORY;
    /** Hard upper bound that keeps automatically derived Hilbert orders in a manageable range. */
    public static final int MAX_ORDER = 20;

    private LAHilbert() {
    }
  }

  public static final class KDTree {
    /** Recursive refinement strategy used by KD-tree partitioning. */
    public static final String REFINE_STRATEGY = "kdtree.refine.strategy";
    /** Historical behavior: keep splitting until a requested fixed leaf count is reached. */
    public static final String REFINE_STRATEGY_FIXED_COUNT = "fixed-count";
    /** Stop recursive KD splits once the estimated feature count of one node fits under the derived threshold. */
    public static final String REFINE_STRATEGY_ADAPTIVE_COUNT = "adaptive-count";
    /** Stop recursive KD splits once the estimated memory load of one node fits under the byte threshold. */
    public static final String REFINE_STRATEGY_ADAPTIVE_LOAD = "adaptive-load";
    /** Optional explicit byte threshold for adaptive KD refinement; otherwise falls back to the shared LAH threshold logic. */
    public static final String PARTITION_BYTES = "kdtree.partition.bytes";
    /**
     * Maximum recursive depth allowed for adaptive KD refinement.
     *
     * <p>A negative value means "no explicit depth cap": adaptive refinement keeps splitting
     * while the count/load threshold is still violated and the node can still be meaningfully
     * separated into two non-empty children.
     */
    public static final String MAX_REFINEMENT_DEPTH = "kdtree.refine.maxDepth";
    public static final String DEFAULT_REFINE_STRATEGY = REFINE_STRATEGY_FIXED_COUNT;
    public static final int DEFAULT_MAX_REFINEMENT_DEPTH = -1;

    private KDTree() {
    }
  }

  public static final class STRTree {
    /** Recursive refinement strategy used by STR-tree partitioning. */
    public static final String REFINE_STRATEGY = "str.refine.strategy";
    /** Historical behavior: bulk-load a fixed-count STR tree from the requested bucket count. */
    public static final String REFINE_STRATEGY_FIXED_COUNT = "fixed-count";
    /** Stop recursive STR grouping once the estimated feature count of one node fits under the derived threshold. */
    public static final String REFINE_STRATEGY_ADAPTIVE_COUNT = "adaptive-count";
    /** Stop recursive STR grouping once the estimated memory load of one node fits under the byte threshold. */
    public static final String REFINE_STRATEGY_ADAPTIVE_LOAD = "adaptive-load";
    /** Optional explicit byte threshold for adaptive STR refinement; otherwise falls back to the shared LAH threshold logic. */
    public static final String PARTITION_BYTES = "str.partition.bytes";
    /**
     * Maximum recursive depth allowed for adaptive STR refinement.
     *
     * <p>A negative value means "no explicit depth cap": adaptive refinement keeps regrouping
     * while the count/load threshold is still violated and one node can still be split into
     * multiple non-empty child groups.
     */
    public static final String MAX_REFINEMENT_DEPTH = "str.refine.maxDepth";
    /** Maximum number of direct children one adaptive STR node may produce in one grouping step. */
    public static final String MAX_CHILDREN = "str.refine.maxChildren";
    public static final String DEFAULT_REFINE_STRATEGY = REFINE_STRATEGY_FIXED_COUNT;
    public static final int DEFAULT_MAX_REFINEMENT_DEPTH = -1;
    public static final int DEFAULT_MAX_CHILDREN = 16;

    private STRTree() {
    }
  }

  public static final class Operator {
    /** Top-level operator selector: read, partition, or spatial_join. */
    public static final String TYPE = "sj.operator.type";
    /** Generic single-input path used by read-oriented entry points and debug apps. */
    public static final String INPUT_PATH = "sj.input.path";
    /** Generic single-input format hint such as wkt, geojson, or shapefile. */
    public static final String INPUT_FORMAT = "sj.input.format";
    /** Partitioner type requested by the operator layer, e.g. lahilbert. */
    public static final String PARTITIONER_TYPE = "sj.partitioner.type";
    /** Loose partition-count hint supplied to partition construction. */
    public static final String NUM_PARTITIONS = "sj.partitioner.numPartitions";
    /** Whether the caller requests disjoint partitioning semantics. */
    public static final String DISJOINT = "sj.partitioner.disjoint";
    /** Left input path used by the spatial-join operator path. */
    public static final String LEFT_INPUT_PATH = "sj.join.left.path";
    /** Right input path used by the spatial-join operator path. */
    public static final String RIGHT_INPUT_PATH = "sj.join.right.path";
    /** Left input format hint for the spatial-join operator path. */
    public static final String LEFT_INPUT_FORMAT = "sj.join.left.format";
    /** Right input format hint for the spatial-join operator path. */
    public static final String RIGHT_INPUT_FORMAT = "sj.join.right.format";
    /** Fraction of input features retained by online experiment sampling. */
    public static final String INPUT_SAMPLE_FRACTION = "sj.input.sample.fraction";
    /** Seed used by deterministic online experiment sampling. */
    public static final String INPUT_SAMPLE_SEED = "sj.input.sample.seed";
    /** Spatial predicate name currently requested for join execution. */
    public static final String JOIN_PREDICATE = "sj.join.predicate";
    /** Join algorithm label used to select the current execution implementation. */
    public static final String JOIN_ALGORITHM = "sj.join.algorithm";
    /** Simplification threshold forwarded to the local partition-level join algorithm. */
    public static final String JOIN_SIMPLIFICATION_THRESHOLD = "sj.join.simplification.threshold";
    /** Whether local joins should quad-split complex geometries before plane-sweep filtering. */
    public static final String JOIN_SIMPLIFICATION_ENABLED = "sj.join.simplification.enabled";
    /** Whether complex geometries should be split before summaries, partitioning, and materialization. */
    public static final String JOIN_PRE_PARTITION_SPLIT_ENABLED = "sj.join.prePartitionSplit.enabled";
    /** Maximum number of fragments produced by pre-partition splitting for one source geometry. */
    public static final String JOIN_PRE_PARTITION_SPLIT_MAX_FRAGMENTS = "sj.join.prePartitionSplit.maxFragments";
    /** Maximum quad-split depth used by pre-partition splitting for one source geometry. */
    public static final String JOIN_PRE_PARTITION_SPLIT_MAX_DEPTH = "sj.join.prePartitionSplit.maxDepth";
    /** Plane-sweep scan-axis mode used by local partition joins. */
    public static final String JOIN_PLANE_SWEEP_AXIS = "sj.join.planeSweep.axis";
    /** Plane-sweep implementation variant used by local partition joins. */
    public static final String JOIN_PLANE_SWEEP_IMPLEMENTATION = "sj.join.planeSweep.implementation";
    /** Lightweight OID attribute kept in join-ready features after non-geometry attributes are dropped. */
    public static final String JOIN_OID_ATTRIBUTE = "oid";

    /** Force the sweep to order rectangles by their minimum x coordinate. */
    public static final String JOIN_PLANE_SWEEP_AXIS_X = "x";
    /** Force the sweep to order rectangles by their minimum y coordinate. */
    public static final String JOIN_PLANE_SWEEP_AXIS_Y = "y";
    /** Choose between x and y automatically based on the shorter average MBR span. */
    public static final String JOIN_PLANE_SWEEP_AXIS_AUTO = "auto";
    /** Sweep along the main diagonal of the current object space or partition coverage. */
    public static final String JOIN_PLANE_SWEEP_AXIS_DIAGONAL = "diagonal";

    /** Current project baseline plane sweep implementation. */
    public static final String JOIN_PLANE_SWEEP_IMPLEMENTATION_CLASSIC = "classic";
    /** Sliding-window plane sweep that keeps the current candidate window in primitive index arrays. */
    public static final String JOIN_PLANE_SWEEP_IMPLEMENTATION_SLIDING_WINDOW = "sliding-window";

    public static final String DEFAULT_TYPE = "read";
    public static final String DEFAULT_PARTITIONER_TYPE = "lahilbert";
    public static final String DEFAULT_JOIN_PREDICATE = "intersects";
    public static final String DEFAULT_JOIN_ALGORITHM = "local-plane-sweep";
    public static final int DEFAULT_NUM_PARTITIONS = 1;
    public static final boolean DEFAULT_DISJOINT = false;
    public static final double DEFAULT_INPUT_SAMPLE_FRACTION = 1.0d;
    public static final long DEFAULT_INPUT_SAMPLE_SEED = 2026052501L;
    public static final int DEFAULT_JOIN_SIMPLIFICATION_THRESHOLD = 512;
    public static final boolean DEFAULT_JOIN_SIMPLIFICATION_ENABLED = false;
    public static final boolean DEFAULT_JOIN_PRE_PARTITION_SPLIT_ENABLED = false;
    public static final int DEFAULT_JOIN_PRE_PARTITION_SPLIT_MAX_FRAGMENTS = 512;
    public static final int DEFAULT_JOIN_PRE_PARTITION_SPLIT_MAX_DEPTH = 8;
    public static final String DEFAULT_JOIN_PLANE_SWEEP_AXIS = JOIN_PLANE_SWEEP_AXIS_AUTO;
    public static final String DEFAULT_JOIN_PLANE_SWEEP_IMPLEMENTATION = JOIN_PLANE_SWEEP_IMPLEMENTATION_CLASSIC;

    private Operator() {
    }
  }

  public static final class Execution {
    /** Working-set mode: memory reuses cached OID-feature RDDs, disk-hybrid materializes partition data to external storage. */
    public static final String WORKING_SET_MODE = "sj.execution.workingSetMode";
    /** Size of the local coordination thread pool used to dispatch planned jobs. */
    public static final String THREAD_POOL_SIZE = "sj.execution.threadPoolSize";
    /**
     * Optional safety ceiling on how many planned jobs may be pushed concurrently.
     *
     * <p>When callers do not set this option explicitly, the dispatcher now derives the
     * effective concurrent-job ceiling from the dispatch thread-pool size. The numeric
     * default below is therefore only a last-resort fallback when no thread-pool signal
     * is available.
     */
    public static final String MAX_CONCURRENT_JOBS = "sj.execution.maxConcurrentJobs";
    /**
     * Upper bound on how many effective partitions may be packed into one job.
     *
     * <p>When callers do not set this option explicitly, the planner now derives the effective limit
     * from Spark parallelism. The constant default below is therefore only a last-resort fallback when
     * no usable parallelism signal is available.
     */
    public static final String MAX_PARTITIONS_PER_JOB = "sj.execution.maxPartitionsPerJob";
    /** Complexity-estimation mode used when building AbstractFeature and partition profiles. */
    public static final String COMPLEXITY_MODE = "sj.execution.complexity.mode";
    /**
     * Feature-load estimate mode used after partition boundaries are fixed, when deriving final
     * effective partition profiles for task-unit packing and concurrent dispatch.
     *
     * <p>The default keeps the current conservative behavior where retained binary geometry and
     * later materialized geometry are both counted. The looser materialized-only mode is intended
     * for experiments that treat runtime geometries as the main execution pressure and do not want
     * to budget separately for the retained binary payload.
     */
    public static final String PARTITION_LOAD_ESTIMATE_MODE = "sj.execution.partitionLoadEstimateMode";
    /**
     * Partition computation-intensity mode used by job construction.
     *
     * <p>This is intentionally separated from {@link #PARTITION_LOAD_ESTIMATE_MODE}: the latter
     * estimates memory pressure in bytes, while this option controls the scalar computation
     * strength used to classify and group partitions into jobs.
     */
    public static final String PARTITION_INTENSITY_MODE = "sj.execution.partitionIntensityMode";
    /** Final feature-to-partition assignment strategy applied after the resource-aware partition boundaries are fixed. */
    public static final String PARTITION_ASSIGNMENT_STRATEGY = "sj.execution.partitionAssignment.strategy";
    /** Lambda in the HIGH/NORMAL split rule max(lambda * median, quantile). */
    public static final String HIGH_COMPLEXITY_EXPANSION_FACTOR = "sj.execution.highComplexity.lambda";
    /** Quantile rho in the HIGH/NORMAL split rule max(lambda * median, quantile). */
    public static final String HIGH_COMPLEXITY_QUANTILE = "sj.execution.highComplexity.quantile";
    /** Relative band below the threshold that is still treated as near-threshold HIGH to avoid brittle hard splits. */
    public static final String HIGH_COMPLEXITY_GRAY_ZONE_RATIO = "sj.execution.highComplexity.grayZoneRatio";
      /** Explicit job-memory budget override; otherwise the planner derives M * (1 - alpha). */
      public static final String JOB_MEMORY_BUDGET = "sj.execution.job.memoryBudget";
      /**
       * Optional fraction of the global execution-memory budget used as the per-job packing budget.
       *
       * <p>When unset or non-positive, the legacy behavior is preserved: the same derived budget is
       * used for both job packing and active-job dispatch. When set, the planner packs one job with
       * {@code globalExecutionBudget * ratio}, while dispatch still uses the full global budget.
       */
      public static final String JOB_MEMORY_BUDGET_RATIO = "sj.execution.job.memoryBudgetRatio";
      /**
       * Maximum compact OID-to-partition broadcast size allowed when materializing the final
       * `(partitionId, feature)` working set. Larger assignment tables fall back to the
       * existing OID-join replay path to avoid excessive driver/executor pressure.
       */
      public static final String PREPARED_ASSIGNMENT_BROADCAST_MAX_BYTES =
          "sj.execution.preparedAssignmentBroadcast.maxBytes";
      /** Job execution mode: concurrent uses the dispatcher, sequential runs jobs one by one for verification. */
      public static final String JOB_EXECUTION_MODE = "sj.execution.job.executionMode";
    /** Optional base path where disk-hybrid execution materializes final partition data and metadata. */
    public static final String DISK_MATERIALIZATION_BASE_PATH = "sj.execution.disk.basePath";
    /** Whether disk-hybrid debug flows should reload materialized partition files and compare them with the pre-write data. */
    public static final String DISK_VALIDATE_MATERIALIZED = "sj.execution.disk.validateMaterialized";
    /** Whether disk-hybrid execution should keep materialized partition files after the run finishes. */
    public static final String DISK_KEEP_MATERIALIZED = "sj.execution.disk.keepMaterialized";
    /**
     * Whether dual-replication disk-hybrid preparation may construct candidate partitions from
     * aggregated synopsis cells instead of one lightweight descriptor per source object.
     *
     * <p>The default preserves the established scalable execution path. Partition-refinement
     * experiments can disable it when child membership must be derived from every original
     * object's lightweight MBR rather than from an aggregated cell envelope.
     */
    public static final String DISK_SYNOPSIS_PREPARATION_ENABLED =
        "sj.execution.disk.synopsisPreparation.enabled";
    /**
     * Whether the operator should validate geometries immediately after reading and drop invalid
     * records before summaries, partitioning, materialization, and job transport.
     */
    public static final String FILTER_INVALID_GEOMETRIES_EARLY = "sj.execution.filterInvalidGeometriesEarly";
    /** Whether sequential debug runs should print fine-grained per-job timing breakdowns. */
    public static final String DEBUG_PROFILE_JOBS = "sj.execution.debug.profileJobs";
    /** Whether partition-local join execution should print per-partition audit counters for debugging. */
    public static final String DEBUG_AUDIT_PARTITION_JOIN = "sj.execution.debug.auditPartitionJoin";
    /** Object-level pair de-duplication mode after partition-local duplicate avoidance. */
    public static final String OBJECT_PAIR_DEDUP_MODE = "sj.execution.objectPairDedup.mode";
    /** Keep the current reference-point-only counting path. */
    public static final String OBJECT_PAIR_DEDUP_MODE_NONE = "none";
    /** Emit original object-id pairs to external files for downstream global recovery. */
    public static final String OBJECT_PAIR_DEDUP_MODE_GLOBAL = "global";
    /** Optional base directory where global object-pair files are written. */
    public static final String OBJECT_PAIR_DEDUP_OUTPUT_BASE_PATH = "sj.execution.objectPairDedup.outputBasePath";
    /** Whether global object-pair recovery time is included in compute-stage elapsed time. */
    public static final String OBJECT_PAIR_DEDUP_COUNT_IN_COMPUTE_TIME =
        "sj.execution.objectPairDedup.countInComputeTime";
    /** Beta weight in the job deviation formula beta * (max - avg) + gamma * variance. */
    public static final String DEVIATION_MAX_AVG_WEIGHT = "sj.execution.job.deviation.beta";
    /** Gamma weight in the job deviation formula beta * (max - avg) + gamma * variance. */
    public static final String DEVIATION_VARIANCE_WEIGHT = "sj.execution.job.deviation.gamma";
    /** Materialize the final `(partitionId, feature)` working set in serialized memory for later direct partition pruning. */
    public static final String WORKING_SET_MODE_MEMORY = "memory";
    /** Materialize final partition data to external storage and reload only target partition files per job. */
    public static final String WORKING_SET_MODE_DISK_HYBRID = "disk-hybrid";
    /** Human-friendly alias for disk-hybrid mode used by debug entry points. */
    public static final String WORKING_SET_MODE_DISK_MEMORY = "disk-memory";
    /** Fast complexity mode that keeps only the cheapest binary-derived shape cues. */
    public static final String COMPLEXITY_MODE_FAST = "fast";
    /** Balanced complexity mode that mixes binary-derived exact area with lighter approximations. */
    public static final String COMPLEXITY_MODE_BALANCED = "balanced";
    /** Exact-binary complexity mode that computes complexity from precise binary-derived geometry metadata. */
    public static final String COMPLEXITY_MODE_EXACT_BINARY = "exact-binary";
    /** Baseline intensity: sum of the per-feature SizeFunctions complexity on both sides. */
    public static final String PARTITION_INTENSITY_MODE_SIZEFUNCTION = "sizefunction";
    /** Candidate-aware intensity: MBR candidates times log of average left/right feature complexity. */
    public static final String PARTITION_INTENSITY_MODE_CANDIDATE_AVG_COMPLEXITY =
        "candidate-avg-complexity";
    /**
     * CI hybrid intensity: normalized feature count plus normalized candidate-average vertex intensity.
     *
     * <p>This is the execution-time counterpart of the featureCountCandidateAvgVertexIntensity
     * metric used by the correlation and task-unit grouping experiments.
     */
    public static final String PARTITION_INTENSITY_MODE_CI_HYBRID = "ci-hybrid";
    /** Alpha weight of normalized feature count in the CI hybrid partition-intensity mode. */
    public static final String PARTITION_INTENSITY_CI_ALPHA = "sj.execution.partitionIntensity.ciAlpha";
    /**
     * Load estimate used only while constructing and recursively refining partition boundaries.
     *
     * <p>This is intentionally separate from {@link #PARTITION_LOAD_ESTIMATE_MODE}, which describes
     * the conservative execution load written into final partition profiles for task-unit planning.
     */
    public static final String PARTITION_REFINEMENT_LOAD_ESTIMATE_MODE =
        "sj.execution.partitionRefinementLoadEstimateMode";
    /** Conservative execution-load estimate: retained binary geometry plus materialized runtime geometry. */
    public static final String LOAD_ESTIMATE_MODE_CONSERVATIVE_HEAP = "conservative-heap";
    /** Looser execution-load estimate: only the materialized runtime geometry contribution is counted. */
    public static final String LOAD_ESTIMATE_MODE_MATERIALIZED_GEOMETRY = "materialized-geometry";
    /** Current project baseline: both sides are replicated to every final partition they intersect. */
    public static final String PARTITION_ASSIGNMENT_STRATEGY_DUAL_REPLICATION = "dual-replication";
    /** Two-phase variant: one heavier side is still fully replicated first, then the other side is filtered by anchor-side aggregate MBRs. */
    public static final String PARTITION_ASSIGNMENT_STRATEGY_ANCHOR_REPLICATION_FILTER = "anchor-replication-filter";
    /** Canonical assignment-strategy names accepted by the current execution layer. */
    public static final String[] SUPPORTED_PARTITION_ASSIGNMENT_STRATEGIES = new String[]{
        PARTITION_ASSIGNMENT_STRATEGY_DUAL_REPLICATION,
        PARTITION_ASSIGNMENT_STRATEGY_ANCHOR_REPLICATION_FILTER
    };
    /** Resource-aware concurrent execution of multiple planned jobs. */
    public static final String JOB_EXECUTION_MODE_CONCURRENT = "concurrent";
    /** Deterministic sequential execution of planned jobs, useful for full-pipeline verification. */
    public static final String JOB_EXECUTION_MODE_SEQUENTIAL = "sequential";
    public static final int DEFAULT_THREAD_POOL_SIZE = 2;
    public static final int DEFAULT_MAX_CONCURRENT_JOBS = 2;
    /**
     * Last-resort fallback upper bound on partitions packed into one job.
     *
     * <p>The planner now prefers Spark parallelism as the default source of this limit. This numeric value is
     * only used when no explicit option and no usable Spark parallelism hint are available.
     */
    public static final int DEFAULT_MAX_PARTITIONS_PER_JOB = 128;
    public static final String DEFAULT_WORKING_SET_MODE = WORKING_SET_MODE_MEMORY;

    public static final String DEFAULT_COMPLEXITY_MODE = COMPLEXITY_MODE_EXACT_BINARY;
    //public static final String DEFAULT_COMPLEXITY_MODE = COMPLEXITY_MODE_BALANCED;

    public static final String DEFAULT_PARTITION_LOAD_ESTIMATE_MODE = LOAD_ESTIMATE_MODE_CONSERVATIVE_HEAP;
    //public static final String DEFAULT_PARTITION_LOAD_ESTIMATE_MODE = LOAD_ESTIMATE_MODE_MATERIALIZED_GEOMETRY;
    public static final String DEFAULT_PARTITION_REFINEMENT_LOAD_ESTIMATE_MODE =
        LOAD_ESTIMATE_MODE_MATERIALIZED_GEOMETRY;

    public static final String DEFAULT_PARTITION_INTENSITY_MODE = PARTITION_INTENSITY_MODE_CI_HYBRID;
    //public static final String DEFAULT_PARTITION_INTENSITY_MODE = PARTITION_INTENSITY_MODE_CANDIDATE_AVG_COMPLEXITY;
    //public static final String DEFAULT_PARTITION_INTENSITY_MODE = PARTITION_INTENSITY_MODE_SIZEFUNCTION;
    public static final double DEFAULT_PARTITION_INTENSITY_CI_ALPHA = 0.6d;

    public static final String DEFAULT_PARTITION_ASSIGNMENT_STRATEGY = PARTITION_ASSIGNMENT_STRATEGY_DUAL_REPLICATION;
      public static final String DEFAULT_JOB_EXECUTION_MODE = JOB_EXECUTION_MODE_CONCURRENT;
      public static final String DEFAULT_DISK_MATERIALIZATION_BASE_PATH = "D:/sj_temp";
      public static final double DEFAULT_HIGH_COMPLEXITY_EXPANSION_FACTOR = 1.5d;
      public static final double DEFAULT_HIGH_COMPLEXITY_QUANTILE = 0.9d;
      public static final double DEFAULT_HIGH_COMPLEXITY_GRAY_ZONE_RATIO = 0.01d;
      public static final String DEFAULT_PREPARED_ASSIGNMENT_BROADCAST_MAX_BYTES = "256m";
      public static final long DEFAULT_PREPARED_ASSIGNMENT_BROADCAST_MAX_BYTES_BYTES =
          256L * 1024L * 1024L;
      /** Legacy explicit override value kept only as a parser fallback when the option is set manually. */
      public static final String DEFAULT_JOB_MEMORY_BUDGET = "256m";
      public static final long DEFAULT_JOB_MEMORY_BUDGET_BYTES = 256L * 1024L * 1024L;
      public static final double DEFAULT_JOB_MEMORY_BUDGET_RATIO = 0.0d;
    public static final double DEFAULT_DEVIATION_MAX_AVG_WEIGHT = 1.0d;
    public static final double DEFAULT_DEVIATION_VARIANCE_WEIGHT = 1.0d;
    public static final boolean DEFAULT_DISK_VALIDATE_MATERIALIZED = false;
    public static final boolean DEFAULT_DISK_KEEP_MATERIALIZED = false;
    public static final boolean DEFAULT_DISK_SYNOPSIS_PREPARATION_ENABLED = true;
    public static final boolean DEFAULT_FILTER_INVALID_GEOMETRIES_EARLY = false;
    public static final boolean DEFAULT_DEBUG_PROFILE_JOBS = false;
    public static final boolean DEFAULT_DEBUG_AUDIT_PARTITION_JOIN = false;
    public static final String DEFAULT_OBJECT_PAIR_DEDUP_MODE = OBJECT_PAIR_DEDUP_MODE_NONE;
    public static final String DEFAULT_OBJECT_PAIR_DEDUP_OUTPUT_BASE_PATH = "";
    public static final boolean DEFAULT_OBJECT_PAIR_DEDUP_COUNT_IN_COMPUTE_TIME = false;

    private Execution() {
    }
  }

  public static final class DefaultOptions {
    /**
     * Built-in default option entries loaded into {@code SpatialJoinOptions}.
     *
     * <p>The entries are grouped by their role in the current pipeline:
     * Spark resource defaults, operator defaults, execution-planning defaults,
     * and LAHilbert refinement defaults.
     */
    public static final String[][] ENTRIES = new String[][]{
        // Spark resource defaults used when no explicit Spark-submit values are provided.
        {SparkDefaults.EXECUTOR_MEMORY_KEY, SparkDefaults.EXECUTOR_MEMORY},
        {SparkDefaults.EXECUTOR_INSTANCES_KEY, Integer.toString(SparkDefaults.EXECUTOR_INSTANCES)},
        {SparkDefaults.EXECUTOR_CORES_KEY, Integer.toString(SparkDefaults.EXECUTOR_CORES)},
        {SparkDefaults.MEMORY_FRACTION_KEY, Double.toString(SparkDefaults.MEMORY_FRACTION)},
        {SparkDefaults.STORAGE_FRACTION_KEY, Double.toString(SparkDefaults.STORAGE_FRACTION)},

        // Unified operator-layer defaults shared by read, partition, and join entry points.
        {Operator.TYPE, Operator.DEFAULT_TYPE},
        {Operator.PARTITIONER_TYPE, Operator.DEFAULT_PARTITIONER_TYPE},
        {Operator.NUM_PARTITIONS, Integer.toString(Operator.DEFAULT_NUM_PARTITIONS)},
        {Operator.DISJOINT, Boolean.toString(Operator.DEFAULT_DISJOINT)},
        {Operator.INPUT_SAMPLE_FRACTION, Double.toString(Operator.DEFAULT_INPUT_SAMPLE_FRACTION)},
        {Operator.INPUT_SAMPLE_SEED, Long.toString(Operator.DEFAULT_INPUT_SAMPLE_SEED)},
        {Operator.JOIN_PREDICATE, Operator.DEFAULT_JOIN_PREDICATE},
        {Operator.JOIN_ALGORITHM, Operator.DEFAULT_JOIN_ALGORITHM},
        {Operator.JOIN_SIMPLIFICATION_THRESHOLD, Integer.toString(Operator.DEFAULT_JOIN_SIMPLIFICATION_THRESHOLD)},
        {Operator.JOIN_SIMPLIFICATION_ENABLED, Boolean.toString(Operator.DEFAULT_JOIN_SIMPLIFICATION_ENABLED)},
        {Operator.JOIN_PRE_PARTITION_SPLIT_ENABLED, Boolean.toString(Operator.DEFAULT_JOIN_PRE_PARTITION_SPLIT_ENABLED)},
        {Operator.JOIN_PRE_PARTITION_SPLIT_MAX_FRAGMENTS, Integer.toString(Operator.DEFAULT_JOIN_PRE_PARTITION_SPLIT_MAX_FRAGMENTS)},
        {Operator.JOIN_PRE_PARTITION_SPLIT_MAX_DEPTH, Integer.toString(Operator.DEFAULT_JOIN_PRE_PARTITION_SPLIT_MAX_DEPTH)},
        {Operator.JOIN_PLANE_SWEEP_AXIS, Operator.DEFAULT_JOIN_PLANE_SWEEP_AXIS},
        {Operator.JOIN_PLANE_SWEEP_IMPLEMENTATION, Operator.DEFAULT_JOIN_PLANE_SWEEP_IMPLEMENTATION},

        // Execution-planning defaults used after effective partitions have been built.
        {Execution.WORKING_SET_MODE, Execution.DEFAULT_WORKING_SET_MODE},
        {Execution.THREAD_POOL_SIZE, Integer.toString(Execution.DEFAULT_THREAD_POOL_SIZE)},
        {Execution.COMPLEXITY_MODE, Execution.DEFAULT_COMPLEXITY_MODE},
        {Execution.PARTITION_REFINEMENT_LOAD_ESTIMATE_MODE,
            Execution.DEFAULT_PARTITION_REFINEMENT_LOAD_ESTIMATE_MODE},
        {Execution.PARTITION_LOAD_ESTIMATE_MODE, Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE},
        {Execution.PARTITION_INTENSITY_MODE, Execution.DEFAULT_PARTITION_INTENSITY_MODE},
        {Execution.PARTITION_ASSIGNMENT_STRATEGY, Execution.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY},
        {Execution.JOB_EXECUTION_MODE, Execution.DEFAULT_JOB_EXECUTION_MODE},
        {Execution.DISK_MATERIALIZATION_BASE_PATH, Execution.DEFAULT_DISK_MATERIALIZATION_BASE_PATH},
          {Execution.HIGH_COMPLEXITY_EXPANSION_FACTOR, Double.toString(Execution.DEFAULT_HIGH_COMPLEXITY_EXPANSION_FACTOR)},
          {Execution.HIGH_COMPLEXITY_QUANTILE, Double.toString(Execution.DEFAULT_HIGH_COMPLEXITY_QUANTILE)},
          {Execution.HIGH_COMPLEXITY_GRAY_ZONE_RATIO, Double.toString(Execution.DEFAULT_HIGH_COMPLEXITY_GRAY_ZONE_RATIO)},
          {Execution.PREPARED_ASSIGNMENT_BROADCAST_MAX_BYTES, Execution.DEFAULT_PREPARED_ASSIGNMENT_BROADCAST_MAX_BYTES},
        {Execution.DEVIATION_MAX_AVG_WEIGHT, Double.toString(Execution.DEFAULT_DEVIATION_MAX_AVG_WEIGHT)},
        {Execution.DEVIATION_VARIANCE_WEIGHT, Double.toString(Execution.DEFAULT_DEVIATION_VARIANCE_WEIGHT)},
        {Execution.DISK_VALIDATE_MATERIALIZED, Boolean.toString(Execution.DEFAULT_DISK_VALIDATE_MATERIALIZED)},
        {Execution.DISK_KEEP_MATERIALIZED, Boolean.toString(Execution.DEFAULT_DISK_KEEP_MATERIALIZED)},
        {Execution.DISK_SYNOPSIS_PREPARATION_ENABLED,
            Boolean.toString(Execution.DEFAULT_DISK_SYNOPSIS_PREPARATION_ENABLED)},
        {Execution.FILTER_INVALID_GEOMETRIES_EARLY, Boolean.toString(Execution.DEFAULT_FILTER_INVALID_GEOMETRIES_EARLY)},
        {Execution.DEBUG_PROFILE_JOBS, Boolean.toString(Execution.DEFAULT_DEBUG_PROFILE_JOBS)},
        {Execution.DEBUG_AUDIT_PARTITION_JOIN, Boolean.toString(Execution.DEFAULT_DEBUG_AUDIT_PARTITION_JOIN)},
        {Execution.OBJECT_PAIR_DEDUP_MODE, Execution.DEFAULT_OBJECT_PAIR_DEDUP_MODE},
        {Execution.OBJECT_PAIR_DEDUP_OUTPUT_BASE_PATH, Execution.DEFAULT_OBJECT_PAIR_DEDUP_OUTPUT_BASE_PATH},
        {Execution.OBJECT_PAIR_DEDUP_COUNT_IN_COMPUTE_TIME,
            String.valueOf(Execution.DEFAULT_OBJECT_PAIR_DEDUP_COUNT_IN_COMPUTE_TIME)},

        // LAHilbert-specific defaults controlling dynamic refinement and automatic partition sizing.
        {LAHilbert.SAFETY_FRACTION, Double.toString(LAHilbert.DEFAULT_SAFETY_FRACTION)},
        {LAHilbert.LOAD_SCALE_FACTOR, Double.toString(LAHilbert.DEFAULT_LOAD_SCALE_FACTOR)},
        {LAHilbert.REFINE_FANOUT, Integer.toString(LAHilbert.DEFAULT_REFINE_FANOUT)},
        {LAHilbert.MAX_REFINEMENT_DEPTH, Integer.toString(LAHilbert.DEFAULT_MAX_REFINEMENT_DEPTH)},
        {LAHilbert.REFINE_STRATEGY, LAHilbert.DEFAULT_REFINE_STRATEGY},
        {LAHilbert.REFINE_AGGREGATE_MBR_PRUNING_ENABLED,
            Boolean.toString(LAHilbert.DEFAULT_REFINE_AGGREGATE_MBR_PRUNING_ENABLED)},
        {LAHilbert.REFINE_EFFECTIVE_PRUNING_ENABLED,
            Boolean.toString(LAHilbert.DEFAULT_REFINE_EFFECTIVE_PRUNING_ENABLED)},
        {LAHilbert.COALESCE_SMALL_SIBLINGS_ENABLED, Boolean.toString(LAHilbert.DEFAULT_COALESCE_SMALL_SIBLINGS_ENABLED)}
    };

    private DefaultOptions() {
    }
  }
}
