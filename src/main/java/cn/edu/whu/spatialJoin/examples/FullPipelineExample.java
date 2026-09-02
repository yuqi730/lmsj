package cn.edu.whu.spatialJoin.examples;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.operator.SpatialJoinResult;
import cn.edu.whu.spatialJoin.operator.SpatialOperator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.File;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Minimal executable example for the complete LMSJ pipeline.
 *
 * <p>Both execution modes run the same partition-aware workflow: input reading, compact feature
 * preparation, memory-aware partition construction, job planning, job dispatch, partition-local
 * joins, and result aggregation. Only the Spark master differs between local and standalone mode.
 */
public final class FullPipelineExample {

  private static final String DEFAULT_APP_NAME = "LMSJ-Full-Pipeline";

  private FullPipelineExample() {
  }

  /** Runs the complete pipeline on a Spark local master. */
  public static SpatialJoinResult runLocal(String[] args) {
    SpatialJoinOptions options = prepareOptions(args);
    String localThreads = options.getString("local-threads", "*").trim();
    if (localThreads.isEmpty())
      localThreads = "*";
    return runPipeline(options, "local[" + localThreads + "]");
  }

  /** Runs the complete pipeline on an existing Spark standalone cluster. */
  public static SpatialJoinResult runStandalone(String[] args) {
    SpatialJoinOptions options = prepareOptions(args);
    String master = firstNonBlank(
        options.getString("master", null),
        options.getString("spark.master", null));
    if (master == null || !master.startsWith("spark://")) {
      throw new IllegalArgumentException(
          "Standalone mode requires --master=spark://<master-host>:<master-port>");
    }
    return runPipeline(options, master);
  }

  /** Selects local or standalone execution with {@code --mode}. */
  public static void main(String[] args) {
    SpatialJoinOptions commandLine = SpatialOperator.parseSubmitArgs(args);
    if (commandLine.getBoolean("help", false)) {
      printUsage();
      return;
    }

    String mode = commandLine.getString("mode", "local").trim().toLowerCase(Locale.ROOT);
    SpatialJoinResult result;
    if ("local".equals(mode)) {
      result = runLocal(args);
    } else if ("standalone".equals(mode)) {
      result = runStandalone(args);
    } else {
      throw new IllegalArgumentException("Unsupported mode '" + mode + "'. Use local or standalone.");
    }
    printResult(result);
  }

  private static SpatialJoinResult runPipeline(SpatialJoinOptions options, String master) {
    requireInput(options, SpatialJoinConstants.Operator.LEFT_INPUT_PATH, "--left");
    requireInput(options, SpatialJoinConstants.Operator.RIGHT_INPUT_PATH, "--right");
    resolveAutoWorkingSetMode(options, master);
    printExecutionProfile(options);

    SparkConf sparkConf = buildSparkConf(options, master);
    try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
      sparkContext.setLogLevel(options.getString("log-level", "WARN"));
      return new SpatialOperator(options).spatialJoin().executePartitionAware(sparkContext);
    }
  }

  private static SparkConf buildSparkConf(SpatialJoinOptions options, String master) {
    SparkConf sparkConf = new SparkConf().setAppName(options.getString("app-name", DEFAULT_APP_NAME));
    Iterator<Tuple2<String, String>> entries = options.iterator();
    while (entries.hasNext()) {
      Tuple2<String, String> entry = entries.next();
      if (entry._1() != null && entry._1().startsWith("spark."))
        sparkConf.set(entry._1(), entry._2());
    }
    sparkConf.setMaster(master);
    setIfMissing(sparkConf, "spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    setIfMissing(sparkConf, "spark.kryoserializer.buffer", "64m");
    setIfMissing(sparkConf, "spark.kryoserializer.buffer.max", "512m");
    setIfMissing(sparkConf, "spark.ui.enabled", "true");
    setIfMissing(sparkConf, "spark.eventLog.enabled", "false");
    return sparkConf;
  }

  private static SpatialJoinOptions prepareOptions(String[] args) {
    SpatialJoinOptions parsed = SpatialOperator.parseSubmitArgs(args);
    SpatialJoinOptions options = new SpatialJoinOptions(false);
    applyStableExecutionProfile(options);
    overlayExplicitOptions(options, parsed, args);
    copyAlias(options, "left", SpatialJoinConstants.Operator.LEFT_INPUT_PATH, false);
    copyAlias(options, "right", SpatialJoinConstants.Operator.RIGHT_INPUT_PATH, false);
    copyAlias(options, "left-format", SpatialJoinConstants.Operator.LEFT_INPUT_FORMAT, true);
    copyAlias(options, "right-format", SpatialJoinConstants.Operator.RIGHT_INPUT_FORMAT, true);
    copyAlias(options, "working-set", SpatialJoinConstants.Execution.WORKING_SET_MODE, false);
    copyAlias(options, "execution-mode", SpatialJoinConstants.Execution.JOB_EXECUTION_MODE, false);
    copyAlias(options, "thread-pool-size", SpatialJoinConstants.Execution.THREAD_POOL_SIZE, false);
    copyAlias(options, "max-concurrent-jobs", SpatialJoinConstants.Execution.MAX_CONCURRENT_JOBS, false);

    String commonFormat = options.getString("format", null);
    if (isBlank(options.getString(SpatialJoinConstants.Operator.LEFT_INPUT_FORMAT, null)) && !isBlank(commonFormat))
      options.set(SpatialJoinConstants.Operator.LEFT_INPUT_FORMAT, normalizeAutoFormat(commonFormat));
    if (isBlank(options.getString(SpatialJoinConstants.Operator.RIGHT_INPUT_FORMAT, null)) && !isBlank(commonFormat))
      options.set(SpatialJoinConstants.Operator.RIGHT_INPUT_FORMAT, normalizeAutoFormat(commonFormat));

    String partitionSize = options.getString("partition-size", null);
    if (!isBlank(partitionSize)) {
      options.set("pcriterion", "size(" + partitionSize.trim() + ")");
      options.set(SpatialJoinConstants.LAHilbert.PARTITION_BYTES, partitionSize.trim());
    }
    return options;
  }

  /** Stable experiment profile shared by local and standalone execution. Explicit arguments override it. */
  private static void applyStableExecutionProfile(SpatialJoinOptions options) {
    options
        .set(SpatialJoinConstants.Operator.PARTITIONER_TYPE,
            SpatialJoinConstants.Operator.DEFAULT_PARTITIONER_TYPE)
        .set("partition-size", "128m")
        .set("pcriterion", "size(128m)")
        .set(SpatialJoinConstants.LAHilbert.PARTITION_BYTES, "128m")
        .set(SpatialJoinConstants.LAHilbert.SAFETY_FRACTION, "0.8")
        .set(SpatialJoinConstants.LAHilbert.REFINE_STRATEGY,
            SpatialJoinConstants.LAHilbert.REFINE_STRATEGY_OBJECT_CENTER_OFFSET_AXIS_LOAD)
        .set(SpatialJoinConstants.LAHilbert.REFINE_AGGREGATE_MBR_PRUNING_ENABLED, true)
        .set(SpatialJoinConstants.LAHilbert.REFINE_EFFECTIVE_PRUNING_ENABLED, true)
        .set(SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE,
            SpatialJoinConstants.LAHilbert.PARTITION_ROUTING_MODE_GRID_DIRECTORY)
        .set(SpatialJoinConstants.LAHilbert.MAX_REFINEMENT_DEPTH, -1)
        .set(SpatialJoinConstants.KDTree.MAX_REFINEMENT_DEPTH, -1)
        .set(SpatialJoinConstants.STRTree.MAX_REFINEMENT_DEPTH, -1)
        .set(SpatialJoinConstants.Execution.COMPLEXITY_MODE,
            SpatialJoinConstants.Execution.COMPLEXITY_MODE_EXACT_BINARY)
        .set(SpatialJoinConstants.Execution.PARTITION_REFINEMENT_LOAD_ESTIMATE_MODE,
            SpatialJoinConstants.Execution.LOAD_ESTIMATE_MODE_MATERIALIZED_GEOMETRY)
        .set(SpatialJoinConstants.Execution.PARTITION_LOAD_ESTIMATE_MODE,
            SpatialJoinConstants.Execution.LOAD_ESTIMATE_MODE_CONSERVATIVE_HEAP)
        .set(SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE,
            SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE_CI_HYBRID)
        .set(SpatialJoinConstants.Execution.PARTITION_INTENSITY_CI_ALPHA, "0.6")
        .set(SpatialJoinConstants.Operator.JOIN_SIMPLIFICATION_THRESHOLD, 256)
        .set(SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_ENABLED, true)
        .set(SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_MAX_FRAGMENTS, 512)
        .set(SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_MAX_DEPTH, 8)
        .set(SpatialJoinConstants.Operator.INPUT_SAMPLE_FRACTION, "1.0")
        .set(SpatialJoinConstants.Operator.INPUT_SAMPLE_SEED, "2026052501")
        .set(SpatialJoinConstants.Execution.FILTER_INVALID_GEOMETRIES_EARLY, true)
        .set(SpatialJoinConstants.Execution.THREAD_POOL_SIZE, 6)
        .set(SpatialJoinConstants.Execution.MAX_CONCURRENT_JOBS, 6)
        .set(SpatialJoinConstants.Execution.MAX_PARTITIONS_PER_JOB, 12)
        .set(SpatialJoinConstants.Execution.JOB_MEMORY_BUDGET_RATIO, "0.8")
        .set(SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_MODE,
            SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_MODE_NONE)
        .set(SpatialJoinConstants.Execution.OBJECT_PAIR_DEDUP_COUNT_IN_COMPUTE_TIME, false)
        .set(SpatialJoinConstants.Execution.WORKING_SET_MODE, "auto")
        .set(SpatialJoinConstants.Execution.JOB_EXECUTION_MODE,
            SpatialJoinConstants.Execution.JOB_EXECUTION_MODE_CONCURRENT);
  }

  /** Re-applies only command-line keys so library defaults cannot mask the stable profile. */
  private static void overlayExplicitOptions(SpatialJoinOptions target,
                                             SpatialJoinOptions parsed,
                                             String[] args) {
    for (String key : explicitOptionNames(args)) {
      if (parsed.contains(key))
        target.set(key, parsed.getString(key));
    }
  }

  private static Set<String> explicitOptionNames(String[] args) {
    Set<String> names = new HashSet<>();
    if (args == null)
      return names;
    for (String raw : args) {
      if (raw == null)
        continue;
      String token = raw.trim();
      if (token.startsWith("--no-"))
        token = token.substring(5);
      else if (token.startsWith("-no-"))
        token = token.substring(4);
      else if (token.startsWith("--"))
        token = token.substring(2);
      else if (token.startsWith("-"))
        token = token.substring(1);
      else if (token.indexOf('=') < 1 && token.indexOf(':') < 1)
        continue;
      int equals = token.indexOf('=');
      int colon = token.indexOf(':');
      int separator = equals < 0 ? colon : (colon < 0 ? equals : Math.min(equals, colon));
      names.add(separator < 0 ? token : token.substring(0, separator));
    }
    return names;
  }

  /** Matches the stable experiment entry's file-size-based automatic working-set selection. */
  private static void resolveAutoWorkingSetMode(SpatialJoinOptions options, String master) {
    String requested = options.getString(SpatialJoinConstants.Execution.WORKING_SET_MODE, "auto");
    if (!"auto".equalsIgnoreCase(requested))
      return;
    long inputBytes = saturatedAdd(
        estimateInputPathBytes(options.getString(SpatialJoinConstants.Operator.LEFT_INPUT_PATH, "")),
        estimateInputPathBytes(options.getString(SpatialJoinConstants.Operator.RIGHT_INPUT_PATH, "")));
    long abstractBytes = Math.round(inputBytes * 0.10d);
    long residentBytes = saturatedAdd(Math.round(inputBytes * 3.0d), abstractBytes);
    boolean local = master != null && master.startsWith("local[");
    long executorMemory = local
        ? Runtime.getRuntime().maxMemory()
        : options.getSizeAsBytes(
            SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY_KEY,
            SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY);
    int executorInstances = local ? 1 : Math.max(1, options.getInt(
        SpatialJoinConstants.SparkDefaults.EXECUTOR_INSTANCES_KEY,
        SpatialJoinConstants.SparkDefaults.EXECUTOR_INSTANCES));
    double storageFraction = options.getDouble(
        SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION_KEY,
        SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION);
    long totalMemory = saturatedMultiply(executorMemory, executorInstances);
    long executionBudget = Math.max(1L, (long) (totalMemory * Math.max(0.0d, 1.0d - storageFraction)));
    long safeMemoryBudget = Math.max(1L, (long) (executionBudget * 0.85d));
    String selected = residentBytes <= safeMemoryBudget
        ? SpatialJoinConstants.Execution.WORKING_SET_MODE_MEMORY
        : SpatialJoinConstants.Execution.WORKING_SET_MODE_DISK_HYBRID;
    options.set(SpatialJoinConstants.Execution.WORKING_SET_MODE, selected);
    System.out.println("Working-set mode: auto -> " + selected
        + " (estimatedResidentBytes=" + residentBytes
        + ", safeBudgetBytes=" + safeMemoryBudget + ")");
  }

  private static long estimateInputPathBytes(String paths) {
    if (isBlank(paths))
      return 0L;
    long total = 0L;
    for (String path : paths.split(","))
      total = saturatedAdd(total, estimateFileBytes(new File(path.trim())));
    return total;
  }

  private static long estimateFileBytes(File file) {
    if (file == null || !file.exists())
      return 0L;
    if (file.isFile())
      return Math.max(0L, file.length());
    File[] children = file.listFiles();
    if (children == null)
      return 0L;
    long total = 0L;
    for (File child : children)
      total = saturatedAdd(total, estimateFileBytes(child));
    return total;
  }

  private static long saturatedAdd(long left, long right) {
    return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }

  private static long saturatedMultiply(long value, int factor) {
    return factor > 0 && value > Long.MAX_VALUE / factor ? Long.MAX_VALUE : value * factor;
  }

  private static void printExecutionProfile(SpatialJoinOptions options) {
    System.out.println("Execution profile:");
    System.out.println("  partitionThreshold       : " + options.getString(SpatialJoinConstants.LAHilbert.PARTITION_BYTES));
    System.out.println("  partitionSafetyFraction  : " + options.getString(SpatialJoinConstants.LAHilbert.SAFETY_FRACTION));
    System.out.println("  prePartitionSplit        : " + options.getString(SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_ENABLED)
        + " (threshold=" + options.getString(SpatialJoinConstants.Operator.JOIN_SIMPLIFICATION_THRESHOLD)
        + ", maxFragments=" + options.getString(SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_MAX_FRAGMENTS)
        + ", maxDepth=" + options.getString(SpatialJoinConstants.Operator.JOIN_PRE_PARTITION_SPLIT_MAX_DEPTH) + ")");
    System.out.println("  partitionIntensity       : " + options.getString(SpatialJoinConstants.Execution.PARTITION_INTENSITY_MODE)
        + " (alpha=" + options.getString(SpatialJoinConstants.Execution.PARTITION_INTENSITY_CI_ALPHA) + ")");
    System.out.println("  refinementStrategy       : " + options.getString(SpatialJoinConstants.LAHilbert.REFINE_STRATEGY));
    System.out.println("  sharedRefinementMaxDepth : " + options.getString(SpatialJoinConstants.LAHilbert.MAX_REFINEMENT_DEPTH));
    System.out.println("  maxPartitionsPerJob      : " + options.getString(SpatialJoinConstants.Execution.MAX_PARTITIONS_PER_JOB));
    System.out.println("  jobMemoryBudgetRatio     : " + options.getString(SpatialJoinConstants.Execution.JOB_MEMORY_BUDGET_RATIO));
    System.out.println("  workingSetMode           : " + options.getString(SpatialJoinConstants.Execution.WORKING_SET_MODE));
  }

  private static void copyAlias(SpatialJoinOptions options, String alias, String target, boolean formatValue) {
    String value = options.getString(alias, null);
    if (!isBlank(value))
      options.set(target, formatValue ? normalizeAutoFormat(value) : value.trim());
  }

  private static String normalizeAutoFormat(String value) {
    return "auto".equalsIgnoreCase(value.trim()) ? "*auto*" : value.trim();
  }

  private static void requireInput(SpatialJoinOptions options, String key, String displayName) {
    if (isBlank(options.getString(key, null)))
      throw new IllegalArgumentException("Missing required input option " + displayName);
  }

  private static void setIfMissing(SparkConf conf, String key, String value) {
    if (!conf.contains(key))
      conf.set(key, value);
  }

  private static String firstNonBlank(String first, String second) {
    if (!isBlank(first))
      return first.trim();
    return isBlank(second) ? null : second.trim();
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static void printResult(SpatialJoinResult result) {
    System.out.println("LMSJ execution completed");
    System.out.println("  algorithm            : " + result.getAlgorithm());
    System.out.println("  predicate            : " + result.getPredicate());
    System.out.println("  left feature count   : " + result.getLeftCount());
    System.out.println("  right feature count  : " + result.getRightCount());
    System.out.println("  effective partitions : " + result.getEffectivePartitionCount());
    System.out.println("  planned jobs         : " + result.getPlannedJobCount());
    System.out.println("  matched pairs        : " + result.getMatchCount());
  }

  private static void printUsage() {
    System.out.println("Usage: FullPipelineExample --mode=<local|standalone> --left=<path> --right=<path> [options]");
    System.out.println("  --format=<auto|wkt(0)|point(0,1)|geojson|shapefile>");
    System.out.println("  --left-format=<format> --right-format=<format>");
    System.out.println("  --master=spark://host:7077             Required for standalone mode");
    System.out.println("  --local-threads=<*|number>              Default: *");
    System.out.println("  --partition-size=<32m|128m|1g>          Stable default: 128m");
    System.out.println("  --working-set=<auto|memory|disk-hybrid> Stable default: auto");
    System.out.println("  --execution-mode=<concurrent|sequential>");
    System.out.println("  --thread-pool-size=<number> --max-concurrent-jobs=<number>");
    System.out.println("Any underlying sj.*, lahilbert.*, pcriterion, or spark.* option can also be supplied.");
  }
}
