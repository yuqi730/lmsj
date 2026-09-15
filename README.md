# LMSJ

LMSJ (**Limited-Memory Spatial Join**) is a Spark-based spatial join engine for
workloads whose complete intermediate state cannot safely remain in executor
memory. It builds memory-aware spatial partitions, groups them into bounded jobs,
and executes partition-local joins with configurable concurrency.

## Processing pipeline

The full execution path is:

1. Read the two spatial datasets and normalize their geometries.
2. Build summaries and compact binary-backed feature descriptors.
3. Construct and refine spatial partitions according to the configured memory
   threshold.
4. Assign features to effective partitions and build per-partition load profiles.
5. Group effective partitions into jobs that fit the execution budget.
6. Dispatch jobs sequentially or concurrently.
7. Run partition-local filter/refine joins and aggregate the final match count.

The `memory` working-set mode reuses serialized partition data in Spark storage.
The `disk-hybrid` mode materializes partition data outside executor memory and
reloads only the partitions required by each job.

## Package guide

| Package | Responsibility |
| --- | --- |
| `common` | Configuration keys, option parsing, dataset summaries, and accumulators. |
| `core.algorithms` | Plane-sweep filtering, exact geometry refinement, duplicate avoidance, and geometry splitting. |
| `core.partitioning` | Grid, STR, KD-tree, Hilbert, and memory-aware LAHilbert partition construction and refinement. |
| `core.partitioning.helper` | Compact feature descriptors, hierarchy construction, and partition-assignment strategies. |
| `cost` | Partition load, cost, and complexity profiles used during job planning. |
| `execution` | Capacity-first dynamic-programming job grouping, queue planning, bounded dispatch, and per-job execution results. |
| `operator` | Public read, partition, and full spatial-join pipeline APIs. |
| `geom` / `geom.util` | Geometry-backed feature types, envelopes, binary geometry metadata, readers, and writers. |
| `io.reader` | CSV/WKT, GeoJSON, and Shapefile input support. |
| `index` | Global-index construction and partition-criterion handling. |
| `synopses` | Histograms and feature-size/complexity estimators. |
| `sql` | Spark SQL geometry data type and binary geometry conversion. |
| `utils` | Primitive collections, serialization adapters, file helpers, and CRS support. |
| `examples` | Runnable local and Spark standalone full-pipeline examples. |

## Requirements

- JDK 8
- Maven 3.8 or later
- Apache Spark 3.2.x with Scala 2.12
- PowerShell 5.1 or later when using the supplied script

Set `SPARK_HOME` or ensure `spark-submit` is available on `PATH`.

## Build

```powershell
cd LMSJ
mvn clean package
```

The runnable JAR is generated at `target/LMSJ-1.0-SNAPSHOT.jar`.

## Input formats

The reader can auto-detect `.csv`, `.geojson`, and `.shp` inputs. Explicit format
examples include:

| Value | Meaning |
| --- | --- |
| `auto` | Detect the format from the path and file content. |
| `wkt(0)` | Read WKT geometry from CSV column 0. |
| `wkt(geometry)` | Read WKT from a header column named `geometry`; use `-SkipHeader`. |
| `point(0,1)` | Read point X/Y coordinates from CSV columns 0 and 1. |
| `envelope(0,1,2,3)` | Read minimum/maximum X/Y values from four columns. |
| `geojson` | Read GeoJSON features. |
| `shapefile` | Read a `.shp`, a compatible `.zip`, or a directory of shapefiles. |

For CSV input, the default separator is a tab. Pass `-Separator ','` for comma
separation and `-SkipHeader` when the first row contains column names.

## Full-pipeline example

[`FullPipelineExample`](src/main/java/cn/edu/whu/spatialJoin/examples/FullPipelineExample.java)
contains two public static entry points:

```java
SpatialJoinResult localResult = FullPipelineExample.runLocal(args);
SpatialJoinResult clusterResult = FullPipelineExample.runStandalone(args);
```

Its `main` method selects one of them through `--mode=local` or
`--mode=standalone`. Both methods execute the complete partition-aware pipeline;
only the Spark master and resource placement differ.

## Run with PowerShell

The recommended entry point is
[`scripts/run_full_pipeline.ps1`](scripts/run_full_pipeline.ps1). The script builds
the JAR when it is missing, locates `spark-submit`, supplies Spark resource
arguments, and forwards LMSJ options to the example class.

### Local mode

```powershell
.\scripts\run_full_pipeline.ps1 `
  -Mode Local `
  -LeftPath 'D:\data\left.csv' `
  -RightPath 'D:\data\right.csv' `
  -Format 'wkt(0)' `
  -Separator ',' `
  -LocalThreads 8 `
  -DriverMemory 2g `
  -PartitionThreshold 128m
```

Local mode uses `local[N]`, where `N` is controlled by `-LocalThreads`. Use
`-LocalThreads '*'` to use all logical processors.

### Spark standalone mode

Start the Spark master and workers first, then run:

```powershell
.\scripts\run_full_pipeline.ps1 `
  -Mode Standalone `
  -Master 'spark://spark-master-host:7077' `
  -LeftPath 'hdfs:///data/left.geojson' `
  -RightPath 'hdfs:///data/right.geojson' `
  -Format geojson `
  -DriverMemory 2g `
  -ExecutorMemory 2g `
  -ExecutorCores 6 `
  -ExecutorInstances 2 `
  -DefaultParallelism 30 `
  -ShufflePartitions 50 `
  -PartitionThreshold 128m `
  -ThreadPoolSize 6 `
  -MaxConcurrentJobs 6 `
  -MaxPartitionsPerJob 12
```

Every worker must be able to access the input paths. Use HDFS or another shared
filesystem for cluster runs; a driver-local path is generally not visible to
remote workers.

### Script parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `-Mode` | `Local` | Select `Local` or `Standalone`. |
| `-LeftPath`, `-RightPath` | required | Input dataset paths. |
| `-Format` | `auto` | Common format for both inputs. |
| `-LeftFormat`, `-RightFormat` | empty | Override the format for one side. |
| `-Master` | `spark://localhost:7077` | Standalone master URL. |
| `-LocalThreads` | `*` | Local Spark worker-thread count. |
| `-DriverMemory` | `2g` | Driver heap passed to `spark-submit`. |
| `-ExecutorMemory` | `2g` | Heap per standalone executor. |
| `-ExecutorCores` | `6` | Cores per standalone executor. |
| `-ExecutorInstances` | `2` | Used with executor cores to derive Spark standalone total executor cores. |
| `-DefaultParallelism` | `30` | Default Spark task parallelism. |
| `-ShufflePartitions` | `50` | Spark SQL shuffle partition count. |
| `-SchedulerMode` | `FAIR` | Spark scheduler mode used by concurrent jobs. |
| `-LocalityWait` | `0s` | Avoid locality-delay bias between concurrent task units. |
| `-PartitionThreshold` | `128m` | Upper memory-load threshold of one LAHilbert partition; `-PartitionSize` remains an alias. |
| `-PartitionSafetyFraction` | `0.8` | Safety coefficient used by LAHilbert resource-derived memory calculations. |
| `-ComplexityMode` | `exact-binary` | Binary metadata detail retained in partition profiles. |
| `-PartitionIntensityMode` | `ci-hybrid` | Task-unit intensity metric. |
| `-PartitionIntensityCiAlpha` | `0.6` | Feature-count weight in the CI hybrid metric. |
| `-SplitThreshold` | `256` | Vertex threshold that triggers pre-partition geometry splitting. |
| `-PrePartitionSplitEnabled` | `true` | Enable the stable pre-partition split path. |
| `-PrePartitionSplitMaxFragments` | `512` | Maximum fragments generated for one source geometry. |
| `-PrePartitionSplitMaxDepth` | `8` | Maximum recursive pre-split depth. |
| `-RefinementStrategy` | `object-center-offset-axis-load` | LAHilbert recursive refinement strategy. |
| `-RecursiveAggregateMbrPruningEnabled` | `true` | Prune recursively when aggregate left/right MBRs are disjoint. |
| `-RefinementEffectivePruningEnabled` | `true` | Remove ineffective leaves before constructing the final plan. |
| `-PartitionRoutingMode` | `grid-directory` | Route objects through the LAHilbert grid directory. |
| `-SharedMaxRefinementDepth` | `-1` | Unlimited shared LAHilbert/KD/STR refinement depth. |
| `-WorkingSetMode` | `auto` | Select `memory` or `disk-hybrid` with the stable file-size/budget rule. |
| `-ExecutionMode` | `concurrent` | Execute planned jobs concurrently or sequentially. |
| `-ThreadPoolSize` | `6` | Driver-side job-dispatch thread count. |
| `-MaxConcurrentJobs` | `6` | Safety ceiling for simultaneously active jobs. |
| `-MaxPartitionsPerJob` | `12` | Upper bound on effective partitions packed into one task unit. |
| `-JobMemoryBudgetRatio` | `0.8` | Fraction of the global execution budget available when packing one task unit. |
| `-InputSampleFraction` | `1.0` | Read the complete input by default. |
| `-InputSampleSeed` | `2026052501` | Deterministic sampling seed. |
| `-DiskMaterializationBasePath` | `D:/sj_temp` | Base directory for disk-hybrid materialization. |
| `-Separator` | empty | CSV separator; the reader default is a tab. |
| `-SkipHeader` | off | Skip and interpret the first CSV row as a header. |
| `-LogLevel` | `WARN` | Spark log level used by the example. |
| `-Build` | off | Force a Maven rebuild even if the JAR exists. |
| `-SparkHome` | `$env:SPARK_HOME` | Explicit Spark installation directory. |

### Pass advanced options

Use `-ExtraOption` to forward configuration keys that are not exposed as named
script parameters. The following example supplies an explicit per-task-unit
memory budget while keeping the rest of the stable execution profile unchanged:

```powershell
.\scripts\run_full_pipeline.ps1 `
  -Mode Local `
  -LeftPath 'D:\data\left.csv' `
  -RightPath 'D:\data\right.csv' `
  -Format 'wkt(0)' `
  -ExtraOption @(
    '--sj.execution.job.memoryBudget=2g'
  )
```

Supported advanced namespaces include `sj.*`, `lahilbert.*`, `pcriterion`, and
`spark.*`. Named parameters take care of the most common settings; advanced
options are intended for algorithm and resource tuning.

## Stable execution profile

Both example methods apply the same reproducible profile before user overrides:

1. LAHilbert partitions use a `128m` upper load threshold, safety fraction `0.8`,
   object-center-offset-axis-load refinement, aggregate-MBR pruning, effective
   pruning, grid-directory routing, and unlimited refinement depth.
2. Geometries with more than `256` vertices enter pre-partition splitting, with
   at most `512` fragments and recursion depth `8`.
3. Null, empty, and invalid geometries are filtered before summaries and
   partition construction; effective profiles then use `ci-hybrid` intensity
   with `alpha=0.6`.
4. `JobPlanner` sorts effective partitions by the selected intensity and applies
   capacity-first dynamic programming. It minimizes the number of task units first
   and total within-unit intensity SSE second; every unit contains at most `12`
   partitions and uses at most `0.8` of the global execution-memory budget.
5. `JobDispatcher` uses resource-aware concurrent execution with a six-thread
   pool and at most six active jobs.
6. Partition-local execution uses the binary MBR filter, automatic plane-sweep
   axis selection, exact geometry refinement, and deterministic duplicate
   avoidance.

Local and standalone modes therefore share the same read, split, partition,
profile, planning, dispatch, and local-join chain. Their only intended
difference is the Spark master and resource placement.

### CI metric and complexity coefficients

The stable `ci-hybrid` path computes partition intensity from normalized total
feature count and normalized CIB-candidate average vertex complexity. The fitted
coefficients in `SizeFunctions` are not inputs to this final CI value. Exact
binary complexity is still collected in the partition profile to preserve the
same preparation dataflow and to support the optional `sizefunction` and
`candidate-avg-complexity` modes; changing those coefficients does not change a
`ci-hybrid` run's task-unit intensity.

## Result summary

After a successful run, the example prints the selected algorithm and predicate,
left/right feature counts, effective partition count, planned job count, and the
final matched-pair count. Spark logs and the Spark Web UI provide task-level and
executor-level details during execution.
