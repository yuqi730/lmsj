param(
    [ValidateSet('Local', 'Standalone')]
    [string]$Mode = 'Local',

    [Parameter(Mandatory = $true)]
    [string]$LeftPath,

    [Parameter(Mandatory = $true)]
    [string]$RightPath,

    [string]$Format = 'auto',
    [string]$LeftFormat = '',
    [string]$RightFormat = '',
    [string]$Master = 'spark://localhost:7077',
    [string]$LocalThreads = '*',
    [string]$DriverMemory = '2g',
    [string]$ExecutorMemory = '2g',
    [int]$ExecutorCores = 6,
    [int]$ExecutorInstances = 2,
    [int]$DefaultParallelism = 30,
    [int]$ShufflePartitions = 50,
    [ValidateSet('FIFO', 'FAIR')]
    [string]$SchedulerMode = 'FAIR',
    [string]$LocalityWait = '0s',

    [Alias('PartitionSize')]
    [string]$PartitionThreshold = '128m',
    [double]$PartitionSafetyFraction = 0.8,
    [ValidateSet('exact-binary', 'balanced', 'fast')]
    [string]$ComplexityMode = 'exact-binary',
    [ValidateSet('ci-hybrid', 'candidate-avg-complexity', 'sizefunction')]
    [string]$PartitionIntensityMode = 'ci-hybrid',
    [double]$PartitionIntensityCiAlpha = 0.6,
    [int]$SplitThreshold = 256,
    [bool]$PrePartitionSplitEnabled = $true,
    [int]$PrePartitionSplitMaxFragments = 512,
    [int]$PrePartitionSplitMaxDepth = 8,
    [double]$InputSampleFraction = 1.0,
    [long]$InputSampleSeed = 2026052501,
    [ValidateSet('adaptive-axis-load', 'object-center-offset-axis-load', 'equal-quadtree-count', 'equal-quadtree-load')]
    [string]$RefinementStrategy = 'object-center-offset-axis-load',
    [bool]$RecursiveAggregateMbrPruningEnabled = $true,
    [bool]$RefinementEffectivePruningEnabled = $true,
    [ValidateSet('linear', 'grid-directory', 'validate')]
    [string]$PartitionRoutingMode = 'grid-directory',
    [int]$SharedMaxRefinementDepth = -1,

    [ValidateSet('auto', 'memory', 'disk-hybrid')]
    [string]$WorkingSetMode = 'auto',

    [ValidateSet('concurrent', 'sequential')]
    [string]$ExecutionMode = 'concurrent',

    [int]$ThreadPoolSize = 6,
    [int]$MaxConcurrentJobs = 6,
    [int]$MaxPartitionsPerJob = 12,
    [double]$JobMemoryBudgetRatio = 0.8,
    [string]$DiskMaterializationBasePath = 'D:/sj_temp',
    [string]$Separator = '',
    [switch]$SkipHeader,
    [string]$LogLevel = 'WARN',
    [string[]]$ExtraOption = @(),
    [switch]$Build,
    [string]$SparkHome = $env:SPARK_HOME
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$jarPath = Join-Path $projectRoot 'target\LMSJ-1.0-SNAPSHOT.jar'

if ($Build -or -not (Test-Path -LiteralPath $jarPath)) {
    Push-Location $projectRoot
    try {
        & mvn package
        if ($LASTEXITCODE -ne 0) {
            throw "Maven build failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

$sparkSubmit = $null
if ($SparkHome) {
    $candidate = Join-Path $SparkHome 'bin\spark-submit.cmd'
    if (Test-Path -LiteralPath $candidate) {
        $sparkSubmit = $candidate
    }
}
if (-not $sparkSubmit) {
    $command = Get-Command spark-submit.cmd -ErrorAction SilentlyContinue
    if (-not $command) {
        $command = Get-Command spark-submit -ErrorAction SilentlyContinue
    }
    if ($command) {
        $sparkSubmit = $command.Source
    }
}
if (-not $sparkSubmit) {
    throw 'spark-submit was not found. Set SPARK_HOME or pass -SparkHome.'
}

if (-not $LeftFormat) {
    $LeftFormat = $Format
}
if (-not $RightFormat) {
    $RightFormat = $Format
}

if ($Mode -eq 'Local') {
    $sparkMaster = "local[$LocalThreads]"
}
else {
    if (-not $Master.StartsWith('spark://')) {
        throw 'Standalone mode requires -Master spark://<master-host>:<master-port>.'
    }
    $sparkMaster = $Master
}

$submitArgs = @(
    '--class', 'cn.edu.whu.spatialJoin.examples.FullPipelineExample',
    '--master', $sparkMaster,
    '--driver-memory', $DriverMemory,
    '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
    '--conf', 'spark.kryoserializer.buffer=64m',
    '--conf', 'spark.kryoserializer.buffer.max=512m',
    '--conf', "spark.default.parallelism=$DefaultParallelism",
    '--conf', "spark.sql.shuffle.partitions=$ShufflePartitions",
    '--conf', "spark.scheduler.mode=$SchedulerMode",
    '--conf', "spark.locality.wait=$LocalityWait"
)

if ($SchedulerMode -eq 'FAIR') {
    $fairSchedulerFile = Join-Path $projectRoot 'src\main\resources\fairscheduler.xml'
    $submitArgs += @('--conf', "spark.scheduler.allocation.file=$fairSchedulerFile")
}

if ($Mode -eq 'Standalone') {
    $totalExecutorCores = $ExecutorCores * $ExecutorInstances
    $submitArgs += @(
        '--executor-memory', $ExecutorMemory,
        '--executor-cores', $ExecutorCores,
        '--total-executor-cores', $totalExecutorCores,
        '--conf', "spark.executor.instances=$ExecutorInstances"
    )
}

$appArgs = @(
    "--mode=$($Mode.ToLowerInvariant())",
    "--master=$sparkMaster",
    "--left=$LeftPath",
    "--right=$RightPath",
    "--left-format=$LeftFormat",
    "--right-format=$RightFormat",
    "--partition-size=$PartitionThreshold",
    "--lahilbert.partition.bytes=$PartitionThreshold",
    "--lahilbert.memory.safetyFraction=$PartitionSafetyFraction",
    "--sj.execution.complexity.mode=$ComplexityMode",
    "--sj.execution.partitionIntensityMode=$PartitionIntensityMode",
    "--sj.execution.partitionIntensity.ciAlpha=$PartitionIntensityCiAlpha",
    "--sj.join.simplification.threshold=$SplitThreshold",
    "--sj.join.prePartitionSplit.enabled=$($PrePartitionSplitEnabled.ToString().ToLowerInvariant())",
    "--sj.join.prePartitionSplit.maxFragments=$PrePartitionSplitMaxFragments",
    "--sj.join.prePartitionSplit.maxDepth=$PrePartitionSplitMaxDepth",
    "--sj.input.sample.fraction=$InputSampleFraction",
    "--sj.input.sample.seed=$InputSampleSeed",
    '--sj.execution.filterInvalidGeometriesEarly=true',
    "--lahilbert.refine.strategy=$RefinementStrategy",
    "--lahilbert.refine.aggregateMbrPruning.enabled=$($RecursiveAggregateMbrPruningEnabled.ToString().ToLowerInvariant())",
    "--lahilbert.refine.effectivePruning.enabled=$($RefinementEffectivePruningEnabled.ToString().ToLowerInvariant())",
    "--lahilbert.partitionRouting.mode=$PartitionRoutingMode",
    "--lahilbert.refine.maxDepth=$SharedMaxRefinementDepth",
    "--kdtree.refine.maxDepth=$SharedMaxRefinementDepth",
    "--str.refine.maxDepth=$SharedMaxRefinementDepth",
    "--working-set=$WorkingSetMode",
    "--execution-mode=$ExecutionMode",
    "--thread-pool-size=$ThreadPoolSize",
    "--max-concurrent-jobs=$MaxConcurrentJobs",
    "--sj.execution.maxPartitionsPerJob=$MaxPartitionsPerJob",
    "--sj.execution.job.memoryBudgetRatio=$JobMemoryBudgetRatio",
    '--sj.execution.partitionRefinementLoadEstimateMode=materialized-geometry',
    '--sj.execution.partitionLoadEstimateMode=conservative-heap',
    '--sj.execution.objectPairDedup.mode=none',
    '--sj.execution.objectPairDedup.countInComputeTime=false',
    "--sj.execution.disk.basePath=$DiskMaterializationBasePath",
    "--spark.driver.memory=$DriverMemory",
    "--spark.executor.memory=$ExecutorMemory",
    "--spark.executor.cores=$ExecutorCores",
    "--spark.executor.instances=$ExecutorInstances",
    "--spark.default.parallelism=$DefaultParallelism",
    "--spark.sql.shuffle.partitions=$ShufflePartitions",
    "--spark.scheduler.mode=$SchedulerMode",
    "--spark.locality.wait=$LocalityWait",
    "--log-level=$LogLevel"
)

if ($Separator) {
    $appArgs += "--separator=$Separator"
}
if ($SkipHeader) {
    $appArgs += '--skipheader=true'
}
foreach ($option in $ExtraOption) {
    if ($option) {
        $appArgs += $option
    }
}

Write-Host "Running LMSJ in $Mode mode with master $sparkMaster"
& $sparkSubmit @submitArgs $jarPath @appArgs
if ($LASTEXITCODE -ne 0) {
    throw "LMSJ execution failed with exit code $LASTEXITCODE"
}
