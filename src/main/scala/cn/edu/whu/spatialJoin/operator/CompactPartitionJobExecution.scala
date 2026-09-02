package cn.edu.whu.spatialJoin.operator

import cn.edu.whu.spatialJoin.core.partitioning.SpatialPartitioner
import cn.edu.whu.spatialJoin.geom.IFeature
import org.apache.spark.TaskContext
import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext}

import scala.collection.JavaConverters._

/**
 * Executes one partition-aware job only on the target Spark partitions instead of
 * scanning the full global partition layout and leaving most tasks empty.
 */
object CompactPartitionJobExecution {

  private case class PartitionJoinMeasure(matchCount: Long, localJoinNanos: Long)

  def executeSelectedPartitions(operator: SpatialJoinOperator,
                                partitioner: SpatialPartitioner,
                                useReferencePointDuplicateAvoidance: Boolean,
                                jobId: Int,
                                leftPartitionFeatures: JavaPairRDD[Integer, IFeature],
                                rightPartitionFeatures: JavaPairRDD[Integer, IFeature],
                                selectedPhysicalPartitionIds: Array[Int],
                                localPhysicalToGlobalPartitionIds: Array[Int],
                                expectedLeftCountsByGlobalPartitionId: Array[Long],
                                expectedRightCountsByGlobalPartitionId: Array[Long],
                                executionSource: String): SpatialJoinOperator.PartitionTaskExecutionSummary = {
    executeSelectedPartitionsInternal(
      operator,
      partitioner,
      useReferencePointDuplicateAvoidance,
      jobId,
      leftPartitionFeatures,
      rightPartitionFeatures,
      selectedPhysicalPartitionIds,
      localPhysicalToGlobalPartitionIds,
      expectedLeftCountsByGlobalPartitionId,
      expectedRightCountsByGlobalPartitionId,
      executionSource,
      collectLocalJoinNanos = false)
  }

  def executeSelectedPartitionsForLocalJoinTiming(operator: SpatialJoinOperator,
                                                  partitioner: SpatialPartitioner,
                                                  useReferencePointDuplicateAvoidance: Boolean,
                                                  jobId: Int,
                                                  leftPartitionFeatures: JavaPairRDD[Integer, IFeature],
                                                  rightPartitionFeatures: JavaPairRDD[Integer, IFeature],
                                                  selectedPhysicalPartitionIds: Array[Int],
                                                  localPhysicalToGlobalPartitionIds: Array[Int],
                                                  expectedLeftCountsByGlobalPartitionId: Array[Long],
                                                  expectedRightCountsByGlobalPartitionId: Array[Long],
                                                  executionSource: String): SpatialJoinOperator.PartitionTaskExecutionSummary = {
    executeSelectedPartitionsInternal(
      operator,
      partitioner,
      useReferencePointDuplicateAvoidance,
      jobId,
      leftPartitionFeatures,
      rightPartitionFeatures,
      selectedPhysicalPartitionIds,
      localPhysicalToGlobalPartitionIds,
      expectedLeftCountsByGlobalPartitionId,
      expectedRightCountsByGlobalPartitionId,
      executionSource,
      collectLocalJoinNanos = true)
  }

  def executeSelectedPartitionsToObjectPairFiles(operator: SpatialJoinOperator,
                                                 partitioner: SpatialPartitioner,
                                                 useReferencePointDuplicateAvoidance: Boolean,
                                                 jobId: Int,
                                                 leftPartitionFeatures: JavaPairRDD[Integer, IFeature],
                                                 rightPartitionFeatures: JavaPairRDD[Integer, IFeature],
                                                 selectedPhysicalPartitionIds: Array[Int],
                                                 localPhysicalToGlobalPartitionIds: Array[Int],
                                                 expectedLeftCountsByGlobalPartitionId: Array[Long],
                                                 expectedRightCountsByGlobalPartitionId: Array[Long],
                                                 executionSource: String,
                                                 outputRoot: String): SpatialJoinOperator.PartitionTaskExecutionSummary = {
    if (operator == null || partitioner == null || leftPartitionFeatures == null || rightPartitionFeatures == null ||
      selectedPhysicalPartitionIds == null || selectedPhysicalPartitionIds.isEmpty ||
      outputRoot == null || outputRoot.trim.isEmpty) {
      return new SpatialJoinOperator.PartitionTaskExecutionSummary(0L, 0L, 0L, 0L)
    }

    val sparkContext = JavaSparkContext.fromSparkContext(leftPartitionFeatures.context)
    val planeSweepXAxisCalls =
      sparkContext.sc.longAccumulator(s"sj.planeSweepAxisX.objectPairCompact.$jobId.${System.nanoTime()}")
    val planeSweepYAxisCalls =
      sparkContext.sc.longAccumulator(s"sj.planeSweepAxisY.objectPairCompact.$jobId.${System.nanoTime()}")
    val planeSweepDiagonalAxisCalls =
      sparkContext.sc.longAccumulator(s"sj.planeSweepAxisDiagonal.objectPairCompact.$jobId.${System.nanoTime()}")

    val physicalToGlobal = Option(localPhysicalToGlobalPartitionIds).map(_.clone())
    val normalizedOutputRoot = stripTrailingSlash(outputRoot.trim)

    val partitionMatches = leftPartitionFeatures.rdd.zipPartitions(rightPartitionFeatures.rdd, preservesPartitioning = false) {
      (leftIterator, rightIterator) =>
        val physicalPartitionId = TaskContext.getPartitionId()
        val globalPartitionId = physicalToGlobal match {
          case Some(mapping) if physicalPartitionId >= 0 && physicalPartitionId < mapping.length =>
            mapping(physicalPartitionId)
          case _ =>
            physicalPartitionId
        }
        val leftFeatures = materializePartitionFeatures(leftIterator)
        val rightFeatures = materializePartitionFeatures(rightIterator)
        val outputFile = s"$normalizedOutputRoot/part-job${jobIdFormatted(jobId)}-physical${idFormatted(physicalPartitionId)}-partition${idFormatted(globalPartitionId)}.tsv"
        val emitted = operator.writePartitionJoinObjectPairLines(
          partitioner,
          globalPartitionId,
          leftFeatures,
          rightFeatures,
          useReferencePointDuplicateAvoidance,
          planeSweepXAxisCalls,
          planeSweepYAxisCalls,
          planeSweepDiagonalAxisCalls,
          executionSource,
          jobId,
          expectedLeftCountsByGlobalPartitionId,
          expectedRightCountsByGlobalPartitionId,
          outputFile)
        Iterator.single(java.lang.Long.valueOf(emitted))
    }

    val partitionResults = partitionMatches.sparkContext.runJob(
      partitionMatches,
      (iterator: Iterator[java.lang.Long]) => if (iterator.hasNext) iterator.next() else java.lang.Long.valueOf(0L),
      selectedPhysicalPartitionIds.toSeq)

    val totalMatches = partitionResults.foldLeft(0L)((sum, value) => sum + value.longValue())
    new SpatialJoinOperator.PartitionTaskExecutionSummary(
      totalMatches,
      0L,
      planeSweepXAxisCalls.value,
      planeSweepYAxisCalls.value,
      planeSweepDiagonalAxisCalls.value)
  }

  private def executeSelectedPartitionsInternal(operator: SpatialJoinOperator,
                                                partitioner: SpatialPartitioner,
                                                useReferencePointDuplicateAvoidance: Boolean,
                                                jobId: Int,
                                                leftPartitionFeatures: JavaPairRDD[Integer, IFeature],
                                                rightPartitionFeatures: JavaPairRDD[Integer, IFeature],
                                                selectedPhysicalPartitionIds: Array[Int],
                                                localPhysicalToGlobalPartitionIds: Array[Int],
                                                expectedLeftCountsByGlobalPartitionId: Array[Long],
                                                expectedRightCountsByGlobalPartitionId: Array[Long],
                                                executionSource: String,
                                                collectLocalJoinNanos: Boolean): SpatialJoinOperator.PartitionTaskExecutionSummary = {
    if (operator == null || partitioner == null || leftPartitionFeatures == null || rightPartitionFeatures == null ||
      selectedPhysicalPartitionIds == null || selectedPhysicalPartitionIds.isEmpty) {
      return new SpatialJoinOperator.PartitionTaskExecutionSummary(0L, 0L, 0L, 0L)
    }

    val sparkContext = JavaSparkContext.fromSparkContext(leftPartitionFeatures.context)
    val planeSweepXAxisCalls =
      sparkContext.sc.longAccumulator(s"sj.planeSweepAxisX.compact.$jobId.${System.nanoTime()}")
    val planeSweepYAxisCalls =
      sparkContext.sc.longAccumulator(s"sj.planeSweepAxisY.compact.$jobId.${System.nanoTime()}")
    val planeSweepDiagonalAxisCalls =
      sparkContext.sc.longAccumulator(s"sj.planeSweepAxisDiagonal.compact.$jobId.${System.nanoTime()}")
    val localJoinNanos =
      if (collectLocalJoinNanos) sparkContext.sc.longAccumulator(s"sj.localJoinNanos.compact.$jobId.${System.nanoTime()}")
      else null

    val physicalToGlobal = Option(localPhysicalToGlobalPartitionIds).map(_.clone())

    val partitionMatches = leftPartitionFeatures.rdd.zipPartitions(rightPartitionFeatures.rdd, preservesPartitioning = false) {
      (leftIterator, rightIterator) =>
        val physicalPartitionId = TaskContext.getPartitionId()
        val globalPartitionId = physicalToGlobal match {
          case Some(mapping) if physicalPartitionId >= 0 && physicalPartitionId < mapping.length =>
            mapping(physicalPartitionId)
          case _ =>
            physicalPartitionId
        }
        val leftFeatures = materializePartitionFeatures(leftIterator)
        val rightFeatures = materializePartitionFeatures(rightIterator)
        val joinStartNanos = if (collectLocalJoinNanos) System.nanoTime() else 0L
        val matchCount = operator.executePartitionJoin(
          partitioner,
          globalPartitionId,
          leftFeatures,
          rightFeatures,
          useReferencePointDuplicateAvoidance,
          localJoinNanos,
          null,
          planeSweepXAxisCalls,
          planeSweepYAxisCalls,
          planeSweepDiagonalAxisCalls,
          executionSource,
          jobId,
          expectedLeftCountsByGlobalPartitionId,
          expectedRightCountsByGlobalPartitionId)
        val joinElapsedNanos =
          if (collectLocalJoinNanos) Math.max(0L, System.nanoTime() - joinStartNanos) else 0L
        Iterator.single(PartitionJoinMeasure(matchCount, joinElapsedNanos))
    }

    val partitionResults = partitionMatches.sparkContext.runJob(
      partitionMatches,
      (iterator: Iterator[PartitionJoinMeasure]) => if (iterator.hasNext) iterator.next() else PartitionJoinMeasure(0L, 0L),
      selectedPhysicalPartitionIds.toSeq)

    val totalMatches = partitionResults.foldLeft(0L)((sum, value) => sum + value.matchCount)
    val partitionLocalJoinNanos =
      if (collectLocalJoinNanos) partitionResults.map(measure => Math.max(0L, measure.localJoinNanos))
      else Array.emptyLongArray
    new SpatialJoinOperator.PartitionTaskExecutionSummary(
      totalMatches,
      if (localJoinNanos == null) 0L else localJoinNanos.value,
      partitionLocalJoinNanos,
      planeSweepXAxisCalls.value,
      planeSweepYAxisCalls.value,
      planeSweepDiagonalAxisCalls.value)
  }

  private def materializePartitionFeatures(iterator: Iterator[(Integer, IFeature)]): java.util.List[IFeature] = {
    val features = new java.util.ArrayList[IFeature]()
    while (iterator.hasNext) {
      features.add(iterator.next()._2)
    }
    features
  }

  private def stripTrailingSlash(path: String): String =
    if (path.endsWith("/")) stripTrailingSlash(path.dropRight(1)) else path

  private def idFormatted(value: Int): String = f"$value%05d"

  private def jobIdFormatted(value: Int): String = f"$value%05d"
}
