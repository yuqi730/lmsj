package cn.edu.whu.spatialJoin.index

import cn.edu.whu.spatialJoin.algortithms.SpatialDataTypes.{JavaSpatialRDD, SpatialRDD}
import cn.edu.whu.spatialJoin.common.{SpatialJoinConstants, SpatialJoinOptions, Summary}
import cn.edu.whu.spatialJoin.core.partitioning._
import cn.edu.whu.spatialJoin.core.partitioning.helper.{AbstractFeature, AbstractFeatureHierarchy}
import cn.edu.whu.spatialJoin.geom.{EnvelopeNDLite, IFeature}
import cn.edu.whu.spatialJoin.synopses.{AbstractHistogram, UniformHistogram}
import cn.edu.whu.spatialJoin.utils.{IntArray, OperationParam}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

/**
 * A helper object for creating indexes and partitioning [[SpatialRDD]]s
 */
object IndexHelper extends Logging {
  /**The different ways for specifying the number of partitions*/
  trait PartitionCriterion
  /**The number of partitions is explicitly specified*/
  case object Fixed extends PartitionCriterion
  /**The number of partitions is adjusted so that each partition has a number of features*/
  case object FeatureCount extends PartitionCriterion
  /**The number of partitions is adjusted so that each partition has a specified size*/
  case object Size extends PartitionCriterion

  /**Information that is used to calculated the number of partitions*/
  case class NumPartitions(pc: PartitionCriterion, value: Long)

  /**The type of the global index (partitioner)*/
  @OperationParam(
    description = "The type of the global index",
    required = false,
    defaultValue = "rsgrove"
  )
  val GlobalIndex = "gindex"

  /**Whether to build a disjoint index (with no overlapping partitions)*/
  @OperationParam(
    description = "Build a disjoint index with no overlaps between partitions",
    defaultValue = "false"
  )
  val DisjointIndex = "disjoint"

  /**Partition construction mode: full collects all abstract features, hierarchical aggregates them distributedly*/
  val PartitionConstructionMode = "sj.partitioner.build.mode"

  /**Multiplier used to derive the number of coarse distributed aggregation cells in hierarchical mode*/
  val HierarchicalAggregationFactor = "sj.partitioner.hierarchy.factor"

  /**Explicit total number of coarse aggregation cells for hierarchical mode*/
  val HierarchicalAggregationCells = "sj.partitioner.hierarchy.cells"

  /**The criterion used to calculate the number of partitions*/
  @OperationParam(
    description =
      """The criterion used to compute the number of partitions. It can be one of:
- Fixed(n): Create a fixed number of partitions (n partitions)
- Size(s): Create n partitions such that each partition contains around s bytes
- Count(c): Create n partitions such that each partition contains around c records""",
    defaultValue = "Size(32m)"
  )
  val PartitionCriterionThreshold = "pcriterion";

  // ---- The following set of functions help in creating a partitioner from a SpatialRDD and a partitioner class

  /**
   * Compute number of partitions for a partitioner given the partitioning criterion and the summary of the dataset.
   *
   * @param numPartitions the desired number of partitions
   * @param summary    the summary of the dataset
   * @return the preferred number of partitions
   */
  def computeNumberOfPartitions(numPartitions: NumPartitions, summary: Summary): Int = numPartitions.pc match {
    case Fixed => numPartitions.value.toInt
    case FeatureCount => Math.ceil(summary.numFeatures.toDouble / numPartitions.value).toInt
    case Size => Math.ceil(summary.size.toDouble / numPartitions.value).toInt
  }

  /**
   * (Java shortcut to)
   * Compute number of partitions for a partitioner given the partitioning criterion and the summary of the dataset.
   *
   * @param pcriterion the criterion used to define the number of partitions
   * @param value the value associated with the criterion
   * @param summary    the summary of the dataset
   * @return the preferred number of partitions
   */
  def computeNumberOfPartitions(pcriterion: String, value: Long, summary: Summary): Int = {
    val pc: PartitionCriterion = pcriterion.toLowerCase match {
      case "fixed" => Fixed
      case "count" => FeatureCount
      case "size" => Size
    }
    computeNumberOfPartitions(NumPartitions(pc, value), summary)
  }

  /**
   * Constructs a spatial partitioner for the given features. Returns an instance of the spatial partitioner class
   * that is given which is initialized based on the given features.
   *
   * @param features the features to create the partitioner on
   * @param partitionerClass the class of the partitioner to construct
   * @param numPartitions the desired number of partitions (this is just a loose hint not a strict number)
   * @param sizeFunction a function that calculates the size of each feature for load balancing. Only needed if
   *                     the partition criterion is specified through partition size [[Size]]
   * @return a constructed spatial partitioner
   */
  def createPartitioner(features: SpatialRDD,
                        partitionerClass: Class[_ <: SpatialPartitioner],
                        numPartitions: NumPartitions,
                        sizeFunction: IFeature=>Int,
                        opts: SpatialJoinOptions
                       ): SpatialPartitioner = {
    // Whether to generate a disjoint index (if supported)
    val disjoint = opts.getBoolean(DisjointIndex, false)

    // Calculate the summary
    val t1 = System.nanoTime()
    val (histogram, abstractFeatureHierarchy, summary) =
        summarizeDataset(features.filter(f => f.getGeometry != null && !f.getGeometry.isEmpty),
          partitionerClass, sizeFunction, numPartitions, opts)

    val t2 = System.nanoTime

    // Now that the input set has been summarized, we can create the partitioner
    val numCells: Int = computeNumberOfPartitions(numPartitions, summary)
    if (numCells == 1) {
      logError("Input too small. Creating a cell partitioner with one cell")
      // Create a cell partitioner that contains one cell that represents the entire input
      val universe = new EnvelopeNDLite(summary)
      universe.setInfinite()
      new GridPartitioner(universe,numCells)
      // Notice that it might be possible to avoid computing some lightweight metadata here. However, it is not worth it
      // since this case happens only for small datasets
    } else {
      val spatialPartitioner: SpatialPartitioner = partitionerClass.newInstance
      spatialPartitioner.setup(opts, disjoint)
      val pMetadata = spatialPartitioner.getMetadata
      if (disjoint && !pMetadata.disjointSupported)
        throw new RuntimeException("Partitioner " + partitionerClass + " does not support disjoint partitioning")
      // Construct the partitioner
      val nump: Int = computeNumberOfPartitions(numPartitions, summary)
      spatialPartitioner.construct(summary, abstractFeatureHierarchy, histogram, nump)
      val t3 = System.nanoTime
      logInfo(f"Synopses created in ${(t2 - t1) * 1E-9}%f seconds and partitioner '${partitionerClass.getSimpleName}' " +
        f" constructed in ${(t3 - t2) * 1E-9}%f seconds")
      spatialPartitioner
    }
  }

  /**
   * (Java shortcut to)
   * Constructs a spatial partitioner for the given features. Returns an instance of the spatial partitioner class
   * that is given which is initialized based on the given features.
   *
   * @param features the features to create the partitioner on
   * @param partitionerClass the class of the partitioner to construct
   * @param pcriterion the partition criterion {fixed, count, size}
   * @param pvalue the value of partition criterion
   * @param sizeFunction a function that calculates the size of each feature for load balancing. Only needed if
   *                     the partition criterion is specified through partition size [[Size]]
   * @return a constructed spatial partitioner
   */
  def createPartitioner(features: JavaSpatialRDD,
                        partitionerClass: Class[_ <: SpatialPartitioner],
                        pcriterion: String,
                        pvalue: Long,
                        sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int] = {_.getHeapEstimateBytes},
                        opts: SpatialJoinOptions
                       ): SpatialPartitioner = {
    require(sizeFunction != null, "Size function cannot be null. You can use {IFeature::getHeapEstimateBytes} as a default.")
    val pc = pcriterion match {
      case "fixed" => Fixed
      case "count" => FeatureCount
      case "size" => Size
    }
    createPartitioner(features.rdd, partitionerClass, NumPartitions(pc, pvalue), f => sizeFunction.call(f), opts)
  }

  /**
   * Compute up-to three summaries as supported by the partitioner.
   * @param features the features to summarize
   * @param partitionerClass the partitioner class to compute the summaries for
   * @param sizeFunction the function the calculates the size of each feature (if size is needed)
   * @return the three computed summaries with nulls for non-computed ones
   */
  private[spatialJoin] def summarizeDataset(features: SpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                                      sizeFunction: IFeature=>Int, numPartitions: NumPartitions, opts: SpatialJoinOptions)
      : (UniformHistogram, AbstractFeatureHierarchy, Summary) = {
    var summary: Summary = null
    var abstractFeatureHierarchy: AbstractFeatureHierarchy = null
    var histogram: UniformHistogram = null

    summary = Summary.computeForFeatures(features, sizeFunction)
    val preferredNumPartitions = computeNumberOfPartitions(numPartitions, summary)
    abstractFeatureHierarchy = buildAbstractFeatureHierarchy(features, sizeFunction, summary, preferredNumPartitions, opts)
    logDebug(s"Partitioner '${partitionerClass.getSimpleName}' is being built from the full set of lightweight " +
      "feature envelopes; sampled synopsis construction is disabled in the current path.")
    (histogram, abstractFeatureHierarchy, summary)
  }

  /**
   * Builds the lightweight input hierarchy consumed by the partitioners.
   *
   * "full" mode collects one [[AbstractFeature]] per input feature to the driver.
   * "hierarchical" mode first aggregates distributed lightweight envelopes and then
   * ships only the hierarchy levels needed by the driver-side partitioner.
   */
  private def buildAbstractFeatureHierarchy(features: SpatialRDD,
                                            sizeFunction: IFeature => Int,
                                            summary: Summary,
                                            preferredNumPartitions: Int,
                                            opts: SpatialJoinOptions): AbstractFeatureHierarchy = {
    val buildMode = opts.getString(PartitionConstructionMode, "full").toLowerCase
    buildMode match {
      case "hierarchical" => aggregateAbstractFeaturesHierarchically(features, sizeFunction, summary, preferredNumPartitions, opts)
      case _ => new AbstractFeatureHierarchy(Array(collectAbstractFeatures(features, sizeFunction, opts)))
    }
  }

  /** Materializes the full dataset as lightweight per-feature descriptors. */
  private def collectAbstractFeatures(features: SpatialRDD,
                                      sizeFunction: IFeature => Int,
                                      opts: SpatialJoinOptions): Array[AbstractFeature] = {
    val complexityMode = resolveComplexityMode(opts)
    features.zipWithUniqueId().map { case (feature, oid) =>
      AbstractFeature.fromFeatureBinary(
        feature,
        oid,
        feature.getStorageSize.toLong,
        sizeFunction(feature).toLong,
        complexityMode)
    }.collect()
  }

  /**
   * Builds a coarse-to-fine hierarchy of [[AbstractFeature]]s using distributed aggregation.
   *
   * Each level groups the previous level into coarse grid cells, merges their lightweight
   * statistics, and records parent-to-child links so recursive partitioners can descend
   * without rescanning the entire finest-level array on every refinement step.
   */
  private def aggregateAbstractFeaturesHierarchically(features: SpatialRDD,
                                                      sizeFunction: IFeature => Int,
                                                      summary: Summary,
                                                      preferredNumPartitions: Int,
                                                      opts: SpatialJoinOptions): AbstractFeatureHierarchy = {
    if (summary == null || summary.isEmpty || summary.getCoordinateDimension == 0)
      return new AbstractFeatureHierarchy(Array.empty)

    val aggregationFactor = Math.max(1, opts.getInt(HierarchicalAggregationFactor, 4))
    val targetCells = {
      val configuredCells = opts.getInt(HierarchicalAggregationCells, 0)
      if (configuredCells > 0) configuredCells else Math.max(1, preferredNumPartitions * aggregationFactor)
    }
    val levels = scala.collection.mutable.ArrayBuffer[Array[AbstractFeature]]()
    val childIndexesByLevel = scala.collection.mutable.ArrayBuffer[Array[Array[Int]]]()
    val complexityMode = resolveComplexityMode(opts)
    var currentLevelRDD: RDD[AbstractFeature] = features.zipWithUniqueId().map { case (feature, oid) =>
      AbstractFeature.fromFeatureBinary(
        feature,
        oid,
        feature.getStorageSize.toLong,
        sizeFunction(feature).toLong,
        complexityMode)
    }.persist(StorageLevel.MEMORY_ONLY_SER)
    var currentLevelCollected: Array[AbstractFeature] = null
    var currentLevelCount: Long = summary.numFeatures

    var currentTargetCells = targetCells
    var level = 0
    var continue = true
    while (continue && currentTargetCells >= 1) {
      val cellsPerAxis = new Array[Int](summary.getCoordinateDimension)
      GridPartitioner.computeNumberOfPartitionsAlongAxes(summary, currentTargetCells, cellsPerAxis)

      val assignedRDD = currentLevelRDD.map { feature =>
        val cellId = locateAggregationCell(feature, summary, cellsPerAxis)
        (s"level-${level + 1}:$cellId", feature)
      }.persist(StorageLevel.MEMORY_ONLY_SER)

      val aggregatedRDD = assignedRDD
        .reduceByKey((left, right) => new AbstractFeature(left).mergeInPlace(right))
        .map { case (hierarchyKey, abstractFeature) =>
          abstractFeature
            .withAggregationLevel(level + 1)
            .withHierarchyKey(hierarchyKey)
        }
        .persist(StorageLevel.MEMORY_ONLY_SER)
      val aggregated = aggregatedRDD.collect()
      if (aggregated.isEmpty) {
        assignedRDD.unpersist(blocking = true)
        continue = false
      } else {
        if (currentLevelCollected != null) {
          val childAssignments = assignedRDD.map { case (parentHierarchyKey, childFeature) =>
            (parentHierarchyKey, childFeature.getHierarchyKey)
          }.collect()
          childIndexesByLevel.prepend(buildChildIndexes(aggregated, currentLevelCollected, childAssignments))
        }
        levels.prepend(aggregated)
        val currentCount = aggregated.length
        continue = currentCount > 1 && currentCount < currentLevelCount && currentTargetCells > 1
        assignedRDD.unpersist(blocking = true)
        currentLevelRDD.unpersist(blocking = true)
        currentLevelRDD = aggregatedRDD
        currentLevelCollected = aggregated
        currentLevelCount = currentCount
        currentTargetCells = Math.max(1, currentTargetCells / aggregationFactor)
        level += 1
      }
    }

    if (currentLevelRDD != null)
      currentLevelRDD.unpersist(blocking = true)
    if (levels.isEmpty)
      levels.append(collectAbstractFeatures(features, sizeFunction, opts))
    logInfo(s"Hierarchical partition construction built ${levels.length} abstract-feature levels; " +
      s"coarsest level size=${levels.headOption.map(_.length).getOrElse(0)}, finest level size=${levels.lastOption.map(_.length).getOrElse(0)}")
    new AbstractFeatureHierarchy(levels.toArray, childIndexesByLevel.toArray)
  }

  /**
   * Locates the coarse aggregation cell for a lightweight feature.
   *
   * The aggregation phase uses the center of the lightweight envelope because its
   * goal is only to build stable summary blocks, not the final refinement ordering
   * used by LAHilbert.
   */
  private def locateAggregationCell(feature: AbstractFeature,
                                    summary: Summary,
                                    cellsPerAxis: Array[Int]): Long = {
    var cellId = 0L
    var d = feature.getCoordinateDimension - 1
    while (d >= 0) {
      val sideLength = summary.getSideLength(d)
      val rawPosition =
        if (sideLength <= 0 || cellsPerAxis(d) <= 1) 0
        else Math.floor((feature.getCenter(d) - summary.getMinCoord(d)) * cellsPerAxis(d) / sideLength).toInt
      val position = Math.max(0, Math.min(cellsPerAxis(d) - 1, rawPosition))
      cellId = cellId * cellsPerAxis(d) + position
      d -= 1
    }
    cellId
  }

  /**
   * Reconstructs the parent-to-child index lists between two collected hierarchy levels.
   *
   * The resulting structure lets recursive partitioners descend from one aggregated
   * level to the next without scanning the full finest-level feature array again.
   */
  private def buildChildIndexes(parentLevel: Array[AbstractFeature],
                                childLevel: Array[AbstractFeature],
                                assignments: Array[(String, String)]): Array[Array[Int]] = {
    val parentIndexByKey = parentLevel.zipWithIndex.map { case (feature, index) =>
      feature.getHierarchyKey -> index
    }.toMap
    val childIndexByKey = childLevel.zipWithIndex.map { case (feature, index) =>
      feature.getHierarchyKey -> index
    }.toMap
    val groupedChildren = Array.fill(parentLevel.length)(scala.collection.mutable.ArrayBuffer[Int]())
    assignments.foreach { case (parentKey, childKey) =>
      for {
        parentIndex <- parentIndexByKey.get(parentKey)
        childIndex <- childIndexByKey.get(childKey)
      } groupedChildren(parentIndex) += childIndex
    }
    groupedChildren.map(_.toArray)
  }

  /**
   * Parses the user-facing partition criterion string such as `fixed(8)` or `size(128m)`.
   *
   * This helper is shared by the debug apps, the operator layer, and the index
   * builder so all entry points interpret partition-count hints consistently.
   *
   * @param criterionValue a user-given string in the form "method(value)"
   * @return the parsed partition criterion and value
   */
  def parsePartitionCriterion(criterionValue: String): NumPartitions = {
    val pCriterionRegexp = raw"(fixed|count|size)+\((\w+)\)".r
    criterionValue.toLowerCase match {
      case pCriterionRegexp(method, value) => {
        val pc = method match {
          case "fixed" => Fixed
          case "count" => FeatureCount
          case "size" => Size
        }
        val pvalue: Long = StringUtils.TraditionalBinaryPrefix.string2long(value)
        NumPartitions(pc, pvalue)
      }
    }
  }

  // ---- The following set of functions partition and RDD to generate a partitioned RDD using a partitioner instance

  /**
   * :: DeveloperApi :: Assigns each record to one or more partitions based on the given partitioner.
   * NOTE: This method does NOT partition the records; it just assigns each record to the overlapping partitions ID(s).
   * Each record stays in its own RDD partition.
   * @param features the set of features to assign to partitions
   * @param spatialPartitioner the partitioner to use to assign features to partitions
   * @return a new RDD where each feature is assigned to all overlapping partitions
   */
  @DeveloperApi def assignFeaturesToPartitions(features: SpatialRDD,
                                               spatialPartitioner: SpatialPartitioner): RDD[(Int, IFeature)] = {
    val featuresToPartitions: SpatialRDD = removeDisjointDuplicatesIfNeeded(features)
    val mbr: EnvelopeNDLite = new EnvelopeNDLite(spatialPartitioner.getCoordinateDimension)
    if (!spatialPartitioner.isDisjoint) {
      // Non-disjoint partitioners keep one target partition per feature, which matches
      // the current LAHilbert flow where refinement produces a non-disjoint leaf set.
      featuresToPartitions.map(f => {
        mbr.setEmpty()
        (spatialPartitioner.overlapPartition(mbr.merge(f.getGeometry)), f)
      })
    } else {
      // Disjoint partitioners need us to create a list of partition IDs for each record
      featuresToPartitions.flatMap(f => {
        val matchedPartitions = new IntArray
        mbr.setEmpty()
        mbr.merge(f.getGeometry)
        spatialPartitioner.overlapPartitions(mbr, matchedPartitions)
        val resultingPairs = Array.ofDim[(Int, IFeature)](matchedPartitions.size())
        for (i <- 0 until matchedPartitions.size())
          resultingPairs(i) = (matchedPartitions.get(i), f)
        resultingPairs
      })
    }
  }

  /** Resolves the configured feature-complexity mode used during partition planning. */
  private def resolveComplexityMode(opts: SpatialJoinOptions): String =
    if (opts == null) SpatialJoinConstants.Execution.DEFAULT_COMPLEXITY_MODE
    else opts.getString(SpatialJoinConstants.Execution.COMPLEXITY_MODE, SpatialJoinConstants.Execution.DEFAULT_COMPLEXITY_MODE)

  /** Backward-compatible alias kept for older call sites. */
  @DeveloperApi def _assignFeaturesToPartitions(features: SpatialRDD,
                                                spatialPartitioner: SpatialPartitioner): RDD[(Int, IFeature)] =
    assignFeaturesToPartitions(features, spatialPartitioner)

  /**
   * :: DeveloperApi :: Replays a previously computed OID-to-partition assignment instead of recomputing
   * feature/partition spatial relationships.
   *
   * This method is the "assignment by id" counterpart of [[assignFeaturesToPartitions]].
   * The old method decides partition membership on demand by comparing feature envelopes
   * against partition envelopes. This method assumes that the final effective partitioning
   * step has already produced stable {@code FeatureRef(oid, partitionId)} assignments, and
   * simply reloads the matching records by OID for one target partition set.
   *
   * It is therefore appropriate for the later job-execution stage, where we want to avoid
   * repeating the full spatial-assignment check and instead reuse the final partition result.
   *
   * @param featuresByOid cached OID-to-feature mapping prepared earlier from the source data
   * @param assignments final OID-to-partition assignments produced by the partitioner
   * @param targetPartitionIds partition ids requested by one later execution step or job
   * @return a pair RDD keyed by partition id and containing exactly the records assigned to these ids
   */
  @DeveloperApi def assignFeaturesToPartitions_2(featuresByOid: JavaPairRDD[java.lang.Long, IFeature],
                                                 assignments: java.lang.Iterable[SpatialPartitioner.FeatureRef],
                                                 targetPartitionIds: Array[Int]): JavaPairRDD[Integer, IFeature] = {
    if (featuresByOid == null || assignments == null || targetPartitionIds == null || targetPartitionIds.isEmpty) {
      val sparkContext = if (featuresByOid == null)
        JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate())
      else
        JavaSparkContext.fromSparkContext(featuresByOid.context)
      return sparkContext.parallelizePairs(new java.util.ArrayList[scala.Tuple2[Integer, IFeature]]())
    }

    val targetPartitionIdSet = targetPartitionIds.toSet
    val selectedAssignments = assignments.asScala.iterator
      .filter(assignment => targetPartitionIdSet.contains(assignment.getPartitionId))
      .map(assignment => (java.lang.Long.valueOf(assignment.getOid), Integer.valueOf(assignment.getPartitionId)))
      .toSeq

    val sparkContext = JavaSparkContext.fromSparkContext(featuresByOid.context)
    if (selectedAssignments.isEmpty)
      return sparkContext.parallelizePairs(new java.util.ArrayList[scala.Tuple2[Integer, IFeature]]())

    val assignmentRDD = sparkContext.parallelizePairs(selectedAssignments.asJava)
    assignmentRDD
      .join(featuresByOid)
      .mapToPair(tuple => (tuple._2._1, tuple._2._2))
  }

  /** Scala-friendly overload of [[assignFeaturesToPartitions_2]] for direct RDD use. */
  @DeveloperApi def assignFeaturesToPartitions_2(featuresByOid: RDD[(Long, IFeature)],
                                                 assignments: Seq[SpatialPartitioner.FeatureRef],
                                                 targetPartitionIds: Array[Int]): RDD[(Int, IFeature)] = {
    if (featuresByOid == null || assignments == null || targetPartitionIds == null || targetPartitionIds.isEmpty)
      return org.apache.spark.SparkContext.getOrCreate().emptyRDD[(Int, IFeature)]
    val targetPartitionIdSet = targetPartitionIds.toSet
    val selectedAssignments = assignments.iterator
      .filter(assignment => targetPartitionIdSet.contains(assignment.getPartitionId))
      .map(assignment => (assignment.getOid, assignment.getPartitionId))
      .toSeq
    if (selectedAssignments.isEmpty)
      return featuresByOid.sparkContext.emptyRDD[(Int, IFeature)]
    val assignmentRDD = featuresByOid.sparkContext.parallelize(selectedAssignments)
    assignmentRDD.join(featuresByOid).map { case (_, (partitionId, feature)) => (partitionId, feature) }
  }

  /**
   * :: DeveloperApi :: Keeps only the requested Spark partitions from an already partitioned pair RDD.
   *
   * This helper is the physical counterpart of the logical "job contains these partition ids" rule.
   * Unlike filtering by key after the fact, it drops whole Spark partitions by index while preserving
   * the existing spatial partitioner, so later pair-RDD operations can continue to treat the result as
   * already partitioned by the final effective partition ids.
   *
   * @param partitioned already partitioned Scala pair RDD keyed by effective partition id
   * @param targetPartitionIds partition ids that should remain visible to the current job
   * @return a partition-preserving view that keeps only the requested physical partitions
   */
  @DeveloperApi def selectAssignedPartitions(partitioned: RDD[(Int, IFeature)],
                                             targetPartitionIds: Array[Int]): RDD[(Int, IFeature)] = {
    if (partitioned == null || targetPartitionIds == null || targetPartitionIds.isEmpty)
      return org.apache.spark.SparkContext.getOrCreate().emptyRDD[(Int, IFeature)]
    val targetPartitionIdSet = targetPartitionIds.toSet
    partitioned.mapPartitionsWithIndex((partitionId, iterator) => {
      if (targetPartitionIdSet.contains(partitionId)) iterator else Iterator.empty
    }, preservesPartitioning = true)
  }

  /**
   * :: DeveloperApi :: Java-friendly overload of [[selectAssignedPartitions]].
   *
   * @param partitioned already partitioned Java pair RDD keyed by effective partition id
   * @param targetPartitionIds partition ids that should remain visible to the current job
   * @return a partition-preserving Java pair RDD view over only the requested Spark partitions
   */
  @DeveloperApi def selectAssignedPartitions(partitioned: JavaPairRDD[Integer, IFeature],
                                             targetPartitionIds: Array[Int]): JavaPairRDD[Integer, IFeature] = {
    if (partitioned == null || targetPartitionIds == null || targetPartitionIds.isEmpty) {
      val sparkContext = if (partitioned == null)
        JavaSparkContext.fromSparkContext(org.apache.spark.SparkContext.getOrCreate())
      else
        JavaSparkContext.fromSparkContext(partitioned.context)
      return sparkContext.parallelizePairs(new java.util.ArrayList[scala.Tuple2[Integer, IFeature]]())
    }
    val targetPartitionIdSet = targetPartitionIds.toSet
    val selected = partitioned.rdd.mapPartitionsWithIndex((partitionId, iterator) => {
      if (targetPartitionIdSet.contains(partitionId)) iterator else Iterator.empty
    }, preservesPartitioning = true)
    JavaPairRDD.fromRDD[Integer, IFeature](selected)
  }

  /**
   * Partitions the given features using an already initialized [[SpatialPartitioner]]
   * @param features the features to partition
   * @param spatialPartitioner the spatial partition to use.
   * @return a [[SpatialRDD]] that is partitioned
   */
  def partitionFeatures(features: SpatialRDD, spatialPartitioner: SpatialPartitioner): SpatialRDD = {
    assignFeaturesToPartitions(features, spatialPartitioner)
      .partitionBy(spatialPartitioner)
      .mapPartitions(_.map(_._2), preservesPartitioning = true)
  }

  /**
   * Removes duplicated records only when the upstream RDD already carries a disjoint partitioner.
   *
   * This helper is used before repartitioning so we do not compound duplicates that
   * are artifacts of an earlier disjoint partitioning step. For non-spatial or
   * non-disjoint inputs it simply returns the original RDD unchanged.
   *
   * @param features the set of features to remove the duplicates from.
   * @return a set of features with all duplicates removed.
   */
  private[spatialJoin] def removeDisjointDuplicatesIfNeeded(features: SpatialRDD): SpatialRDD = {
    val partitioner = features.partitioner
    if (partitioner.isEmpty || !partitioner.get.isInstanceOf[SpatialPartitioner])
      return features
    val spatialPartitioner = partitioner.get.asInstanceOf[SpatialPartitioner]
    if (!spatialPartitioner.isDisjoint)
      return features
    features.mapPartitionsWithIndex((partitionID, features) => {
      val referenceMBR = spatialPartitioner.getPartitionMBR(partitionID)
      val geometryMBR: EnvelopeNDLite = new EnvelopeNDLite(referenceMBR.getCoordinateDimension)
      val referencePoint: Array[Double] = new Array[Double](referenceMBR.getCoordinateDimension)
      features.filter(f => {
        geometryMBR.setEmpty()
        geometryMBR.merge(f.getGeometry)
        for (d <- 0 until geometryMBR.getCoordinateDimension)
          referencePoint(d) = geometryMBR.getMinCoord(d)
        referenceMBR.containsPoint(referencePoint)
      })
    }, preservesPartitioning = true)
  }

  /** Backward-compatible alias kept for older call sites. */
  private[spatialJoin] def runDuplicateAvoidance(features: SpatialRDD): SpatialRDD =
    removeDisjointDuplicatesIfNeeded(features)


  /**
   * Partitions a JavaSpatialRDD using the given spatial partitioner and returns a new partitioned RDD.
   * If the given partitioner is configured to be disjoint, the returned RDD might contain some replication.
   * @param features the set of features to partition
   * @param partitioner the partitioner to use to partition the features
   * @return the partitioned RDD
   */
  def partitionFeatures(features: JavaSpatialRDD, partitioner: SpatialPartitioner): JavaSpatialRDD =
    JavaRDD.fromRDD(partitionFeatures(features.rdd, partitioner))

  // ---- The following set of functions partition a SpatialRDD given a partitioner class

  /**
   * Partitions the given features using a partitioner of the given type. This method first initializes the partitioner
   * and then uses this initialized partitioner to partition the data.
   * @param features the set of features to spatially partition
   * @param partitionerClass the type of the spatial partition
   * @param sizeFunction the function used to computed the size
   * @param opts additional options
   * @return the same set of input features after they are partitioned.
   */
  def partitionFeatures(features: SpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                        sizeFunction: IFeature=>Int, opts: SpatialJoinOptions): SpatialRDD = {
    val pInfo = parsePartitionCriterion(
      opts.getString(IndexHelper.PartitionCriterionThreshold, s"size(${SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_SIZE})"))
    val spatialPartitioner = createPartitioner(features, partitionerClass, pInfo, sizeFunction, opts)
    partitionFeatures(features, spatialPartitioner)
  }



  /**
   * (Java shortcut) Partition features based on the given partitioner class.
   * @param features a set of features
   * @param partitionerClass the class of the partitioner
   * @param sizeFunction the function that estimates the size of each record
   * @param opts additional options
   * @return a new RDD where the features are partitioned based on the given partitioner class
   */
  def partitionFeatures(features: JavaSpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                        sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                         opts: SpatialJoinOptions) : JavaSpatialRDD =
    JavaRDD.fromRDD(partitionFeatures(features.rdd, partitionerClass, { f => sizeFunction.call(f) }, opts))


  // ---- The following functions provides access to the set of configured partitioners
  /** A table of all the partitioners available */
  lazy val partitioners: Map[String, Class[_ <: SpatialPartitioner]] = {
    val partitionerClasses: Seq[Class[_ <: SpatialPartitioner]] = Seq(
      classOf[GridPartitioner],
      classOf[HCurvePartitioner],
      classOf[STRPartitioner],
      classOf[LAHilbertPartitioner]
    )
    partitionerClasses.map { partitionerClass =>
      val metadata = partitionerClass.getAnnotation(classOf[SpatialPartitioner.Metadata])
      if (metadata == null) {
        logWarning(s"Skipping partitioner '${partitionerClass.getName}' without a valid Metadata annotation")
        null
      } else {
        metadata.extension -> partitionerClass
      }
    }.filter(_ != null).toMap
  }

  import scala.collection.convert.ImplicitConversionsToJava._
  /** Exposes the registered partitioner table to Java callers and debug utilities. */
  def getPartitioners: java.util.Map[String, Class[_ <: SpatialPartitioner]] = partitioners


}
