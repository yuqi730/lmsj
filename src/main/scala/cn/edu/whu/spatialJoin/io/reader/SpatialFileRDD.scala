package cn.edu.whu.spatialJoin.io.reader

import cn.edu.whu.spatialJoin.algortithms.SpatialDataTypes.SpatialRDD
import cn.edu.whu.spatialJoin.algortithms.SpatialPartition
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions
import cn.edu.whu.spatialJoin.enums.GeometryType
import cn.edu.whu.spatialJoin.geom.{EnvelopeND, EnvelopeNDLite, IFeature}
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper
import cn.edu.whu.spatialJoin.utils.OperationParam
import org.apache.commons.io.IOCase
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.LineReader
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable

class SpatialFileRDD(@transient sc: SparkContext,
                     path: String,
                     var opts: SpatialJoinOptions = new SpatialJoinOptions())
  extends SpatialRDD(sc, Seq()) with Logging {

  opts = new SpatialJoinOptions(sc.hadoopConfiguration).mergeWith(sc.getConf).mergeWith(opts)

  lazy val featureReaderClass: Class[_ <: FeatureReader] = SpatialFileRDD.resolveFeatureReaderClass(path, opts)

  lazy val readerMetadata: SpatialReaderMetadata =
    featureReaderClass.getAnnotation[SpatialReaderMetadata](classOf[SpatialReaderMetadata])

  private var applyDuplicateAvoidance: Boolean = true

  override def getPartitions: Array[Partition] =
    SpatialFileRDD.buildPartitions(path, opts, sc.hadoopConfiguration).asInstanceOf[Array[Partition]]

  @transient override val partitioner: Option[Partitioner] = None

  override def compute(split: Partition, context: TaskContext): Iterator[IFeature] =
    SpatialFileRDD.readFilePartition(split.asInstanceOf[SpatialFileRDD.FilePartition], featureReaderClass,
      applyDuplicateAvoidance, opts)

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[SpatialFileRDD.FilePartition].locations
}

object SpatialFileRDD extends Logging {

  class FilePartition(override val index: Int,
                      val path: String,
                      val offset: Long,
                      val length: Long,
                      val locations: Array[String]) extends Partition {

    override def equals(other: Any): Boolean = other match {
      case p: FilePartition => this.path == p.path && this.offset == p.offset && this.length == p.length
      case _ => false
    }
  }

  class SpatialFilePartition(override val index: Int,
                             path: String,
                             offset: Long,
                             length: Long,
                             locations: Array[String],
                             @transient partitionInfo: IFeature)
    extends FilePartition(index, path, offset, length, locations) with SpatialPartition {

    override val mbr: EnvelopeNDLite = new EnvelopeNDLite().merge(partitionInfo.getGeometry)
    override def size: Long = length
    override val numFeatures: Long = getRowValueOrElse(partitionInfo, "Record Count", "-1").toLong
    override val numNonEmptyGeometries: Long = getRowValueOrElse(partitionInfo, "NonEmpty Count", "-1").toLong
    override val numPoints: Long = getRowValueOrElse(partitionInfo, "NumPoints", "-1").toLong
    override val sumSideLength: Array[Double] = new Array[Double](mbr.getCoordinateDimension)
    for (i <- sumSideLength.indices)
      sumSideLength(i) = getRowValueOrElse(partitionInfo, "sum" + GeometryHelper.DimensionNames(i), "NaN").toDouble
    override val geometryType: GeometryType =
      getRowValueOrElse(partitionInfo, "GeometryType", GeometryType.GeometryCollectionName) match {
        case GeometryType.EmptyName => GeometryType.EMPTY
        case GeometryType.PointName => GeometryType.POINT
        case GeometryType.LineStringName => GeometryType.LINESTRING
        case GeometryType.PolygonName => GeometryType.POLYGON
        case GeometryType.MultiPointName => GeometryType.MULTIPOINT
        case GeometryType.MultiLineStringName => GeometryType.MULTILINESTRING
        case GeometryType.MultiPolygonName => GeometryType.MULTIPOLYGON
        case _ => GeometryType.GEOMETRYCOLLECTION
      }
  }

  /** Reads one optional attribute from a Spark row, returning a default when the attribute is absent. */
  def getRowValueOrElse[T](row: Row, attName: String, defaultValue: T): T =
    getRowValueOrElse(row, attName, () => defaultValue)

  /**
   * Lazy-default overload of [[getRowValueOrElse]].
   *
   * This version is used when constructing spatial file partitions so expensive
   * default creation is skipped unless the attribute is truly missing.
   */
  def getRowValueOrElse[T](row: Row, attName: String, getter: () => T): T = {
    if (row.schema.fieldNames == null || !row.schema.fieldNames.contains(attName))
      return getter.apply()
    row.getAs[T](attName)
  }

  @OperationParam(description =
    """The format of the input file {point(xcol,ycol),envelope(x1col,y1col,x2col,y2col),wkt(gcol)}
      |	point(xcol,ycol) indicates a CSV input where xcol and ycol indicate the indexes of the columns that contain the x and y coordinates
      |	envelope(x1col,y1col,x2col,y2col) indicate an input that contains rectangles stored in (x1,y1,x2,y2) format
      |	wkt(gcol) indicate a CSV file with the field (gcol) containing a WKT-encoded geometry.
      |	shapefile: Esri shapefile. Accepts both .shp+.shx+.dbf files or a compressed .zip file with these three files
      |	geojson: GeoJSON file containing features with geometries and properties (attributes)""",
    required = false)
  val InputFormat = "iformat"

  @OperationParam(description = "An optional MBR to filter the input. Format: x1,y1,x2,y2")
  val FilterMBR = "filtermbr"

  @OperationParam(description = "Process the input recursively", defaultValue = "false")
  val Recursive = "recursive"

  val SplitFiles = "SpatialInputFormat.SplitFiles"

  @OperationParam(description = "Minimum split size", showInUsage = false)
  val MinSplitSize: String = FileInputFormat.SPLIT_MINSIZE

  @OperationParam(description = "Maximum split size", showInUsage = false)
  val MaxSplitSize: String = FileInputFormat.SPLIT_MAXSIZE

  val HiddenFileFilter: PathFilter = (p: Path) => p.getName.charAt(0) != '_' && p.getName.charAt(0) != '.'

  val MasterFileFilter: PathFilter = (p: Path) => p.getName.startsWith("_master")

  /**
   * Tries every registered reader until one of them can recognize the input.
   *
   * Reader auto-detection is intentionally centralized here so that callers can
   * keep using the same read entry regardless of whether the format is explicit
   * or delegated to the registry.
   */
  def autodetectInputFormat(path: String,
                            opts: SpatialJoinOptions): (Class[_ <: FeatureReader], SpatialJoinOptions) = {
    val allReaders = FeatureReader.getFeatureReaders.values.iterator().asScala
    var detectedOpts: SpatialJoinOptions = null
    var readerClass: Class[_ <: FeatureReader] = null
    while (detectedOpts == null && allReaders.hasNext) {
      readerClass = allReaders.next()
      val reader = readerClass.newInstance()
      detectedOpts = reader.autoDetect(opts, path)
    }
    if (detectedOpts == null) null else (readerClass, detectedOpts)
  }

  /**
   * Resolves the newest master file inside an indexed directory, if one exists.
   *
   * Indexed directories produced by earlier preprocessing stages may contain one
   * or more `_master*` files. This method picks the latest one and lets partition
   * construction reuse the stored spatial metadata.
   */
  def resolveMasterFilePath(fileSystem: FileSystem, indexPath: Path): Path = {
    val masterFiles = fileSystem.listStatus(indexPath, MasterFileFilter)
    if (masterFiles.isEmpty) return null
    var iMostRecent = 0
    for (i <- 1 until masterFiles.length) {
      if (masterFiles(i).getModificationTime > masterFiles(iMostRecent).getModificationTime)
        iMostRecent = i
    }
    masterFiles(iMostRecent).getPath
  }

  /**
   * Expands one physical file range into one or more Spark input partitions.
   *
   * The method honors the reader split policy and optionally attaches spatial
   * partition metadata when the file came from a master-indexed directory.
   */
  private def appendFilePartitions(opts: SpatialJoinOptions,
                                   fileSystem: FileSystem,
                                   fileStatus: FileStatus,
                                   start: Long,
                                   length: Long,
                                   noSplit: Boolean,
                                   partitionInfo: IFeature,
                                   partitions: mutable.ArrayBuffer[FilePartition]): Unit = {
    val splitSize = if (noSplit) {
      length
    } else {
      val minSize = opts.getLong(MinSplitSize, 1)
      val maxSize = opts.getLong(MaxSplitSize, Long.MaxValue)
      Math.max(minSize, Math.min(maxSize, fileStatus.getBlockSize))
    }
    val blkLocations = fileSystem.getFileBlockLocations(fileStatus, start, length)
    var partitionStart = start
    val fileEnd = start + length
    val splitSlop = 1.1
    while (partitionStart < fileEnd) {
      val blkIndex = blkLocations.find(bl => partitionStart >= bl.getOffset && partitionStart < bl.getOffset + bl.getLength)
      require(blkIndex.isDefined, s"No locations found for block at offset $partitionStart")
      val partitionEnd =
        if ((fileEnd - partitionStart).toDouble / splitSize > splitSlop) partitionStart + splitSize else fileEnd
      partitions.append(
        if (partitionInfo != null)
          new SpatialFilePartition(partitions.length, fileStatus.getPath.toString, partitionStart,
            partitionEnd - partitionStart, blkIndex.get.getHosts, partitionInfo)
        else
          new FilePartition(partitions.length, fileStatus.getPath.toString, partitionStart,
            partitionEnd - partitionStart, blkIndex.get.getHosts)
      )
      partitionStart = partitionEnd
    }
  }

  /**
   * Expands the user path into Spark partitions.
   *
   * This method handles plain files, recursive directory traversal, and indexed
   * directories with a master file. The resulting partitions carry optional
   * spatial metadata so later duplicate avoidance can use the file partition MBR.
   */
  def buildPartitions(path: String, opts: SpatialJoinOptions, conf: Configuration): Array[FilePartition] = {
    val inputPaths = path.split(",")
    val pathsToInspect = mutable.ArrayBuffer[String](inputPaths: _*)
    val recursive = opts.getBoolean(Recursive, false)
    val hadoopConf = opts.loadIntoHadoopConf(new Configuration(conf))
    val featureReaderClass = resolveFeatureReaderClass(inputPaths(0), opts)
    val readerMetadata = featureReaderClass.getAnnotation(classOf[SpatialReaderMetadata])
    val splitFiles = opts.getBoolean(SpatialFileRDD.SplitFiles, true) && !readerMetadata.noSplit()
    var fileFilters: Seq[PathFilter] = Seq(SpatialFileRDD.HiddenFileFilter)
    val wildcard = readerMetadata.filter()
    if (wildcard != null && wildcard.nonEmpty) {
      val wildcardFilter = new WildcardFileFilter(wildcard.split("\n"), IOCase.SYSTEM)
      val wildcardPathFilter: PathFilter = p => wildcardFilter.accept(new File(p.getName))
      fileFilters = fileFilters :+ wildcardPathFilter
    }
    val fileFilter: PathFilter =
      if (fileFilters.length == 1) fileFilters.head else p => fileFilters.forall(filter => filter.accept(p))

    val partitions = mutable.ArrayBuffer[FilePartition]()

    while (pathsToInspect.nonEmpty) {
      val pathToInspect = new Path(pathsToInspect.remove(pathsToInspect.length - 1))
      val fs = pathToInspect.getFileSystem(hadoopConf)
      val fileStatus = fs.getFileStatus(pathToInspect)
      val pathGivenByUser = inputPaths.exists(p => new Path(p).equals(pathToInspect))
      if (fileStatus.isFile && (fileFilter.accept(fileStatus.getPath) || pathGivenByUser)) {
        appendFilePartitions(opts, fs, fileStatus, 0, fileStatus.getLen, !splitFiles, null, partitions)
      } else if (fileStatus.isDirectory && (recursive || pathGivenByUser)) {
        val masterFilePath = resolveMasterFilePath(fs, fileStatus.getPath)
        if (masterFilePath != null) {
          val lineReader = new LineReader(fs.open(masterFilePath))
          val headerLine = new Text()
          lineReader.readLine(headerLine)
          lineReader.close()
          val headerParts = headerLine.toString.split("\t")
          val numDimensions = (headerParts.length - headerParts.indexOf("xmin")) / 2
          val masterFileReader = new CSVFeatureReader
          val masterFileOpts = new SpatialJoinOptions(opts)
            .set(CSVFeatureReader.SkipHeader, true)
            .set(SpatialFileRDD.InputFormat, s"envelopek($numDimensions,xmin)")
            .set(CSVFeatureReader.FieldSeparator, "\t")
            .set(CSVFeatureReader.QuoteCharacters, "\'\'\"\"")
          masterFileReader.initialize(masterFilePath, masterFileOpts)
          try {
            for (spatialPartition <- masterFileReader.iterator().asScala) {
              val partitionPath = new Path(fileStatus.getPath, spatialPartition.getAs("File Name").toString)
              if (fileFilter.accept(partitionPath)) {
                val start = 0L
                val length = spatialPartition.getAs("Data Size").toString.toLong
                appendFilePartitions(opts, fs, fs.getFileStatus(partitionPath), start, length, !splitFiles,
                  spatialPartition, partitions)
              }
            }
          } finally {
            masterFileReader.close()
          }
        } else {
          for (subfile <- fs.listStatus(fileStatus.getPath))
            pathsToInspect.append(subfile.getPath.toString)
        }
      }
    }
    partitions.toArray
  }

  /**
   * Resolves the reader class from an explicit format or by auto-detection.
   *
   * The matching order is:
   * 1. exact short-name match from reader metadata
   * 2. reader-specific {@code isRecognized} check
   * 3. auto-detection when the format is omitted or set to {@code *auto*}
   */
  /**
   * Resolves the concrete feature reader class that should parse the input.
   *
   * This is the central reader-selection method used by the unified local and
   * distributed read paths.
   */
  def resolveFeatureReaderClass(path: String, opts: SpatialJoinOptions): Class[_ <: FeatureReader] = {
    val iformat = opts.getString(SpatialFileRDD.InputFormat)
    var ifClass: Class[_ <: FeatureReader] = null
    if (iformat == null || iformat == "*auto*") {
      if (path != null) {
        val detected = SpatialFileRDD.autodetectInputFormat(path, opts)
        require(detected != null, s"Input format could not be detected for input '$path'")
        ifClass = detected._1
        opts.mergeWith(detected._2)
      }
    } else {
      var allReaders = FeatureReader.getFeatureReaders.values().iterator().asScala
      while (ifClass == null && allReaders.hasNext) {
        val readerClass = allReaders.next()
        val metadata = readerClass.getAnnotation(classOf[SpatialReaderMetadata])
        if ((metadata != null && metadata.shortName().equalsIgnoreCase(iformat)) ||
          readerClass.newInstance().isRecognized(iformat))
          ifClass = readerClass
      }
      if (ifClass == null) {
        allReaders = FeatureReader.getFeatureReaders.values().iterator().asScala
        var corrections = Array[String]()
        for (readerClass <- allReaders) {
          val c = readerClass.newInstance().iformatCorrections(iformat)
          if (c != null)
            corrections = corrections ++ c
        }
        throw new RuntimeException(s"Input format '$iformat' cannot be recognized. " +
          s"Perhaps you mean one of {${corrections.mkString(",")}}")
      }
    }
    ifClass
  }

  /**
   * Reads one file split through the selected reader.
   *
   * When the split carries a spatial partition MBR, the returned iterator applies
   * reference-point duplicate avoidance so boundary records are emitted only from
   * the partition that owns the lower-left reference point.
   */
  def readFilePartition(partition: FilePartition,
                        featureReaderClass: Class[_ <: FeatureReader],
                        applyDuplicateAvoidance: Boolean,
                        opts: SpatialJoinOptions): Iterator[IFeature] = {
    val featureReader = featureReaderClass.newInstance()
    featureReader.initialize(partition, opts)
    val features = featureReader.iterator().asScala
    if (!partition.isInstanceOf[SpatialFilePartition] ||
      partition.asInstanceOf[SpatialFilePartition].mbr == null ||
      !applyDuplicateAvoidance) {
      features
    } else {
      val mbr = partition.asInstanceOf[SpatialFilePartition].mbr
      features.filter(f => {
        val recordMBR = new EnvelopeND(f.getGeometry.getFactory).merge(f.getGeometry)
        var refPointInMBR = true
        var d = 0
        while (d < recordMBR.getCoordinateDimension && refPointInMBR) {
          refPointInMBR = recordMBR.getMinCoord(d) >= mbr.getMinCoord(d) &&
            recordMBR.getMinCoord(d) < mbr.getMaxCoord(d)
          if (!refPointInMBR && recordMBR.getSideLength(d) == 0)
            refPointInMBR = recordMBR.getMinCoord(d) == mbr.getMaxCoord(d)
          d += 1
        }
        refPointInMBR
      })
    }
  }

  /** Java-friendly alias for [[readFilePartition]]. */
  def readFilePartitionJ(partition: FilePartition,
                         featureReaderClass: Class[_ <: FeatureReader],
                         opts: SpatialJoinOptions): java.util.Iterator[IFeature] =
    readFilePartition(partition, featureReaderClass, applyDuplicateAvoidance = true, opts).asJava

  /**
   * Reads a path locally by iterating over the resolved file partitions one by one.
   *
   * This is the non-Spark debug path used by local tests and debug applications.
   */
  def readPathLocally(path: String,
                      iformat: String,
                      opts: SpatialJoinOptions,
                      conf: Configuration): Iterator[IFeature] = {
    val readerOpts =
      if (opts != null) new SpatialJoinOptions(opts).set(SpatialFileRDD.InputFormat, iformat)
      else new SpatialJoinOptions(SpatialFileRDD.InputFormat -> iformat)
    val readerClass = resolveFeatureReaderClass(path, readerOpts)
    val partitions = buildPartitions(path, readerOpts, conf)
    new Iterator[IFeature] {
      private var currentPartition = 0
      private var currentIterator: Iterator[IFeature] =
        if (partitions.isEmpty) Iterator.empty
        else SpatialFileRDD.readFilePartition(partitions(currentPartition), readerClass, applyDuplicateAvoidance = true, readerOpts)

      override def hasNext: Boolean = currentIterator.hasNext || currentPartition < partitions.length - 1

      override def next(): IFeature = {
        if (!currentIterator.hasNext && currentPartition < partitions.length - 1) {
          currentPartition += 1
          currentIterator = SpatialFileRDD.readFilePartition(partitions(currentPartition), readerClass,
            applyDuplicateAvoidance = true, readerOpts)
        }
        currentIterator.next()
      }
    }
  }

  /** Java-friendly alias for [[readPathLocally]]. */
  def readLocalJ(path: String,
                 format: String,
                 opts: SpatialJoinOptions,
                 conf: Configuration): java.util.Iterator[IFeature] =
    readPathLocally(path, format, opts, conf).asJava

  /**
   * Disables duplicate avoidance on all reachable [[SpatialFileRDD]] ancestors.
   *
   * Some downstream workflows intentionally want to keep boundary duplicates.
   * This helper walks the dependency chain and flips the flag on every source
   * RDD participating in that branch.
   */
  private[reader] def disableDuplicateAvoidanceRecursively(rdd: RDD[_]): Unit = {
    val rddsToCheck = new mutable.ArrayBuffer[RDD[_]]()
    rddsToCheck.append(rdd)
    while (rddsToCheck.nonEmpty) {
      val rddToCheck = rddsToCheck.remove(rddsToCheck.length - 1)
      if (rddToCheck.isInstanceOf[SpatialFileRDD]) {
        rddToCheck.asInstanceOf[SpatialFileRDD].applyDuplicateAvoidance = false
      } else {
        for (dependency <- rddToCheck.dependencies)
          rddsToCheck.append(dependency.rdd)
      }
    }
  }

  /** Backward-compatible alias kept for older call sites. */
  def rowGetOrElse[T](row: Row, attName: String, defaultValue: T): T =
    getRowValueOrElse(row, attName, defaultValue)

  /** Backward-compatible lazy-default alias. */
  def rowGetOrElse[T](row: Row, attName: String, getter: () => T): T =
    getRowValueOrElse(row, attName, getter)

  /** Backward-compatible alias kept for older call sites. */
  def getFeatureReaderClass(path: String, opts: SpatialJoinOptions): Class[_ <: FeatureReader] =
    resolveFeatureReaderClass(path, opts)

  /** Backward-compatible alias kept for older call sites. */
  def createPartitions(path: String, opts: SpatialJoinOptions, conf: Configuration): Array[FilePartition] =
    buildPartitions(path, opts, conf)

  /** Backward-compatible alias kept for older call sites. */
  def readPartition(partition: FilePartition,
                    featureReaderClass: Class[_ <: FeatureReader],
                    applyDuplicateAvoidance: Boolean,
                    opts: SpatialJoinOptions): Iterator[IFeature] =
    readFilePartition(partition, featureReaderClass, applyDuplicateAvoidance, opts)

  /** Backward-compatible alias kept for older call sites. */
  def readPartitionJ(partition: FilePartition,
                     featureReaderClass: Class[_ <: FeatureReader],
                     opts: SpatialJoinOptions): java.util.Iterator[IFeature] =
    readFilePartitionJ(partition, featureReaderClass, opts)

  /** Backward-compatible alias kept for older call sites. */
  def readLocal(path: String,
                iformat: String,
                opts: SpatialJoinOptions,
                conf: Configuration): Iterator[IFeature] =
    readPathLocally(path, iformat, opts, conf)

  /** Backward-compatible alias kept for older call sites. */
  def getMasterFilePath(fileSystem: FileSystem, indexPath: Path): Path =
    resolveMasterFilePath(fileSystem, indexPath)

  /** Backward-compatible alias kept for older call sites. */
  private[reader] def skipDuplicateAvoidance(rdd: RDD[_]): Unit =
    disableDuplicateAvoidanceRecursively(rdd)
}
