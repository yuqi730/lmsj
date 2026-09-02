package cn.edu.whu.spatialJoin.common

import cn.edu.whu.spatialJoin.algortithms.SpatialDataTypes.{JavaSpatialRDD, SpatialRDD}
import cn.edu.whu.spatialJoin.algortithms.SpatialPartition
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeature
import cn.edu.whu.spatialJoin.enums.GeometryType
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper
import cn.edu.whu.spatialJoin.geom.{EnvelopeND, EnvelopeNDLite, IFeature}
import com.fasterxml.jackson.core.JsonGenerator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry
import utils.LRUCache

import java.io.IOException

/**
 * A fixed-size summary for a set of features.
 */
class Summary extends EnvelopeNDLite {

  /** Total estimated storage size for the features */
  var size: Long = 0L

  /** Total number of features in the data file */
  var numFeatures: Long = 0L

  /** Total number of points in the geometries. e.g., linestrings and polygons can contribute more than one point */
  var numPoints: Long = 0L

  /** Count the number of non-empty geometries. This is used to calculate the average side length correctly */
  var numNonEmptyGeometries: Long = 0L

  /** The sum of side length along each dimension. Can be used to find the average side length. */
  var sumSideLength: Array[Double] = _

  /** The sum of volumes (area for 2D rectangles) of all bounding box of non-empty geometries. */
  private var sumVolume: Double = _

  /** The type of geometry stored in the dataset. */
  var geometryType = GeometryType.EMPTY

  /**
   * Copy constructor
   * @param other another summary to copy from
   */
  def this(other: Summary) {
    this()
    super.set(other)
    this.size = other.size
    this.numFeatures = other.numFeatures
    this.numPoints = other.numPoints
    this.numNonEmptyGeometries = other.numNonEmptyGeometries
    if (other.sumSideLength != null)
      this.sumSideLength = Array(other.sumSideLength: _*)
    this.geometryType = other.geometryType
  }

  protected def setNumPoints(p: Long): Unit = this.numPoints = p
  protected def setNumFeatures(f: Long): Unit = this.numFeatures = f
  protected def setSize(s: Long): Unit = this.size = s

  /** Increments the feature counter while incremental summaries are being merged. */
  def incrementNumFeatures(inc: Long = 1): Unit = this.numFeatures += inc

  /**
   * Resizes the summary to a target coordinate dimension and allocates the
   * side-length accumulator array used by average-length statistics.
   */
  override def setCoordinateDimension(numDimensions: Int): Unit = {
    super.setCoordinateDimension(numDimensions)
    if (this.sumSideLength == null || this.sumSideLength.length != numDimensions)
      this.sumSideLength = new Array[Double](numDimensions)
  }

  /**
   * Expands the summary to enclose another partial summary. This can be used for incremental computation relying on
   * the associative and commutative properties on the attributes in the summary.
   * @param other the other summary to expand to
   * @return this summary so that the method can be called serially
   */
  def expandToSummary(other: Summary): Summary = {
    if (other.isEmpty)
      return this
    if (this.isEmpty)
      this.setCoordinateDimension(other.getCoordinateDimension)
    assert(this.getCoordinateDimension == other.getCoordinateDimension,
      s"Incompatible number of dimensions ${this.getCoordinateDimension} != ${other.getCoordinateDimension}")
    merge(other)
    this.numFeatures += other.numFeatures
    this.size += other.size
    this.numPoints += other.numPoints
    for (d <- this.sumSideLength.indices)
      this.sumSideLength(d) += other.sumSideLength(d)
    this.numNonEmptyGeometries += other.numNonEmptyGeometries
    mergeWithGeometryType(other.geometryType)
    this
  }

  /**
   * Update the geometry type based on the given one.
   * @param otherGeometryType the geometry type to include in this summary
   */
  protected def mergeWithGeometryType(otherGeometryType: GeometryType): Unit =
    this.geometryType = this.geometryType.coerce(otherGeometryType)

  /**
   * Expands the summary to enclose the given geometry and update the size with the given size
   *
   * @param geometry a geometry to include in this summary
   * @param size     the size of the given geometry in bytes
   */
  def expandToGeometryWithSize(geometry: Geometry, size: Int): Unit = {
    this.numFeatures += 1
    this.size += size
    if (geometry == null)
      return
    if (this.getCoordinateDimension == 0)
      this.setCoordinateDimension(GeometryHelper.getCoordinateDimension(geometry))
    merge(geometry)
    this.mergeWithGeometryType(Summary.GeometryNameToType(geometry.getGeometryType))
    if (geometry.isEmpty) return
    this.numNonEmptyGeometries += 1
    this.numPoints += geometry.getNumPoints
    val e = new EnvelopeND(geometry.getFactory, this.getCoordinateDimension)
    e.merge(geometry)
    this.sumVolume += e.getArea
    for (d <- sumSideLength.indices)
      sumSideLength(d) += e.getSideLength(d)
  }

  /**
   * Expands the summary to enclose the given geometry
   * @param geom the geometry to include in this summary
   */
  def expandToGeometry(geom: Geometry): Unit = expandToGeometryWithSize(geom, GeometryHelper.getGeometryStorageSize(geom))

  /**
   * Expands the summary to enclose the given feature. In addition to what [[expandToGeometry()]] does,
   * it increase the total size to account for the non-geometric attributes in this feature.
   *
   * @param f the feature to include in this summary
   */
  def expandToFeature(f: IFeature): Unit = {
    if (f == null)
      return
    expandToGeometryWithSize(f.getGeometry, f.getStorageSize)
  }

  /**
   * Expands the summary to enclose a lightweight feature descriptor used during partition construction.
   *
   * The byte size accumulated here is the lightweight feature's heap estimate,
   * which keeps partition construction aligned with the memory pressure expected
   * later during in-memory join execution.
   *
   * @param f the lightweight per-feature partition descriptor
   */
  def expandToAbstractFeature(f: AbstractFeature): Unit = {
    if (f == null) {
      return
    }
    this.numFeatures += 1
    this.size += f.getHeapEstimateBytes
    this.numPoints += f.getNumPoints
    this.mergeWithGeometryType(f.getGeometryType)
    if (f.isEmpty)
      return
    if (this.getCoordinateDimension == 0)
      this.setCoordinateDimension(f.getCoordinateDimension)
    merge(f)
    this.numNonEmptyGeometries += f.getMemberCount
    this.sumVolume += f.getEnvelopeArea
    for (d <- sumSideLength.indices)
      sumSideLength(d) += f.getSideLength(d)
  }

  override def setEmpty(): Unit = {
    super.setEmpty()
    this.numFeatures = 0
    this.size = 0
    this.numPoints = 0
    this.numNonEmptyGeometries = 0
    this.geometryType = GeometryType.EMPTY
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) return false
    val that = other.asInstanceOf[Summary]
    if (this.numFeatures != that.numFeatures) return false
    if (this.size != that.size) return false
    if (this.numPoints != that.numPoints) return false
    if (this.numNonEmptyGeometries != that.numNonEmptyGeometries) return false
    if (this.geometryType ne that.geometryType) return false
    for ($d <- this.sumSideLength.indices) {
      if (this.sumSideLength($d) != that.sumSideLength($d)) return false
    }
    true
  }

  /** Returns the average envelope side length of non-empty geometries along one dimension. */
  def averageSideLength(i: Int): Double =
    if (numNonEmptyGeometries == 0) 0.0d else sumSideLength(i) / numNonEmptyGeometries

  /** Returns the average envelope area of non-empty geometries. */
  def averageArea: Double =
    if (numNonEmptyGeometries == 0) 0.0d else sumVolume / numNonEmptyGeometries

  /** Formats the summary in a compact human-readable debug string. */
  override def toString: String = {
    val str = new StringBuilder
    // Write MBR
    str.append("MBR: [(")
    for ($d <- 0 until getCoordinateDimension) {
      if ($d != 0) str.append(", ")
      str.append(getMinCoord($d))
    }
    str.append("), (")
    for ($d <- 0 until getCoordinateDimension) {
      if ($d != 0) str.append(", ")
      str.append(getMaxCoord($d))
    }
    str.append(")]")
    str.append(", size: ")
    str.append(size)
    str.append(", numFeatures: ")
    str.append(numFeatures)
    str.append(", numPoints: ")
    str.append(numPoints)
    str.append(", avgArea: ")
    str.append(averageArea)
    str.append(", avgSideLength: [")
    for ($d <- 0 until getCoordinateDimension) {
      if ($d != 0) str.append(", ")
      str.append(averageSideLength($d))
    }
    str.append("]")
    str.toString
  }

  /**
   * Converts the summary to a [[Row]] with schema that can be used with the Dataframe API.
   * The returned row has the following structure:
   * - extent:
   *   - min_coord: an array of doubles that represent the minimum along each dimension
   *   - max_coord: an array of doubles that represent the maximum along each dimension
   * - size: The total size of all features in bytes as a 64-bit long integer.
   * - num_feature: the total number of features as a 64-bit long integer.
   * - num_non_empty_features: the total number of features with non-empty geometries
   * - num_points: the total number of points in all geometries
   * - avg_area: the average area of the MBR of all features
   * - avg_sidelength: an array of doubles that represent the average side length of the MBRs along each dimension
   * @return a row with schema that contains all the information of the summary.
   */
  def asRow: Row = {
    val mbrSchema = StructType(Seq(
      StructField("min_coord", new ArrayType(DoubleType, false)),
      StructField("max_coord", new ArrayType(DoubleType, false)),
    ))
    val schema = StructType(Seq(
      StructField("extent", mbrSchema),
      StructField("size", LongType),
      StructField("num_features", LongType),
      StructField("num_non_empty_features", LongType),
      StructField("num_points", LongType),
      StructField("avg_area", DoubleType),
      StructField("avg_sidelength", new ArrayType(DoubleType, false)),
    ))

    val minCoord = new Array[Double](getCoordinateDimension)
    val maxCoord = new Array[Double](getCoordinateDimension)
    val avgSideLengthVal = new Array[Double](getCoordinateDimension)
    for ($d <- 0 until getCoordinateDimension) {
      minCoord($d) = getMinCoord($d)
      maxCoord($d) = getMaxCoord($d)
      avgSideLengthVal($d) = averageSideLength($d)
    }
    val mbrValue = new GenericRowWithSchema(Array(minCoord, maxCoord), mbrSchema)

    val values = Array(mbrValue, size, numFeatures, numNonEmptyGeometries, numPoints, averageArea, avgSideLengthVal)
    new GenericRowWithSchema(values, schema)
  }
}

object Summary {
  /**
   * A dictionary that maps each geometry name to a GeometryType instance
   */
  val GeometryNameToType: Map[String, GeometryType] = GeometryType.values.map(gt => gt.typename -> gt).toMap + ("LinearRing" -> GeometryType.LINESTRING)

  /**
   * Writes the summary of the dataset in JSON format
   *
   * @param jsonGenerator the generate to write the output to
   * @param summary       the summary of the dataset
   * @param feature       a sample feature to get the attribute names and types
   * @throws IOException if an error happens while writing the output
   */
  /** Serializes one dataset summary together with one sample feature schema to JSON. */
  @throws[IOException]
  def writeSummaryWithSchema(jsonGenerator: JsonGenerator, summary: Summary, feature: IFeature): Unit = {
    jsonGenerator.writeStartObject()
    // Write MBR
    jsonGenerator.writeFieldName("extent")
    jsonGenerator.writeStartArray()
    jsonGenerator.writeNumber(summary.getMinCoord(0))
    jsonGenerator.writeNumber(summary.getMinCoord(1))
    jsonGenerator.writeNumber(summary.getMaxCoord(0))
    jsonGenerator.writeNumber(summary.getMaxCoord(1))
    jsonGenerator.writeEndArray()
    // Write sizes, number of records, and average area
    jsonGenerator.writeNumberField("size", summary.size)
    jsonGenerator.writeNumberField("num_features", summary.numFeatures)
    jsonGenerator.writeNumberField("num_non_empty_features", summary.numNonEmptyGeometries)
    jsonGenerator.writeNumberField("num_points", summary.numPoints)
    jsonGenerator.writeNumberField("avg_area", summary.averageArea)
    // Write average side length
    jsonGenerator.writeFieldName("avg_sidelength")
    jsonGenerator.writeStartArray()
    jsonGenerator.writeNumber(summary.averageSideLength(0))
    jsonGenerator.writeNumber(summary.averageSideLength(1))
    jsonGenerator.writeEndArray()
    // Write Coordinate Reference System (CRS)
    jsonGenerator.writeFieldName("srid")
    jsonGenerator.writeNumber(feature.getGeometry.getSRID)
    // Write attributes
    if (feature.length > 1) {
      jsonGenerator.writeFieldName("attributes")
      jsonGenerator.writeStartArray()
      for (i <- feature.iNonGeom) {
        jsonGenerator.writeStartObject()
        var fieldName = feature.getName(i)
        if (fieldName == null || fieldName.isEmpty) fieldName = "attribute#" + i
        jsonGenerator.writeStringField("name", fieldName)
        jsonGenerator.writeStringField("type", getTypeAsString(feature.schema(i).dataType))
        jsonGenerator.writeEndObject() // End of attribute type

      }
      jsonGenerator.writeEndArray() // End of attributes

    }
    jsonGenerator.writeEndObject() // End of root object
  }

  /** Maps Spark SQL data types to stable JSON-friendly schema labels. */
  def getTypeAsString(dataType: DataType): String = dataType match {
    case StringType => "string"
    case IntegerType | LongType => "integer"
    case FloatType | DoubleType => "number"
    case BooleanType => "boolean"
    case DateType => "date"
    case MapType(keyType, valueType, _) => s"map(${getTypeAsString(keyType)} -> ${getTypeAsString(valueType)})"
    case ArrayType(elementType, _) => s"list(${getTypeAsString(elementType)})"
    case _ => "unknown"
  }

  /**
   * Compute partial summaries for the given [[SpatialRDD]].
   * It returns a set of Summaries in an RDD that can be combined to produce the final summary.
   * Typically, it returns one summary per partition in the input. If the number of partitions is
   * larger than 1,000, they are further coalesced into 1,000 partitions to reduce memory requirement
   * for a subsequent reduce action.
   * @param features the set of features to summarize
   * @param sizeFunction the function used to estimate the size of each feature
   * @return an RDD of summaries
   */
  private def computePartialSummaries(features: SpatialRDD,
                                      sizeFunction: IFeature => Int = f => f.getStorageSize): RDD[Summary] = {
    var partialSummaries = features.mapPartitionsWithIndex((i, features) => {
      val summary = new Summary
      while (features.hasNext) {
        val feature = features.next()
        if (summary.getCoordinateDimension == 0)
          summary.setCoordinateDimension(GeometryHelper.getCoordinateDimension(feature.getGeometry))
        summary.expandToGeometryWithSize(feature.getGeometry, sizeFunction(feature))
      }
      Option((i, summary)).iterator
    }).values

    // If the number of partitions is very large, Spark would fail because it will try to collect all
    // the partial summaries locally and would run out of memory in such case
    if (partialSummaries.getNumPartitions > 1000)
      partialSummaries = partialSummaries.coalesce(1000)
    partialSummaries
  }

  /**
   * Compute the summary of a set of features with a custom function for estimating the size of a features
   *
   * This is the main exact-summary path used before partition construction. Dataset
   * size is accumulated from the actual loaded features rather than caller-provided
   * estimates, which keeps later load-aware partitioning decisions grounded in
   * real byte counts.
   *
   * @param features the set of features to summarize
   * @param sizeFunction a function that returns the size for each features
   * @return the summary of the input features
   */
  def computeForFeatures(features: SpatialRDD, sizeFunction: IFeature => Int = f => f.getStorageSize) : Summary = {
    if (features.getNumPartitions == 0) {
      new Summary()
    } else if (features.partitions.forall(_.isInstanceOf[SpatialPartition])) {
      // No need to process the data. Just combine the partitions
      features.partitions.map(_.asInstanceOf[SpatialPartition].asSummary)
        .reduce((summary1, summary2) => summary1.expandToSummary(summary2))
    } else {
      val partialSummaries: RDD[Summary] = computePartialSummaries(features, sizeFunction)
      partialSummaries.reduce((mbr1, mbr2) => mbr1.expandToSummary(mbr2))
    }
  }

  /**
   * Compute the MBR, count, and size of a JavaRDD of geometries
   * @param features the JavaRDD of features to compute the summary for
   * @return a Summary for the given features
   */
  def computeForFeatures(features : JavaSpatialRDD) : Summary = computeForFeatures(features.rdd)

  /**
   * Java helper that computes an exact summary with a caller-provided feature-size function.
   *
   * @param features the JavaRDD of features to compute the summary for
   * @return a Summary for the given features
   */
  def computeForFeaturesWithSize(features : JavaSpatialRDD, sizeFunction: IFeature => Int) : Summary =
    computeForFeatures(features.rdd, sizeFunction)

  /**
   * Compute a dataset summary from lightweight per-feature partition descriptors.
   *
   * This method is used after the full feature stream has already been abstracted
   * into [[AbstractFeature]]s for partition construction.
   *
   * @param features the lightweight descriptors of all input features
   * @return the aggregated summary
   */
  def computeForAbstractFeatures(features: Array[AbstractFeature]): Summary = {
    val summary = new Summary()
    if (features != null) {
      features.foreach(summary.expandToAbstractFeature)
    }
    summary
  }

  /**
   * Create a summary accumulator that uses the method [[IFeature#getHeapEstimateBytes]] to accumulate the sizes of
   * the features.
   *
   * @param sc the spark context to register the accumulator to
   * @return the initialized and registered accumulator
   */
  def createSummaryAccumulator(sc: SparkContext) : SummaryAccumulator = createSummaryAccumulator(sc, _.getHeapEstimateBytes)

  /**
   * Create a summary accumulator that uses a caller-provided size function while
   * preserving the same summary aggregation semantics used elsewhere in the project.
   *
   * @param sc the spark context to register the accumulator to
   * @return the initialized and registered accumulator
   */
  def createSummaryAccumulator(sc: SparkContext, sizeFunction: IFeature => Int) : SummaryAccumulator = {
    val accumulator = new SummaryAccumulator(sizeFunction)
    sc.register(accumulator, "SummaryAccumulator")
    accumulator
  }

  /**Driver-side cache of summaries for RDDs to avoid recomputing the summary multiple times*/
  val cachedSummaries: collection.mutable.Map[Int, Summary] = new LRUCache[Int, Summary](100)

  /**
   * Retrieves a cached summary for the RDD or computes and caches it on first access.
   *
   * This is mainly used by higher-level operator flows that may need the same exact
   * dataset summary more than once during one run.
   *
   * @param rdd the RDD to compute the summary for
   * @return either a cached summary or a newly computed summary
   */
  def getOrCompute(rdd: SpatialRDD): Summary = cachedSummaries.getOrElseUpdate(rdd.id, {computeForFeatures(rdd)})
}
