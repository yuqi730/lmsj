/*
 * Copyright 2020 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.whu.spatialJoin.geom

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants
import cn.edu.whu.spatialJoin.geom.util.{GeometryHelper, GeometryMetadataReader}
import cn.edu.whu.spatialJoin.sql.GeometryDataType
import cn.edu.whu.spatialJoin.synopses.SizeFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

import java.util
import scala.collection.GenTraversableOnce

/**
 * An interface to a geometric feature (geometry + other attributes)
 */
trait IFeature extends Serializable with Row {

  /**
   * The index of the geometry field
   */
  lazy val iGeom: Int = schema.indexWhere(_.dataType == GeometryDataType)

  /**
   * A traversable sequence of indices of all non-geometry attributes, i.e., [0, length[ - {[[iGeom]]}
   */
  lazy val iNonGeom: GenTraversableOnce[Int] =
    if (iGeom == 0) {
      1 until length
    } else {
      (0 until iGeom) ++ ((iGeom + 1) until length)
    }

  /**
   * An iterable over non geometry attributes indexes for Java.
   * Can be used with simplified Java loops such as `for (int i : feature.iNonGeomJ) { ... }`
   */
  lazy val iNonGeomJ: java.lang.Iterable[java.lang.Integer] = () => new java.util.Iterator[java.lang.Integer] {
    var i: Int = -1
    val lastI: Int = if (iGeom == length - 1) length - 2 else length - 1

    override def hasNext: Boolean = i < lastI

    override def next(): Integer = {
      i += 1
      if (i == iGeom)
        i += 1
      i
    }
  }

  /**
   * The geometry contained in the feature.
   *
   * @return the geometry in this attribute
   */
  def getGeometry: Geometry = if (iGeom == -1) EmptyGeometry.instance else { get(iGeom).asInstanceOf[Geometry] }

  /**
   * Returns the serialized geometry payload when available.
   *
   * <p>This method lets lightweight planning code inspect geometry metadata
   * directly from binary storage without forcing a full JTS geometry object to
   * be materialized first.
   */
  def getGeometryBytes: Array[Byte] = {
    if (iGeom == -1 || isNullAt(iGeom))
      null
    else {
      get(iGeom) match {
        case bytes: Array[Byte] => bytes
        case geometry: Geometry => GeometryDataType.serialize(geometry)
        case _ =>
          val geometry = getGeometry
          if (geometry == null) null else GeometryDataType.serialize(geometry)
      }
    }
  }

  /**
   * Return the SparkSQL data type of the given attribute.
   * @param i the index of the attribute in the range [0, length[
   * @return the type of the attribute or null if unknown
   */
  def getDataType(i: Int): DataType = schema(i).dataType

  /**
   * Return the name of the given attribute.
   * @param i the index of the attribute in the range [0, length[
   * @return the type of the attribute or null if unknown
   */
  def getName(i: Int): String = schema(i).name

  /**
   * If names are associated with attributes, this function returns the name of the attribute at the given position
   * (0-based).
   *
   * @param i the index of the attribute to return its name
   * @return the name of the given attribute index or `null` if it does not exist
   */
  def getAttributeName(i: Int): String = schema(i + 1).name

  /**
   * The estimated total size of the feature in bytes including the geometry and features
   *
   * @return the storage size in bytes
   */
  def getStorageSize: Int = {
    var size: Int = 0
    for (i <- 0 until length) {
      if (!isNullAt(i))
        size += IFeature.getStorageSize(get(i), schema(i).dataType)
    }
    size
  }

  /**
   * Returns a JVM-heap-oriented size estimate for this feature.
   *
   * <p>This estimate is used by the current partitioning path because the final
   * join execution keeps the serialized geometry bytes around and also
   * materializes JTS geometries lazily during computation, so memory pressure is
   * usually higher than the plain serialized size.
   */
  def getHeapEstimateBytes: Int = {
    var size: Long = IFeature.baseFeatureHeapOverhead(length)
    for (i <- 0 until length) {
      if (!isNullAt(i))
        size += IFeature.getHeapEstimateSize(get(i), schema(i).dataType)
    }
    IFeature.safeToInt(size)
  }

  /**
   * Returns the estimated heap bytes when the feature stays in its binary-geometry representation.
   *
   * <p>This estimate reflects the current pre-computation state used by the project:
   * feature objects plus serialized geometry byte arrays remain in memory, but JTS
   * geometry objects are not materialized yet.
   */
  def getBinaryHeapEstimateBytes: Int = {
    var size: Long = IFeature.baseFeatureHeapOverhead(length)
    for (i <- 0 until length) {
      if (!isNullAt(i)) {
        size += (schema(i).dataType match {
          case GeometryDataType =>
            IFeature.estimateRetainedBinaryGeometryHeap(getGeometryBytes)
          case _ =>
            IFeature.getHeapEstimateSize(get(i), schema(i).dataType)
        })
      }
    }
    IFeature.safeToInt(size)
  }

  /**
   * Returns the estimated heap bytes after geometry values are materialized as JTS objects.
   *
   * <p>This estimate is useful for the later join phase where the target partition's
   * features are deserialized and kept as actual geometries for predicate evaluation.
   */
  def getMaterializedGeometryHeapEstimateBytes: Int = {
    var size: Long = IFeature.baseFeatureHeapOverhead(length)
    for (i <- 0 until length) {
      if (!isNullAt(i)) {
        size += (schema(i).dataType match {
          case GeometryDataType =>
            val bytes = getGeometryBytes
            if (bytes != null)
              IFeature.estimateMaterializedGeometryHeapFromBinary(bytes)
            else
              IFeature.estimateMaterializedGeometryHeap(getGeometry)
          case _ =>
            IFeature.getHeapEstimateSize(get(i), schema(i).dataType)
        })
      }
    }
    IFeature.safeToInt(size)
  }

  /**
   * Returns the execution-side load estimate selected by the current planning mode.
   *
   * <p>The default conservative mode counts both retained binary geometry bytes and the later
   * materialized runtime geometry contribution. The looser materialized-geometry mode keeps only
   * the runtime geometry contribution and is intended for experiments that prefer a less
   * conservative job-packing budget.
   */
  def getExecutionLoadEstimateBytes(mode: String): Int = {
    val normalizedMode =
      if (mode == null) SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE
      else mode.trim.toLowerCase
    normalizedMode match {
      case SpatialJoinConstants.Execution.LOAD_ESTIMATE_MODE_MATERIALIZED_GEOMETRY =>
        getMaterializedGeometryHeapEstimateBytes
      case _ =>
        getHeapEstimateBytes
    }
  }

  /**
   * Returns the planning-oriented computation-complexity estimate of this feature.
   *
   * <p>The method prefers serialized geometry metadata so partition planning and
   * later job grouping can stay on the binary geometry path. It falls back to the
   * exact geometry-based complexity only when serialized bytes are unavailable.
   */
  def getComplexityEstimate: Double =
    getComplexityEstimate(null)

  /**
   * Returns the planning-oriented computation-complexity estimate of this feature using the requested mode.
   *
   * Supported modes are `fast`, `balanced`, and `exact-binary`. The no-argument
   * overload keeps the project's current default.
   */
  def getComplexityEstimate(mode: String): Double = {
    val geometryBytes = getGeometryBytes
    if (geometryBytes == null) {
      SizeFunctions.computeActualComplexity(this)
    } else {
      SizeFunctions.estimateComplexity(geometryBytes, mode)
    }
  }

  override def toString(): String = IFeature.toString(this)
}

object IFeature {
  private val ObjectAlignmentBytes = 8L
  private val ObjectHeaderBytes = 16L
  private val ArrayHeaderBytes = 24L
  private val ReferenceBytes = 8L
  private val StringHeaderBytes = 40L
  private val MapHeaderBytes = 48L
  private val MapEntryBytes = 32L
  private val FeatureHeaderBytes = 48L

  def getStorageSize(value: Any, dataType: DataType): Int = dataType match {
    case StringType => value.asInstanceOf[String].length
    case IntegerType => 4
    case LongType | DoubleType | TimestampType => 8
    case BooleanType => 1
    case GeometryDataType if value.isInstanceOf[Array[Byte]] => value.asInstanceOf[Array[Byte]].length
    case GeometryDataType => GeometryHelper.getGeometryStorageSize(value.asInstanceOf[Geometry])
    case ArrayType(k, true) =>
      val list = value.asInstanceOf[Seq[Any]]
      if (list.isEmpty)
        0
      else
        list.size * getStorageSize(list.head, k)
    case MapType(keyType, valueType, true) =>
      val map = value.asInstanceOf[Map[Any, Any]]
      map.size * (getStorageSize(map.keys.head, keyType) + getStorageSize(map.values.head, valueType))
  }

  def getHeapEstimateSize(value: Any, dataType: DataType): Int =
    if (value == null) {
      0
    } else dataType match {
      case StringType =>
        alignHeapBytes(StringHeaderBytes + value.asInstanceOf[String].length.toLong * 2L)
      case ByteType | ShortType | IntegerType | FloatType | BooleanType =>
        alignHeapBytes(ObjectHeaderBytes)
      case LongType | DoubleType | TimestampType | DateType =>
        alignHeapBytes(ObjectHeaderBytes + 8L)
      case BinaryType =>
        estimateByteArrayHeap(value.asInstanceOf[Array[Byte]])
      case GeometryDataType if value.isInstanceOf[Array[Byte]] =>
        GeometryMetadataReader.estimateHeapBytes(value.asInstanceOf[Array[Byte]])
      case GeometryDataType =>
        estimateMaterializedGeometryHeap(value.asInstanceOf[Geometry])
      case ArrayType(elementType, _) =>
        val list = value.asInstanceOf[Seq[Any]]
        val elementBytes = list.foldLeft(0L)((total, item) => total + getHeapEstimateSize(item, elementType))
        alignHeapBytes(ArrayHeaderBytes + list.size.toLong * ReferenceBytes + elementBytes)
      case MapType(keyType, valueType, _) =>
        val map = value.asInstanceOf[Map[Any, Any]]
        val entryBytes = map.foldLeft(0L) { case (total, (key, mapValue)) =>
          total + MapEntryBytes + getHeapEstimateSize(key, keyType) + getHeapEstimateSize(mapValue, valueType)
        }
        alignHeapBytes(MapHeaderBytes + entryBytes)
      case _ =>
        alignHeapBytes(Math.max(8L, getStorageSize(value, dataType).toLong))
    }

  def baseFeatureHeapOverhead(attributeCount: Int): Int =
    alignHeapBytes(FeatureHeaderBytes + ArrayHeaderBytes + Math.max(0L, attributeCount.toLong) * ReferenceBytes)

  private def estimateByteArrayHeap(bytes: Array[Byte]): Int =
    if (bytes == null) 0 else alignHeapBytes(ArrayHeaderBytes + bytes.length.toLong)

  def estimateRetainedBinaryGeometryHeap(bytes: Array[Byte]): Int =
    GeometryMetadataReader.estimateRetainedBinaryHeapBytes(bytes)

  def estimateMaterializedGeometryHeapFromBinary(bytes: Array[Byte]): Int =
    GeometryMetadataReader.estimateMaterializedGeometryHeapBytes(bytes)

  def estimateMaterializedGeometryHeap(geometry: Geometry): Int =
    if (geometry == null) 0
    else {
      val geometryBytes = Math.max(0L, GeometryHelper.getGeometryStorageSize(geometry).toLong)
      alignHeapBytes(96L + geometryBytes * 2L)
    }

  private def alignHeapBytes(value: Long): Int = safeToInt {
    if (value <= 0L) 0L
    else {
      val remainder = value % ObjectAlignmentBytes
      if (remainder == 0L) value else value + (ObjectAlignmentBytes - remainder)
    }
  }

  private[geom] def safeToInt(value: Long): Int =
    if (value <= 0L) 0 else Math.min(Int.MaxValue.toLong, value).toInt

  def toString(feature: IFeature): String = {
    val b: StringBuilder = new StringBuilder
    for ($i <- 0 until feature.length) {
      if ($i != 0)
        b.append(';')
      val value: Any = feature.get($i)
      b.append(value)
    }
    b.toString()
  }

  def equals(f1: IFeature, f2: IFeature): Boolean = {
    if (f1.length != f2.length) return false
    for (iAttr <- 0 until f1.length) {
      val name1 = f1.getName(iAttr)
      val name2 = f2.getName(iAttr)
      val namesEqual = ((name1 == null || name1.isEmpty) && (name2 == null || name2.isEmpty)) || (name1 != null && name1 == name2)
      if (!namesEqual) return false
      if (!(f1.get(iAttr) == f2.get(iAttr))) return false
    }
    true
  }

}
