package cn.edu.whu.spatialJoin.sql

import cn.edu.whu.spatialJoin.geom.util.{GeometryReader, GeometryWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, UserDefinedType}
import org.locationtech.jts.geom.Geometry

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

/**
 * Spark SQL geometry UDT used by this project.
 *
 * The logical type is still `GeometryDataType`, while the physical SQL storage
 * type is `BinaryType`. In other words:
 * - storage / row transfer: `Array[Byte]`
 * - computation after deserialization: JTS `Geometry`
 */
class GeometryDataType private() extends UserDefinedType[Geometry] {
  override def sqlType: DataType = BinaryType

  override def serialize(geometry: Geometry): Array[Byte] = {
    if (geometry == null) return null

    val baos = new ByteArrayOutputStream()
    val dataoutput = new DataOutputStream(baos)
    try {
      GeometryDataType.geometryWriter.write(geometry, dataoutput, true)
      dataoutput.flush()
      baos.toByteArray
    } finally {
      dataoutput.close()
    }
  }

  override def deserialize(datum: Any): Geometry = {
    if (datum == null) return null

    val bytes = datum match {
      case b: Array[Byte] => b
      case other => throw new IllegalArgumentException(s"Unsupported datum type: ${other.getClass}")
    }

    val datainput = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      GeometryDataType.geometryReader.parse(datainput)
    } finally {
      datainput.close()
    }
  }

  override def userClass: Class[Geometry] = classOf[Geometry]

  override def typeName: String = "geometry"

  override def toString: String = "GeometryType"

  override def hashCode(): Int = classOf[GeometryDataType].hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[GeometryDataType] || other == GeometryDataType
}

case object GeometryDataType extends GeometryDataType {
  @transient var _geometryWriter: GeometryWriter = _
  def geometryWriter: GeometryWriter = {
    if (_geometryWriter == null)
      _geometryWriter = new GeometryWriter
    _geometryWriter
  }

  @transient var _geometryReader: GeometryReader = _
  def geometryReader: GeometryReader = {
    if (_geometryReader == null)
      _geometryReader = new GeometryReader(GeometryReader.DefaultGeometryFactory)
    _geometryReader
  }

  def geometryType: DataType = GeometryDataType

  /**
   * Reads a geometry from Spark's internal binary row representation and
   * deserializes it into a JTS Geometry object for computation.
   */
  def getGeometryFromRow(record: InternalRow, i: Int): Geometry =
    if (record.isNullAt(i)) null else deserialize(record.getBinary(i))

  /**
   * Deserializes an already extracted binary geometry payload into a JTS Geometry.
   */
  def getGeometryFromArray(bytes: Array[Byte]): Geometry = deserialize(bytes)

  /**
   * Serializes a JTS Geometry into the binary form used in Spark internal rows.
   */
  def setGeometryInRow(geometry: Geometry): Array[Byte] = serialize(geometry)
}
