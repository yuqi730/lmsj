package cn.edu.whu.spatialJoin.common

import com.fasterxml.jackson.core.JsonFactory
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions.{defaultHadoopConf, defaultSparkConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils

import java.io._
import java.lang.reflect.InvocationTargetException
import java.util.Properties
import scala.language.implicitConversions

class SpatialJoinOptions(loadDefaults: Boolean = true)
  extends scala.collection.mutable.HashMap[String, String] with Serializable {

  if (loadDefaults)
    this.mergeWith(SpatialJoinOptions.defaultOptions)

  def this(options: SpatialJoinOptions) {
    this(false)
    for (entry <- options)
      this.put(entry._1, entry._2)
  }

  def this() {
    this(true)
  }

  def this(conf: Configuration) {
    this(false)
    val confIter = conf.iterator()
    while (confIter.hasNext) {
      val entry = confIter.next()
      this.put(entry.getKey, entry.getValue)
    }
  }

  def set(key: String, value: Any): SpatialJoinOptions = {
    this.put(key, value.toString)
    this
  }

  def getString(key: String, defaultValue: String): String = super.getOrElse(key, defaultValue)

  def getString(key: String): String = this.getString(key, null)

  def getInt(key: String, defaultValue: Int): Int = super.getOrElse(key, defaultValue.toString).toInt

  def setInt(key: String, value: Int): SpatialJoinOptions = this.set(key, value.toString)

  def getLong(key: String, defaultValue: Long): Long = super.getOrElse(key, defaultValue.toString).toLong

  def setLong(key: String, value: Long): SpatialJoinOptions = this.set(key, value.toString)

  def getDouble(key: String, defaultValue: Double): Double = super.getOrElse(key, defaultValue.toString).toDouble

  def getFloat(key: String, defaultValue: Float): Float = super.getOrElse(key, defaultValue.toString).toFloat

  def getBoolean(key: String, defaultValue: Boolean): Boolean = super.getOrElse(key, defaultValue.toString).toBoolean

  def setBoolean(key: String, value: Boolean): SpatialJoinOptions = this.set(key, value.toString)

  def getSizeAsBytes(key: String, defaultValue: String): Long =
    JavaUtils.byteStringAsBytes(super.getOrElse(key, defaultValue))

  def getSizeAsBytes(key: String, defaultValue: Long): Long =
    if (contains(key)) JavaUtils.byteStringAsBytes(super.get(key).get) else defaultValue

  def retainIndex(index: Int): SpatialJoinOptions = {
    val suffix = s"[$index]"
    val newOpts = new SpatialJoinOptions(false)
    this.foreach(kv => {
      if (kv._1.indexOf('[') == -1)
        newOpts.put(kv._1, kv._2)
      else if (kv._1.endsWith(suffix))
        newOpts.put(kv._1.replace(suffix, ""), kv._2)
    })
    newOpts
  }

  def storeToTextFile(fs: FileSystem, p: Path): Unit = {
    val prop = new Properties
    this.foreach(kv => prop.put(kv._1, kv._2))
    val out = fs.create(p)
    val writer = new OutputStreamWriter(out)
    try prop.store(writer, "SpatialJoin options")
    finally writer.close()
  }

  def loadFromTextFile(fs: FileSystem, path: Path): SpatialJoinOptions = {
    val in = fs.open(path)
    try {
      loadFromTextFile(in)
    } finally in.close()
  }

  def loadFromTextFile(in: InputStream): SpatialJoinOptions = {
    val reader = new InputStreamReader(in)
    val properties = new Properties
    properties.load(reader)
    val entries = properties.entrySet().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      this.put(entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[String])
    }
    this
  }

  def getEnumIgnoreCase[T <: Enum[T]](name: String, defaultValue: T): T = {
    val confValue: Option[String] = this.get(name)
    if (confValue.isEmpty)
      return defaultValue
    try {
      val valuesMethod = defaultValue.getDeclaringClass.getMethod("values")
      val values = valuesMethod.invoke(null).asInstanceOf[Array[T]]
      for (value <- values) {
        if (value.toString.equalsIgnoreCase(confValue.get))
          return value
      }
    } catch {
      case e @ (_: NoSuchMethodException | _: IllegalAccessException | _: InvocationTargetException) =>
        e.printStackTrace()
    }
    defaultValue
  }

  override def toString: String = {
    val str = new StringBuffer
    for (entry <- this) {
      if (entry._2 == "true") str.append("-" + entry._1)
      else if (entry._2 == "false") str.append("-no-" + entry._1)
      else {
        str.append(entry._1)
        str.append(":")
        str.append(entry._2)
      }
      str.append(" ")
    }
    str.toString
  }

  def toJson: String = {
    val jsonData = new ByteArrayOutputStream()
    val jsonGenerator = new JsonFactory().createGenerator(jsonData)
    jsonGenerator.writeStartObject()
    for (v <- this)
      jsonGenerator.writeStringField(v._1, v._2)
    jsonGenerator.writeEndObject()
    jsonGenerator.close()
    new String(jsonData.toByteArray)
  }

  def getValues(key: String): Array[String] = {
    var values = Seq[String]()
    for (p <- this) {
      if (p._1 == key || (p._1.startsWith(key) && p._1.indexOf('[') == key.length))
        values = values :+ p._2
    }
    values.toArray
  }

  def loadIntoHadoopConf(conf: Configuration = null): Configuration = {
    val hadoopConf: Configuration = if (conf == null) {
      val newConf = new Configuration(false)
      val defaultIter = defaultHadoopConf.iterator()
      while (defaultIter.hasNext) {
        val entry = defaultIter.next()
        newConf.set(entry.getKey, entry.getValue)
      }
      newConf
    } else {
      conf
    }
    for (entry <- this)
      hadoopConf.set(entry._1, entry._2)
    hadoopConf
  }

  def loadIntoSparkConf(conf: SparkConf = null): SparkConf = {
    val sparkConf: SparkConf = if (conf == null) {
      val newConf = new SparkConf(false)
      for (entry <- defaultSparkConf.getAll)
        newConf.set(entry._1, entry._2)
      newConf
    } else conf
    for ((k, v) <- this)
      sparkConf.set(k, v)
    sparkConf
  }

  def mergeWith(opts: SpatialJoinOptions): SpatialJoinOptions = {
    for ((k, v) <- opts)
      this.set(k, v)
    this
  }

  def getClass[U](name: String, defaultValue: Class[_ <: U], xface: Class[U]): Class[_ <: U] = try {
    val className = get(name)
    if (className.isEmpty)
      return defaultValue
    val theClass = Class.forName(className.get)
    if (theClass != null && !xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass + " not " + xface.getName)
    else if (theClass != null)
      theClass.asSubclass(xface)
    else
      null
  } catch {
    case e: Exception =>
      throw new RuntimeException(e)
  }

  def setClass(name: String, theClass: Class[_], xface: Class[_]): Unit = {
    if (!xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass + " not " + xface.getName)
    set(name, theClass.getName)
  }
}

object SpatialJoinOptions {
  implicit def fromPairs(map: Iterable[(String, Any)]): SpatialJoinOptions = {
    val options = new SpatialJoinOptions()
    for (entry <- map)
      options.put(entry._1, entry._2.toString)
    options
  }

  implicit def fromPair(entry: (String, Any)): SpatialJoinOptions = new SpatialJoinOptions().set(entry._1, entry._2.toString)

  implicit def fromMap(map: java.util.Map[String, String]): SpatialJoinOptions = {
    val options = new SpatialJoinOptions()
    val entries = map.entrySet().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      options.put(entry.getKey, entry.getValue)
    }
    options
  }

  def fromStrings(args: Iterable[String]): SpatialJoinOptions = {
    val options = new SpatialJoinOptions()
    val optionName = "((\\w+)(\\[\\d+\\])?)"
    val booleanTrueRegex = raw"-$optionName".r
    val booleanFalseRegex = raw"-no-$optionName".r
    val optionValue = raw"${optionName}:(.*)".r
    args.foreach {
      case booleanTrueRegex(nameNumber, name, number) => options.set(nameNumber, true)
      case booleanFalseRegex(nameNumber, name, number) => options.set(nameNumber, false)
      case optionValue(nameNumber, name, number, argvalue) => options.set(nameNumber, argvalue)
      case _ =>
    }
    options
  }

  def fromStringArray(args: Array[String]): SpatialJoinOptions = fromStrings(args.toIterable)

  implicit def fromSparkConf(sparkConf: SparkConf): SpatialJoinOptions = fromPairs(sparkConf.getAll)

  lazy val defaultOptions: SpatialJoinOptions = {
    val options = new SpatialJoinOptions(false)
    for (entry <- SpatialJoinConstants.DefaultOptions.ENTRIES)
      options.put(entry(0), entry(1))
    options
  }

  lazy val defaultHadoopConf: Configuration = new Configuration(false)

  lazy val defaultSparkConf: SparkConf = {
    val sparkConf = new SparkConf(false)
    for (entry <- SpatialJoinConstants.DefaultOptions.ENTRIES) {
      if (entry(0).startsWith("spark."))
        sparkConf.set(entry(0), entry(1))
    }
    sparkConf
  }
}
