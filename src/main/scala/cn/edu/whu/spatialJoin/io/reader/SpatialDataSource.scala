package cn.edu.whu.spatialJoin.io.reader

import cn.edu.whu.spatialJoin.algortithms.SpatialDataTypes.{JavaSpatialRDD, SpatialRDD}
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions
import cn.edu.whu.spatialJoin.geom.IFeature
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}

import scala.collection.JavaConverters._

/**
 * Unified entry point for spatial data access.
 *
 * This object deliberately hides the old Operation/CLI discovery layer and exposes
 * one stable API for the rest of the project: given a path, an optional format,
 * and [[SpatialJoinOptions]], it resolves the reader and returns either local
 * iterators or distributed RDDs of [[IFeature]].
 */
object SpatialDataSource {

  private val AutoFormat = "*auto*"

  /**
   * Builds a read-scoped options object with a normalized input-format entry.
   *
   * This helper is shared by local reads, distributed reads, and reader resolution
   * so all of them interpret the format string in exactly the same way.
   */
  private def buildReadOptions(format: String, opts: SpatialJoinOptions): SpatialJoinOptions = {
    val normalized = if (opts == null) new SpatialJoinOptions() else new SpatialJoinOptions(opts)
    val resolvedFormat = if (format == null || format.trim.isEmpty) AutoFormat else format.trim
    normalized.set(SpatialFileRDD.InputFormat, resolvedFormat)
  }

  /**
   * Resolves the concrete reader class for the given input path and format hint.
   *
   * Callers typically use this before a debug run so they can verify which reader
   * the registry selected before any actual records are read.
   */
  def resolveReaderClass(path: String, format: String, opts: SpatialJoinOptions = null): Class[_ <: FeatureReader] =
    SpatialFileRDD.resolveFeatureReaderClass(path, buildReadOptions(format, opts))

  /** Typed-format overload of [[resolveReaderClass]]. */
  def resolveReaderClass(path: String, format: SpatialInputFormat, opts: SpatialJoinOptions): Class[_ <: FeatureReader] =
    resolveReaderClass(path, if (format == null) AutoFormat else format.formatName(), opts)

  /** Backward-compatible alias kept for older call sites. */
  def getReaderClass(path: String, format: String, opts: SpatialJoinOptions = null): Class[_ <: FeatureReader] =
    resolveReaderClass(path, format, opts)

  /** Backward-compatible typed-format alias. */
  def getReaderClass(path: String, format: SpatialInputFormat, opts: SpatialJoinOptions): Class[_ <: FeatureReader] =
    resolveReaderClass(path, format, opts)

  /** Reads spatial features as a distributed [[SpatialRDD]]. */
  def read(sc: SparkContext, path: String, format: String = AutoFormat, opts: SpatialJoinOptions = null): SpatialRDD =
    new SpatialFileRDD(sc, path, buildReadOptions(format, opts))

  /** Typed-format overload of the distributed read entry. */
  def read(sc: SparkContext, path: String, format: SpatialInputFormat, opts: SpatialJoinOptions): SpatialRDD =
    read(sc, path, if (format == null) AutoFormat else format.formatName(), opts)

  /** Java-friendly shortcut for distributed reads. */
  def readJ(sc: JavaSparkContext,
            path: String,
            format: String = AutoFormat,
            opts: SpatialJoinOptions = null): JavaSpatialRDD =
    JavaRDD.fromRDD(read(sc.sc, path, format, opts))

  def readJ(sc: JavaSparkContext,
            path: String,
            format: SpatialInputFormat,
            opts: SpatialJoinOptions): JavaSpatialRDD =
    readJ(sc, path, if (format == null) AutoFormat else format.formatName(), opts)

  /** Reads spatial features locally without creating a Spark job. */
  def readLocal(path: String,
                format: String = AutoFormat,
                opts: SpatialJoinOptions = null,
                conf: Configuration = new Configuration()): Iterator[IFeature] =
    SpatialFileRDD.readPathLocally(path, if (format == null || format.trim.isEmpty) AutoFormat else format.trim, opts, conf)

  /** Typed-format overload of the local read entry. */
  def readLocal(path: String,
                format: SpatialInputFormat,
                opts: SpatialJoinOptions,
                conf: Configuration): Iterator[IFeature] =
    readLocal(path, if (format == null) AutoFormat else format.formatName(), opts, conf)

  /** Java-friendly shortcut for local reads. */
  def readLocalJ(path: String,
                 format: String = AutoFormat,
                 opts: SpatialJoinOptions = null,
                 conf: Configuration = new Configuration()): java.util.Iterator[IFeature] =
    readLocal(path, format, opts, conf).asJava

  def readLocalJ(path: String,
                 format: SpatialInputFormat,
                 opts: SpatialJoinOptions,
                 conf: Configuration): java.util.Iterator[IFeature] =
    readLocal(path, format, opts, conf).asJava
}
