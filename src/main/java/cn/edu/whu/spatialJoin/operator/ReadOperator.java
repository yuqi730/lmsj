package cn.edu.whu.spatialJoin.operator;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.geom.IFeature;
import cn.edu.whu.spatialJoin.io.reader.FeatureReader;
import cn.edu.whu.spatialJoin.io.reader.SpatialDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;

/**
 * Read-focused operator wrapper.
 *
 * <p>This class normalizes the input path/format pair and exposes both local and Spark-backed
 * read paths on top of the unified {@link SpatialDataSource} entry.
 */
public final class ReadOperator implements Serializable {

  private static final long serialVersionUID = 1L;

  private final SpatialJoinOptions options;

  ReadOperator(SpatialJoinOptions options) {
    this.options = new SpatialJoinOptions(options);
  }

  /** Returns a defensive copy of the normalized read options. */
  public SpatialJoinOptions getOptions() {
    return new SpatialJoinOptions(options);
  }

  /** Returns the input path that this read operator will load. */
  public String getInputPath() {
    String path = options.getString(SpatialJoinConstants.Operator.INPUT_PATH);
    if (path == null || path.trim().isEmpty()) {
      throw new IllegalStateException("Missing input path option: " + SpatialJoinConstants.Operator.INPUT_PATH);
    }
    return path;
  }

  /** Returns the normalized input-format string, defaulting to auto-detection. */
  public String getInputFormat() {
    String format = options.getString(SpatialJoinConstants.Operator.INPUT_FORMAT);
    return format == null || format.trim().isEmpty() ? "*auto*" : format.trim();
  }

  /**
   * Resolves the concrete reader that will be used for the configured input.
   *
   * @return feature reader class
   */
  public Class<? extends FeatureReader> resolveReaderClass() {
    return SpatialDataSource.resolveReaderClass(getInputPath(), getInputFormat(), getOptions());
  }

  /**
   * Reads the configured input locally without launching a Spark job.
   *
   * This method is mainly used by unit tests, debug applications, and small-file
   * validation workflows that want to inspect a few features directly.
   */
  public java.util.Iterator<IFeature> readLocal(Configuration conf) {
    Configuration effectiveConf = conf == null ? new Configuration() : conf;
    return SpatialDataSource.readLocalJ(getInputPath(), getInputFormat(), getOptions(), effectiveConf);
  }

  /**
   * Java-friendly alias for {@link #readLocal(Configuration)}.
   *
   * @param conf optional Hadoop configuration
   * @return iterator over local features
   */
  public java.util.Iterator<IFeature> readLocalJ(Configuration conf) {
    return readLocal(conf);
  }

  /**
   * Reads the configured input as a Spark RDD.
   *
   * @param sparkContext active Spark context
   * @return distributed feature RDD
   */
  public RDD<IFeature> read(SparkContext sparkContext) {
    return SpatialDataSource.read(sparkContext, getInputPath(), getInputFormat(), getOptions());
  }

  public JavaRDD<IFeature> readJ(JavaSparkContext sparkContext) {
    return SpatialDataSource.readJ(sparkContext, getInputPath(), getInputFormat(), getOptions());
  }

  /** Backward-compatible alias kept for older call sites. */
  public Class<? extends FeatureReader> getReaderClass() {
    return resolveReaderClass();
  }
}
