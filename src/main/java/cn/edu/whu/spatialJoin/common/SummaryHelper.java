package cn.edu.whu.spatialJoin.common;

import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeature;
import cn.edu.whu.spatialJoin.enums.GeometryType;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.IFeature;
import cn.edu.whu.spatialJoin.geom.util.GeometryMetadataReader;
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.function.ToIntFunction;

/**
 * Java-friendly helpers for dataset summaries and summary-derived options.
 *
 * <p>The current partitioning path computes dataset size from the actual loaded
 * features by using {@link IFeature#getHeapEstimateBytes()} together with the
 * same summary aggregation semantics used elsewhere in the codebase. This keeps
 * the dataset-level byte size closer to the JVM heap pressure expected during
 * later in-memory join execution rather than treating it as a caller-provided
 * estimate.
 */
  public final class SummaryHelper implements Serializable {

  private static final long serialVersionUID = 1L;

  @FunctionalInterface
  /** Java-serializable feature-size function used during distributed summary computation. */
  public interface FeatureSizeFunction extends ToIntFunction<IFeature>, Serializable {
  }

  /** Utility class; use static helper methods only. */
  private SummaryHelper() {
  }

  /**
   * Computes a distributed summary from the loaded feature RDD using each
   * feature's heap-oriented size estimate.
   *
   * @param features distributed features
   * @return aggregated dataset summary
   */
  public static Summary computeFor(JavaRDD<IFeature> features) {
    return computeFor(features, SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  /**
   * Computes a distributed summary from the loaded feature RDD using the configured execution-side
   * feature-load estimate mode.
   */
  public static Summary computeFor(JavaRDD<IFeature> features, String loadEstimateMode) {
    return computeFor(features, (FeatureSizeFunction) feature ->
        feature == null ? 0 : feature.getExecutionLoadEstimateBytes(loadEstimateMode));
  }

  /**
   * Computes a distributed summary using the feature heap estimate after geometry values are
   * considered materialized as runtime objects, without additionally counting the retained binary
   * geometry payload in the size term.
   *
   * <p>This is intended for diagnostics that want a looser "loaded geometry" footprint rather
   * than the more conservative execution-pressure estimate used by partition planning.
   */
  public static Summary computeForMaterializedGeometry(JavaRDD<IFeature> features) {
    return computeFor(features, (FeatureSizeFunction) IFeature::getMaterializedGeometryHeapEstimateBytes);
  }

  /**
   * Computes a distributed summary directly from serialized geometry metadata whenever possible.
   *
   * <p>This path avoids eagerly materializing JTS geometries while still producing the
   * envelope/count/byte statistics needed by large partition-analysis runs.
   *
   * @param features distributed features
   * @return aggregated dataset summary using the binary-metadata path
   */
  public static Summary computeForBinaryMetadata(JavaRDD<IFeature> features) {
    return computeForBinaryMetadata(features, SpatialJoinConstants.Execution.DEFAULT_PARTITION_LOAD_ESTIMATE_MODE);
  }

  /**
   * Computes a distributed summary directly from serialized geometry metadata whenever possible
   * while using the configured execution-side feature-load estimate mode.
   */
  public static Summary computeForBinaryMetadata(JavaRDD<IFeature> features, String loadEstimateMode) {
    if (features == null) {
      return new Summary();
    }
    return features.aggregate(
        new Summary(),
        new Function2<Summary, IFeature, Summary>() {
          @Override
          public Summary call(Summary summary, IFeature feature) {
            accumulateFeatureSummaryBinary(summary, feature, loadEstimateMode);
            return summary;
          }
        },
        new Function2<Summary, Summary, Summary>() {
          @Override
          public Summary call(Summary left, Summary right) {
            if (left.getCoordinateDimension() == 0 && !right.isEmpty()) {
              left.setCoordinateDimension(right.getCoordinateDimension());
            }
            left.expandToSummary(right);
            return left;
          }
        });
  }

  /**
   * Computes the exact dataset summary from disk-backed lightweight descriptors.
   *
   * <p>Each descriptor represents one source object, so this preserves the same feature count,
   * envelope, and selected load estimate as the binary-metadata summary path without decoding the
   * original feature again.
   */
  public static Summary computeForAbstractFeatures(JavaRDD<AbstractFeature> features) {
    if (features == null) {
      return new Summary();
    }
    return features.aggregate(
        new Summary(),
        new Function2<Summary, AbstractFeature, Summary>() {
          @Override
          public Summary call(Summary summary, AbstractFeature feature) {
            if (summary != null && feature != null && !feature.isEmpty()) {
              summary.expandToAbstractFeature(feature);
            }
            return summary;
          }
        },
        new Function2<Summary, Summary, Summary>() {
          @Override
          public Summary call(Summary left, Summary right) {
            if (left.getCoordinateDimension() == 0 && !right.isEmpty()) {
              left.setCoordinateDimension(right.getCoordinateDimension());
            }
            left.expandToSummary(right);
            return left;
          }
        });
  }

  /** Computes a distributed summary using serialized/storage byte estimates. */
  public static Summary computeForStorage(JavaRDD<IFeature> features) {
    return computeFor(features, (FeatureSizeFunction) IFeature::getStorageSize);
  }

  /**
   * Computes a distributed summary from the loaded feature RDD using a custom
   * per-feature size function.
   *
   * @param features distributed features
   * @param sizeFunction custom size function
   * @return aggregated dataset summary
   */
  public static Summary computeFor(JavaRDD<IFeature> features, FeatureSizeFunction sizeFunction) {
    if (features == null) {
      return new Summary();
    }
    return features.aggregate(
        new Summary(),
        new Function2<Summary, IFeature, Summary>() {
          @Override
          public Summary call(Summary summary, IFeature feature) {
            accumulateFeatureSummary(summary, feature, sizeFunction);
            return summary;
          }
        },
        new Function2<Summary, Summary, Summary>() {
          @Override
          public Summary call(Summary left, Summary right) {
            if (left.getCoordinateDimension() == 0 && !right.isEmpty()) {
              left.setCoordinateDimension(right.getCoordinateDimension());
            }
            left.expandToSummary(right);
            return left;
          }
        });
  }

  /**
   * Writes one dataset summary into options as min/max coordinates plus exact
   * dataset byte size.
   *
   * @param options target options
   * @param summary dataset summary
   * @param minKey option key for min coordinates
   * @param maxKey option key for max coordinates
   * @param sizeKey option key for total size in bytes
   * @return the updated options object
   */
  public static SpatialJoinOptions applyDatasetSummary(SpatialJoinOptions options,
                                                       Summary summary,
                                                       String minKey,
                                                       String maxKey,
                                                       String sizeKey) {
    return applyDatasetSummary(options, summary, minKey, maxKey, sizeKey, null);
  }

  /**
   * Writes one dataset summary into options as min/max coordinates plus exact dataset byte size
   * and optional feature count.
   *
   * @param options target options
   * @param summary dataset summary
   * @param minKey option key for min coordinates
   * @param maxKey option key for max coordinates
   * @param sizeKey option key for total size in bytes
   * @param featureCountKey option key for total feature count
   * @return the updated options object
   */
  public static SpatialJoinOptions applyDatasetSummary(SpatialJoinOptions options,
                                                       Summary summary,
                                                       String minKey,
                                                       String maxKey,
                                                       String sizeKey,
                                                       String featureCountKey) {
    SpatialJoinOptions targetOptions = options == null ? new SpatialJoinOptions() : options;
    if (sizeKey != null) {
      long sizeBytes = summary == null ? 0L : Math.max(0L, summary.size());
      targetOptions.set(sizeKey, Long.toString(sizeBytes));
    }
    if (featureCountKey != null) {
      long featureCount = summary == null ? 0L : Math.max(0L, summary.numFeatures());
      targetOptions.set(featureCountKey, Long.toString(featureCount));
    }
    if (summary == null || summary.isEmpty() || summary.getCoordinateDimension() == 0) {
      return targetOptions;
    }
    if (minKey != null) {
      targetOptions.set(minKey, formatSummaryCoords(summary, true));
    }
    if (maxKey != null) {
      targetOptions.set(maxKey, formatSummaryCoords(summary, false));
    }
    return targetOptions;
  }

  /**
   * Writes both left and right dataset summaries into the LAHilbert option
   * keys so that the partitioner can derive the overlap domain and use exact
   * dataset heap-estimate totals gathered during reading.
   *
   * @param options target options
   * @param leftSummary left dataset summary
   * @param rightSummary right dataset summary
   * @return the updated options object
   */
  public static SpatialJoinOptions applyLAHilbertDatasetSummaries(SpatialJoinOptions options,
                                                                  Summary leftSummary,
                                                                  Summary rightSummary) {
    SpatialJoinOptions targetOptions = options == null ? new SpatialJoinOptions() : options;
    applyDatasetSummary(
        targetOptions,
        leftSummary,
        SpatialJoinConstants.LAHilbert.LEFT_MIN_COORDS,
        SpatialJoinConstants.LAHilbert.LEFT_MAX_COORDS,
        SpatialJoinConstants.LAHilbert.LEFT_SUMMARY_SIZE,
        SpatialJoinConstants.LAHilbert.LEFT_SUMMARY_FEATURES);
    applyDatasetSummary(
        targetOptions,
        rightSummary,
        SpatialJoinConstants.LAHilbert.RIGHT_MIN_COORDS,
        SpatialJoinConstants.LAHilbert.RIGHT_MAX_COORDS,
        SpatialJoinConstants.LAHilbert.RIGHT_SUMMARY_SIZE,
        SpatialJoinConstants.LAHilbert.RIGHT_SUMMARY_FEATURES);
    return targetOptions;
  }

  /** Accumulates one loaded feature into a mutable summary during distributed aggregation. */
  private static void accumulateFeatureSummary(Summary summary, IFeature feature, FeatureSizeFunction sizeFunction) {
    if (summary == null || feature == null) {
      return;
    }
    Geometry geometry = feature.getGeometry();
    if (summary.getCoordinateDimension() == 0 && geometry != null) {
      summary.setCoordinateDimension(GeometryHelper.getCoordinateDimension(geometry));
    }
    int featureSize = sizeFunction == null ? feature.getHeapEstimateBytes() : sizeFunction.applyAsInt(feature);
    summary.expandToGeometryWithSize(geometry, featureSize);
  }

  /** Accumulates one feature into a summary using only lightweight binary-derived metadata. */
  private static void accumulateFeatureSummaryBinary(Summary summary, IFeature feature, String loadEstimateMode) {
    if (summary == null || feature == null) {
      return;
    }
    byte[] geometryBytes = feature.getGeometryBytes();
    if (geometryBytes == null) {
      AbstractFeature abstractFeature = AbstractFeature.fromFeatureBinary(feature, -1L);
      summary.expandToAbstractFeature(abstractFeature);
      return;
    }

    GeometryMetadataReader.Metadata metadata =
        GeometryMetadataReader.read(geometryBytes, GeometryMetadataReader.DetailLevel.SUMMARY);
    EnvelopeNDLite envelope =
        metadata == null || metadata.isEmpty() ? new EnvelopeNDLite() : metadata.getEnvelope();
    int numPoints = metadata == null ? 0 : metadata.getNumPoints();
    GeometryType geometryType =
        metadata == null ? GeometryType.EMPTY : metadata.getGeometryType();
    double envelopeArea =
        metadata == null ? 0.0d : Math.max(0.0d, metadata.getEnvelopeArea2D());
    AbstractFeature abstractFeature = new AbstractFeature(
        envelope,
        -1L,
        numPoints,
        Math.max(0L, feature.getStorageSize()),
        Math.max(0L, feature.getExecutionLoadEstimateBytes(loadEstimateMode)),
        0.0d,
        envelopeArea,
        geometryType);
    summary.expandToAbstractFeature(abstractFeature);
  }

  /** Formats summary min/max coordinates into the comma-separated option payload expected by LAHilbert. */
  private static String formatSummaryCoords(Summary summary, boolean minCoords) {
    StringBuilder builder = new StringBuilder();
    for (int d = 0; d < summary.getCoordinateDimension(); d++) {
      if (d > 0) {
        builder.append(',');
      }
      builder.append(minCoords ? summary.getMinCoord(d) : summary.getMaxCoord(d));
    }
    return builder.toString();
  }
}
