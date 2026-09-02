package cn.edu.whu.spatialJoin.synopses;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.enums.GeometryType;
import cn.edu.whu.spatialJoin.geom.IFeature;
import cn.edu.whu.spatialJoin.geom.util.GeometryMetadataReader;
import org.locationtech.jts.geom.Geometry;

/**
 * Complexity estimation used by partition profiling and job planning.
 *
 * <p>Two calculation paths are available:
 * - exact complexity from the materialized JTS geometry
 * - estimated complexity from serialized geometry metadata without forcing JTS materialization
 *
 * The estimated path is the one used by partition/job planning because input
 * features usually stay in binary form until the final join computation starts.
 */
public final class SizeFunctions {

    private static final double COMPLEXITY_CONSTANT = 8.015e-05;
    private static final double POINT_FACTOR = 2.597e-07;
    private static final double AREA_FACTOR = 2.012e-05;
    private static final double CONVEX_RATIO_FACTOR = -5.346e-07;
    private static final double MBR_RATIO_FACTOR = -6.993e-06;
    private static final double EPSILON = 1.0e-12;

    private SizeFunctions() {
    }

    /** Computes the exact complexity value from the materialized feature geometry. */
    public static double computeActualComplexity(IFeature feature) {
        return feature == null ? 0.0d : computeActualComplexity(feature.getGeometry());
    }

    /** Computes the exact complexity value from a JTS geometry instance. */
    public static double computeActualComplexity(Geometry geometry) {
        if (geometry == null || geometry.isEmpty()) {
            return 0.0d;
        }

        double pointCount = Math.max(1.0d, geometry.getNumPoints());
        double area = Math.max(0.0d, geometry.getArea());
        if (area <= EPSILON) {
            return buildComplexityScore(pointCount, 0.0d, 1.0d, 1.0d);
        }

        double convexHullArea = Math.max(area, geometry.convexHull().getArea());
        double mbrArea = Math.max(area, geometry.getEnvelopeInternal().getArea());
        double convexHullRatio = safeRatio(convexHullArea, area);
        double mbrRatio = safeRatio(mbrArea, area);
        return buildComplexityScore(pointCount, area, convexHullRatio, mbrRatio);
    }

    /** Estimates complexity directly from serialized geometry bytes using the requested mode. */
    public static double estimateComplexity(byte[] geometryBytes, String mode) {
        if (geometryBytes == null || geometryBytes.length == 0) {
            return 0.0d;
        }
        return estimateComplexity(
            GeometryMetadataReader.read(geometryBytes, resolveMetadataDetailLevel(mode)),
            mode);
    }

    /** Estimates complexity from already-parsed geometry metadata. */
    public static double estimateComplexity(GeometryMetadataReader.Metadata metadata) {
        return estimateComplexity(metadata, SpatialJoinConstants.Execution.DEFAULT_COMPLEXITY_MODE);
    }

    /** Estimates complexity from already-parsed geometry metadata using the requested mode. */
    public static double estimateComplexity(GeometryMetadataReader.Metadata metadata, String mode) {
        if (metadata == null || metadata.isEmpty()) {
            return 0.0d;
        }
        switch (normalizeComplexityMode(mode)) {
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_FAST:
                return estimateComplexity(
                    metadata.getGeometryType(),
                    metadata.getNumPoints(),
                    metadata.getEnvelopeArea2D());
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_BALANCED:
                return estimateBalancedComplexity(metadata);
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_EXACT_BINARY:
            default:
                return estimateExactComplexity(metadata);
        }
    }

    /**
     * Estimates complexity from the lightweight fields retained during partition planning.
     *
     * <p>The current binary-path estimate now uses exact metadata extracted from
     * serialized geometry bytes whenever available:
     * exact 2D area, exact convex-hull area, and exact MBR area. This keeps the
     * estimate close to the materialized-geometry complexity without eagerly
     * deserializing the feature.
     */
    public static double estimateComplexity(GeometryType geometryType, int numPoints, double envelopeArea) {
        GeometryType safeType = geometryType == null ? GeometryType.EMPTY : geometryType;
        double pointCount = Math.max(0.0d, numPoints);
        if (safeType == GeometryType.EMPTY) {
            return 0.0d;
        }
        if (safeType == GeometryType.POINT || safeType == GeometryType.MULTIPOINT) {
            return buildComplexityScore(Math.max(1.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }
        if (safeType == GeometryType.LINESTRING || safeType == GeometryType.MULTILINESTRING) {
            return buildComplexityScore(Math.max(2.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }

        double mbrArea = Math.max(0.0d, envelopeArea);
        if (mbrArea <= EPSILON) {
            return buildComplexityScore(Math.max(1.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }

        double fillRatio = estimateFillRatio(safeType, pointCount);
        double areaEstimate = Math.max(EPSILON, mbrArea * fillRatio);
        double mbrRatio = safeRatio(mbrArea, areaEstimate);
        double convexHullRatio = 1.0d + 0.35d * (mbrRatio - 1.0d);
        return buildComplexityScore(pointCount, areaEstimate, convexHullRatio, mbrRatio);
    }

    /**
     * Estimates complexity from already-parsed metadata using exact area and
     * exact convex-hull area extracted from serialized geometry bytes.
     */
    public static double estimateExactComplexity(GeometryMetadataReader.Metadata metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return 0.0d;
        }

        GeometryType safeType = metadata.getGeometryType() == null ? GeometryType.EMPTY : metadata.getGeometryType();
        double pointCount = Math.max(0.0d, metadata.getNumPoints());
        if (safeType == GeometryType.EMPTY) {
            return 0.0d;
        }
        if (safeType == GeometryType.POINT || safeType == GeometryType.MULTIPOINT) {
            return buildComplexityScore(Math.max(1.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }
        if (safeType == GeometryType.LINESTRING || safeType == GeometryType.MULTILINESTRING) {
            return buildComplexityScore(Math.max(2.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }

        double area = Math.max(0.0d, metadata.getExactArea());
        if (area <= EPSILON) {
            return buildComplexityScore(Math.max(1.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }

        double convexHullArea = Math.max(area, metadata.getConvexHullArea());
        double mbrArea = Math.max(area, metadata.getEnvelopeArea2D());
        double convexHullRatio = safeRatio(convexHullArea, area);
        double mbrRatio = safeRatio(mbrArea, area);
        return buildComplexityScore(pointCount, area, convexHullRatio, mbrRatio);
    }

    /** Resolves how much binary metadata needs to be parsed for the requested planning mode. */
    public static GeometryMetadataReader.DetailLevel resolveMetadataDetailLevel(String mode) {
        switch (normalizeComplexityMode(mode)) {
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_FAST:
                return GeometryMetadataReader.DetailLevel.SUMMARY;
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_BALANCED:
                return GeometryMetadataReader.DetailLevel.BALANCED;
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_EXACT_BINARY:
            default:
                return GeometryMetadataReader.DetailLevel.EXACT;
        }
    }

    /**
     * Estimates complexity from binary metadata without running the full convex-hull path.
     *
     * <p>This mode preserves exact area and exact MBR ratio while using a structural
     * approximation for the hull term. It is intended to be a middle ground between the
     * cheap heuristic and the exact-binary path.
     */
    public static double estimateBalancedComplexity(GeometryMetadataReader.Metadata metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return 0.0d;
        }

        GeometryType safeType = metadata.getGeometryType() == null ? GeometryType.EMPTY : metadata.getGeometryType();
        double pointCount = Math.max(0.0d, metadata.getNumPoints());
        if (safeType == GeometryType.EMPTY) {
            return 0.0d;
        }
        if (safeType == GeometryType.POINT || safeType == GeometryType.MULTIPOINT) {
            return buildComplexityScore(Math.max(1.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }
        if (safeType == GeometryType.LINESTRING || safeType == GeometryType.MULTILINESTRING) {
            return buildComplexityScore(Math.max(2.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }

        double area = Math.max(0.0d, metadata.getExactArea());
        if (area <= EPSILON) {
            return buildComplexityScore(Math.max(1.0d, pointCount), 0.0d, 1.0d, 1.0d);
        }

        double mbrArea = Math.max(area, metadata.getEnvelopeArea2D());
        double mbrRatio = safeRatio(mbrArea, area);
        double convexHullRatio = estimateBalancedHullRatio(metadata, mbrRatio);
        return buildComplexityScore(pointCount, area, convexHullRatio, mbrRatio);
    }

    /** Normalizes the user-facing complexity mode string to one of the supported values. */
    public static String normalizeComplexityMode(String mode) {
        if (mode == null || mode.trim().isEmpty()) {
            return SpatialJoinConstants.Execution.DEFAULT_COMPLEXITY_MODE;
        }
        String normalized = mode.trim().toLowerCase();
        switch (normalized) {
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_FAST:
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_BALANCED:
            case SpatialJoinConstants.Execution.COMPLEXITY_MODE_EXACT_BINARY:
                return normalized;
            default:
                return SpatialJoinConstants.Execution.DEFAULT_COMPLEXITY_MODE;
        }
    }

    /** Builds the scalar complexity score from the model inputs. */
    private static double buildComplexityScore(double pointCount,
                                               double area,
                                               double convexHullRatio,
                                               double mbrRatio) {
        double score = COMPLEXITY_CONSTANT +
            POINT_FACTOR * Math.max(0.0d, pointCount) +
            AREA_FACTOR * Math.max(0.0d, area) +
            CONVEX_RATIO_FACTOR * Math.max(1.0d, convexHullRatio) +
            MBR_RATIO_FACTOR * Math.max(1.0d, mbrRatio);
        return Double.isFinite(score) ? Math.max(0.0d, score) : 0.0d;
    }

    /** Safely computes a positive ratio and clamps degenerate cases to 1. */
    private static double safeRatio(double numerator, double denominator) {
        if (!Double.isFinite(numerator) || !Double.isFinite(denominator) || denominator <= EPSILON) {
            return 1.0d;
        }
        return Math.max(1.0d, numerator / denominator);
    }

    /** Estimates how tightly a polygonal geometry fills its MBR. */
    private static double estimateFillRatio(GeometryType geometryType, double pointCount) {
        double boundedPointCount = Math.max(1.0d, pointCount);
        if (geometryType == GeometryType.ENVELOPE) {
            return 1.0d;
        }
        if (geometryType == GeometryType.POLYGON) {
            return clamp(0.92d - 0.06d * Math.log1p(boundedPointCount), 0.25d, 0.95d);
        }
        if (geometryType == GeometryType.MULTIPOLYGON || geometryType == GeometryType.GEOMETRYCOLLECTION) {
            return clamp(0.78d - 0.05d * Math.log1p(boundedPointCount), 0.15d, 0.90d);
        }
        return clamp(0.70d - 0.04d * Math.log1p(boundedPointCount), 0.15d, 0.90d);
    }

    /**
     * Approximates the hull ratio from exact area/MBR statistics and lightweight structure counters.
     *
     * <p>Compared with the fast mode, this version benefits from exact area and can
     * therefore derive a more stable MBR ratio. Multipart, multi-ring, and hole-rich
     * geometries receive a slightly larger hull-ratio estimate because they are less
     * likely to be close to convex.
     */
    private static double estimateBalancedHullRatio(GeometryMetadataReader.Metadata metadata, double mbrRatio) {
        double hullRatio = 1.0d + 0.35d * (mbrRatio - 1.0d);
        hullRatio += 0.03d * Math.max(0, metadata.getPartCount() - 1);
        hullRatio += 0.02d * Math.max(0, metadata.getHoleCount());
        hullRatio += 0.01d * Math.max(0, metadata.getRingCount() - metadata.getPartCount());
        return Math.max(1.0d, hullRatio);
    }

    private static double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }
}
