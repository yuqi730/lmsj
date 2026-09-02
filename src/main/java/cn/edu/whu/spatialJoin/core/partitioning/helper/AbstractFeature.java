package cn.edu.whu.spatialJoin.core.partitioning.helper;

import cn.edu.whu.spatialJoin.enums.GeometryType;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.IFeature;
import cn.edu.whu.spatialJoin.geom.util.GeometryMetadataReader;
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper;
import cn.edu.whu.spatialJoin.synopses.SizeFunctions;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

/**
 * A lightweight abstract feature descriptor used during partition construction.
 *
 * <p>This structure keeps only the information needed by partitioners:
 * the feature MBR together with a few load-related metrics such as number
 * of points, serialized size, heap estimate, and envelope area. It is intended to
 * represent the full input dataset during partition construction without
 * carrying the full geometry payload or arbitrary attributes. In hierarchical
 * build mode it can also carry a lightweight hierarchy key for driver-side
 * refinement across aggregated levels.
 */
public class AbstractFeature extends EnvelopeNDLite implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Stable per-feature identifier later reused by assignment and binary-feature reload. */
  private long oid = -1L;
  /** Number of points represented by the original geometry or aggregated descriptor. */
  private int numPoints;
  /** Serialized-size estimate kept mainly for storage-oriented diagnostics. */
  private long storageSizeBytes;
  /** Heap-oriented load estimate used by load-aware partitioning and job planning. */
  private long heapEstimateBytes;
  /** Estimated computation complexity contributed by this feature or aggregate. */
  private double complexityEstimate;
  /** Area of the descriptor envelope, used by complexity estimation and diagnostics. */
  private double envelopeArea;
  /** Number of finest-level members folded into this descriptor. */
  private long memberCount = 1L;
  /** Whether the representative-coordinate moments exactly describe all finest-level members. */
  private boolean representativeMomentsExact;
  /** Mean and centered second moment of finest-level object MBR-center X coordinates. */
  private double representativeMeanX;
  private double representativeM2X;
  /** Mean and centered second moment of finest-level object MBR-center Y coordinates. */
  private double representativeMeanY;
  private double representativeM2Y;
  /** Hierarchy level where this descriptor currently lives; 0 means finest level. */
  private int aggregationLevel = 0;
  /** Coerced geometry type represented by this descriptor or aggregate. */
  private GeometryType geometryType = GeometryType.EMPTY;
  /** Key that links aggregated descriptors across hierarchy levels. */
  private String hierarchyKey;

  /** Creates an empty lightweight descriptor. */
  public AbstractFeature() {
    super();
  }

  /** Copy constructor used while aggregating hierarchy levels. */
  public AbstractFeature(AbstractFeature other) {
    super(other);
    this.oid = other.oid;
    this.numPoints = other.numPoints;
    this.storageSizeBytes = other.storageSizeBytes;
    this.heapEstimateBytes = other.heapEstimateBytes;
    this.complexityEstimate = other.complexityEstimate;
    this.envelopeArea = other.envelopeArea;
    this.memberCount = other.memberCount;
    this.representativeMomentsExact = other.representativeMomentsExact;
    this.representativeMeanX = other.representativeMeanX;
    this.representativeM2X = other.representativeM2X;
    this.representativeMeanY = other.representativeMeanY;
    this.representativeM2Y = other.representativeM2Y;
    this.aggregationLevel = other.aggregationLevel;
    this.geometryType = other.geometryType;
    this.hierarchyKey = other.hierarchyKey;
  }

  /**
   * Fully specified constructor used when all lightweight statistics are already known.
   *
   * @param envelope descriptor envelope or aggregate envelope
   * @param oid stable feature identifier
   * @param numPoints number of points represented by the descriptor
   * @param storageSizeBytes serialized-size estimate
   * @param heapEstimateBytes heap-oriented load estimate
   * @param complexityEstimate precomputed computation-complexity estimate
   * @param envelopeArea area of the descriptor envelope
   * @param memberCount number of finest-level members represented by the descriptor
   * @param aggregationLevel hierarchy level where the descriptor lives
   * @param geometryType geometry type represented by the descriptor
   * @param hierarchyKey key used to stitch aggregated hierarchy levels together
   */
  public AbstractFeature(EnvelopeNDLite envelope,
                         long oid,
                         int numPoints,
                         long storageSizeBytes,
                         long heapEstimateBytes,
                         double complexityEstimate,
                         double envelopeArea,
                         long memberCount,
                         int aggregationLevel,
                         GeometryType geometryType,
                         String hierarchyKey) {
    super(envelope);
    this.oid = oid;
    this.numPoints = numPoints;
    this.storageSizeBytes = storageSizeBytes;
    this.heapEstimateBytes = Math.max(0L, heapEstimateBytes);
    this.complexityEstimate = Math.max(0.0d, complexityEstimate);
    this.envelopeArea = envelopeArea;
    this.memberCount = Math.max(1L, memberCount);
    this.aggregationLevel = Math.max(0, aggregationLevel);
    this.geometryType = geometryType == null ? GeometryType.EMPTY : geometryType;
    this.hierarchyKey = hierarchyKey;
  }

  /**
   * Convenience constructor for one finest-level feature when complexity has already been computed.
   */
  public AbstractFeature(EnvelopeNDLite envelope,
                         long oid,
                         int numPoints,
                         long storageSizeBytes,
                         long heapEstimateBytes,
                         double complexityEstimate,
                         double envelopeArea,
                         GeometryType geometryType) {
    this(envelope, oid, numPoints, storageSizeBytes, heapEstimateBytes, complexityEstimate, envelopeArea, 1L, 0, geometryType, null);
  }

  /**
   * Convenience constructor that derives heap estimate and complexity from a serialized-size proxy.
   */
  public AbstractFeature(EnvelopeNDLite envelope,
                         long oid,
                         int numPoints,
                         long storageSizeBytes,
                         double envelopeArea,
                         long memberCount,
                         int aggregationLevel,
                         GeometryType geometryType,
                         String hierarchyKey) {
    this(envelope, oid, numPoints, storageSizeBytes, storageSizeBytes,
        SizeFunctions.estimateComplexity(geometryType, numPoints, envelopeArea),
        envelopeArea, memberCount, aggregationLevel, geometryType, hierarchyKey);
  }

  /**
   * Convenience constructor that derives complexity from explicit heap and envelope statistics.
   */
  public AbstractFeature(EnvelopeNDLite envelope,
                         long oid,
                         int numPoints,
                         long storageSizeBytes,
                         long heapEstimateBytes,
                         double envelopeArea,
                         GeometryType geometryType) {
    this(envelope, oid, numPoints, storageSizeBytes, heapEstimateBytes,
        SizeFunctions.estimateComplexity(geometryType, numPoints, envelopeArea),
        envelopeArea, 1L, 0, geometryType, null);
  }

  /**
   * Convenience constructor for a finest-level feature when heap estimate equals serialized-size estimate.
   */
  public AbstractFeature(EnvelopeNDLite envelope,
                         long oid,
                         int numPoints,
                         long storageSizeBytes,
                         double envelopeArea,
                         GeometryType geometryType) {
    this(envelope, oid, numPoints, storageSizeBytes, storageSizeBytes,
        SizeFunctions.estimateComplexity(geometryType, numPoints, envelopeArea),
        envelopeArea, 1L, 0, geometryType, null);
  }

  /**
   * Creates a lightweight descriptor using the feature's own load estimate.
   *
   * @deprecated Prefer {@link #fromFeatureBinary(IFeature, long)} so partition construction
   * can stay on the binary geometry path and avoid eager geometry materialization.
   *
   * @param feature source feature that may already carry serialized geometry bytes
   * @param oid stable identifier assigned to the source feature
   * @return a lightweight descriptor built through eager geometry materialization
   */
  @Deprecated
  public static AbstractFeature fromFeature(IFeature feature, long oid) {
    return fromFeature(feature, oid, feature == null ? 0L : feature.getStorageSize(),
        feature == null ? 0L : feature.getHeapEstimateBytes());
  }

  /**
   * Creates a lightweight descriptor by reading metadata directly from serialized geometry bytes.
   *
   * <p>This is the preferred constructor for partition construction because it can
   * extract envelope, point count, and geometry type without materializing a full
   * JTS geometry object when the feature already stores geometry in binary form.
   *
   * @param feature source feature that may already carry serialized geometry bytes
   * @param oid stable identifier assigned to the source feature
   * @return a lightweight descriptor that prefers the binary-geometry path
   */
  public static AbstractFeature fromFeatureBinary(IFeature feature, long oid) {
    return fromFeatureBinary(feature, oid,
        feature == null ? 0L : feature.getStorageSize(),
        feature == null ? 0L : feature.getHeapEstimateBytes(),
        null);
  }

  /**
   * Creates a lightweight descriptor by reading metadata directly from serialized geometry bytes
   * and evaluating feature complexity with the requested mode.
   *
   * @param feature source feature that may already carry serialized geometry bytes
   * @param oid stable identifier assigned to the source feature
   * @param complexityMode requested complexity-estimation mode
   * @return a lightweight descriptor that prefers the binary-geometry path
   */
  public static AbstractFeature fromFeatureBinary(IFeature feature, long oid, String complexityMode) {
    return fromFeatureBinary(feature, oid,
        feature == null ? 0L : feature.getStorageSize(),
        feature == null ? 0L : feature.getHeapEstimateBytes(),
        complexityMode);
  }

  /**
   * Creates a lightweight descriptor by reading metadata directly from serialized
   * geometry bytes, falling back to full-geometry parsing only when binary metadata
   * is unavailable.
   *
   * @param feature source feature that may already carry serialized geometry bytes
   * @param oid stable identifier assigned to the source feature
   * @param storageSizeBytes serialized-size estimate to store in the descriptor
   * @return a lightweight descriptor that prefers the binary-geometry path
   */
  public static AbstractFeature fromFeatureBinary(IFeature feature, long oid, long storageSizeBytes) {
    return fromFeatureBinary(feature, oid, storageSizeBytes, storageSizeBytes, null);
  }

  /**
   * Creates a lightweight descriptor by reading metadata directly from serialized
   * geometry bytes while carrying both serialized and heap-oriented load metrics.
   *
   * @param feature source feature that may already carry serialized geometry bytes
   * @param oid stable identifier assigned to the source feature
   * @param storageSizeBytes serialized-size estimate to store in the descriptor
   * @param heapEstimateBytes heap-oriented load estimate to store in the descriptor
   * @return a lightweight descriptor that prefers the binary-geometry path
   */
  public static AbstractFeature fromFeatureBinary(IFeature feature, long oid, long storageSizeBytes, long heapEstimateBytes) {
    return fromFeatureBinary(feature, oid, storageSizeBytes, heapEstimateBytes, null);
  }

  /**
   * Creates a lightweight descriptor by reading metadata directly from serialized
   * geometry bytes while carrying both serialized and heap-oriented load metrics
   * and evaluating complexity with the requested mode.
   *
   * @param feature source feature that may already carry serialized geometry bytes
   * @param oid stable identifier assigned to the source feature
   * @param storageSizeBytes serialized-size estimate to store in the descriptor
   * @param heapEstimateBytes heap-oriented load estimate to store in the descriptor
   * @param complexityMode requested complexity-estimation mode
   * @return a lightweight descriptor; when binary metadata is unavailable, the method
   *         falls back to the eager-geometry path
   */
  public static AbstractFeature fromFeatureBinary(IFeature feature,
                                                  long oid,
                                                  long storageSizeBytes,
                                                  long heapEstimateBytes,
                                                  String complexityMode) {
    AbstractFeature abstractFeature = createBaseFeature(oid, storageSizeBytes, heapEstimateBytes);
    if (feature == null) {
      return abstractFeature;
    }

    byte[] geometryBytes = feature.getGeometryBytes();
    if (geometryBytes == null) {
      return fromFeature(feature, oid, storageSizeBytes, heapEstimateBytes);
    }

    try {
      GeometryMetadataReader.Metadata metadata = GeometryMetadataReader.read(
          geometryBytes,
          resolveMetadataDetailLevel(complexityMode));
      applyMetadata(abstractFeature, metadata);
      abstractFeature.complexityEstimate = SizeFunctions.estimateComplexity(metadata, complexityMode);
      return abstractFeature;
    } catch (RuntimeException e) {
      return fromFeature(feature, oid, storageSizeBytes, heapEstimateBytes, complexityMode);
    }
  }

  /**
   * Creates the lightweight descriptor used during partition construction.
   *
   * Only geometry-derived summary information is retained here: MBR, point count,
   * serialized size, heap estimate, geometry type, and a stable OID. The full geometry payload
   * and ordinary attributes stay out of the partition-construction path.
   *
   * @deprecated Prefer {@link #fromFeatureBinary(IFeature, long, long)} so partition construction
   * can extract lightweight geometry metadata from serialized bytes first and only fall back
   * to full geometry parsing when binary metadata is unavailable.
   *
   * @param feature source feature that will be materialized eagerly
   * @param oid stable identifier assigned to the source feature
   * @param storageSizeBytes serialized-size estimate to store in the descriptor
   * @return a lightweight descriptor built through eager geometry materialization
   */
  @Deprecated
  public static AbstractFeature fromFeature(IFeature feature, long oid, long storageSizeBytes) {
    return fromFeature(feature, oid, storageSizeBytes, storageSizeBytes);
  }

  /**
   * Creates the lightweight descriptor used during partition construction while
   * carrying both serialized size and heap-estimate load metrics.
   *
   * @param feature source feature that will be materialized eagerly
   * @param oid stable identifier assigned to the source feature
   * @param storageSizeBytes serialized-size estimate to store in the descriptor
   * @param heapEstimateBytes heap-oriented load estimate to store in the descriptor
   * @return a lightweight descriptor built through eager geometry materialization
   */
  @Deprecated
  public static AbstractFeature fromFeature(IFeature feature, long oid, long storageSizeBytes, long heapEstimateBytes) {
    return fromFeature(feature, oid, storageSizeBytes, heapEstimateBytes, null);
  }

  /**
   * Creates the lightweight descriptor used during partition construction while
   * carrying both serialized size and heap-estimate load metrics.
   *
   * @param feature source feature that will be materialized eagerly
   * @param oid stable identifier assigned to the source feature
   * @param storageSizeBytes serialized-size estimate to store in the descriptor
   * @param heapEstimateBytes heap-oriented load estimate to store in the descriptor
   * @param complexityMode requested complexity-estimation mode
   * @return a lightweight descriptor built through eager geometry materialization
   */
  @Deprecated
  public static AbstractFeature fromFeature(IFeature feature,
                                            long oid,
                                            long storageSizeBytes,
                                            long heapEstimateBytes,
                                            String complexityMode) {
    AbstractFeature abstractFeature = createBaseFeature(oid, storageSizeBytes, heapEstimateBytes);
    if (feature == null) {
      return abstractFeature;
    }

    Geometry geometry = feature.getGeometry();
    if (geometry == null || geometry.isEmpty()) {
      abstractFeature.numPoints = geometry == null ? 0 : geometry.getNumPoints();
      abstractFeature.geometryType = GeometryType.EMPTY;
      abstractFeature.envelopeArea = 0.0d;
      return abstractFeature;
    }

    abstractFeature.setCoordinateDimension(GeometryHelper.getCoordinateDimension(geometry));
    abstractFeature.merge(geometry);
    abstractFeature.numPoints = geometry.getNumPoints();
    abstractFeature.geometryType = resolveGeometryType(geometry);
    abstractFeature.envelopeArea = abstractFeature.getArea();
    abstractFeature.complexityEstimate =
        complexityMode == null ? SizeFunctions.computeActualComplexity(geometry) : feature.getComplexityEstimate(complexityMode);
    return abstractFeature;
  }

  /**
   * Creates the base empty descriptor before geometry-specific metadata is filled in.
   *
   * @param oid stable feature identifier
   * @param storageSizeBytes serialized-size estimate
   * @param heapEstimateBytes heap-oriented load estimate
   * @return a minimally initialized descriptor that later metadata-population steps can enrich
   */
  private static AbstractFeature createBaseFeature(long oid, long storageSizeBytes, long heapEstimateBytes) {
    AbstractFeature abstractFeature = new AbstractFeature();
    abstractFeature.oid = oid;
    abstractFeature.storageSizeBytes = Math.max(0L, storageSizeBytes);
    abstractFeature.heapEstimateBytes = Math.max(0L, heapEstimateBytes);
    abstractFeature.complexityEstimate = 0.0d;
    abstractFeature.hierarchyKey = "feature:" + oid;
    return abstractFeature;
  }

  private static GeometryMetadataReader.DetailLevel resolveMetadataDetailLevel(String complexityMode) {
    String normalized = SizeFunctions.normalizeComplexityMode(complexityMode);
    switch (normalized) {
      case cn.edu.whu.spatialJoin.common.SpatialJoinConstants.Execution.COMPLEXITY_MODE_FAST:
        return GeometryMetadataReader.DetailLevel.SUMMARY;
      case cn.edu.whu.spatialJoin.common.SpatialJoinConstants.Execution.COMPLEXITY_MODE_BALANCED:
        return GeometryMetadataReader.DetailLevel.BALANCED;
      case cn.edu.whu.spatialJoin.common.SpatialJoinConstants.Execution.COMPLEXITY_MODE_EXACT_BINARY:
      default:
        return GeometryMetadataReader.DetailLevel.EXACT;
    }
  }

  /**
   * Copies parsed geometry metadata into an existing descriptor.
   *
   * <p>This method is the bridge between the binary metadata reader and the lightweight
   * partition-construction model. It fills only geometry-derived fields and intentionally
   * leaves identifiers/load estimates unchanged.
   *
   * @param abstractFeature descriptor being populated
   * @param metadata metadata parsed from serialized geometry bytes
   */
  private static void applyMetadata(AbstractFeature abstractFeature, GeometryMetadataReader.Metadata metadata) {
    if (abstractFeature == null || metadata == null || metadata.isEmpty()) {
      if (abstractFeature != null) {
        abstractFeature.numPoints = 0;
        abstractFeature.geometryType = GeometryType.EMPTY;
        abstractFeature.envelopeArea = 0.0d;
        abstractFeature.complexityEstimate = 0.0d;
      }
      return;
    }
    EnvelopeNDLite envelope = metadata.getEnvelope();
    if (envelope.getCoordinateDimension() > 0 && !envelope.isEmpty()) {
      abstractFeature.set(envelope);
      abstractFeature.setCoordinateDimension(envelope.getCoordinateDimension());
      abstractFeature.envelopeArea = abstractFeature.getArea();
    } else {
      abstractFeature.envelopeArea = 0.0d;
    }
    abstractFeature.numPoints = metadata.getNumPoints();
    abstractFeature.geometryType = metadata.getGeometryType();
    abstractFeature.complexityEstimate = SizeFunctions.estimateComplexity(metadata);
  }

  /**
   * Merges another lightweight descriptor into this one.
   *
   * This is the key operation used by hierarchical partition construction: multiple
   * feature-level descriptors are folded into one coarse cell while preserving the
   * load-related statistics needed by later refinement.
   * @param other descriptor being folded into this aggregate
   * @return {@code this} descriptor after the in-place merge
   */
  public AbstractFeature mergeInPlace(AbstractFeature other) {
    if (other == null) {
      return this;
    }
    long thisMemberCount = Math.max(1L, this.memberCount);
    long otherMemberCount = Math.max(1L, other.memberCount);
    boolean exactMergedMoments =
        this.representativeMomentsExact && other.representativeMomentsExact;
    double[] mergedX = mergeMoments(
        thisMemberCount,
        this.representativeMeanX,
        this.representativeM2X,
        otherMemberCount,
        other.representativeMeanX,
        other.representativeM2X);
    double[] mergedY = mergeMoments(
        thisMemberCount,
        this.representativeMeanY,
        this.representativeM2Y,
        otherMemberCount,
        other.representativeMeanY,
        other.representativeM2Y);
    if (this.isEmpty()) {
      this.set(other);
    } else if (!other.isEmpty()) {
      this.merge(other);
    }
    this.numPoints += other.numPoints;
    this.storageSizeBytes += other.storageSizeBytes;
    this.heapEstimateBytes += other.heapEstimateBytes;
    this.complexityEstimate += other.complexityEstimate;
    this.envelopeArea += other.envelopeArea;
    this.memberCount += other.memberCount;
    this.representativeMomentsExact = exactMergedMoments;
    this.representativeMeanX = mergedX[0];
    this.representativeM2X = mergedX[1];
    this.representativeMeanY = mergedY[0];
    this.representativeM2Y = mergedY[1];
    this.aggregationLevel = Math.max(this.aggregationLevel, other.aggregationLevel);
    this.geometryType = this.geometryType.coerce(other.geometryType);
    return this;
  }

  /**
   * Updates the hierarchy level carried by this descriptor.
   *
   * @param aggregationLevel hierarchy level to store
   * @return {@code this} descriptor for fluent aggregation code
   */
  public AbstractFeature withAggregationLevel(int aggregationLevel) {
    this.aggregationLevel = Math.max(0, aggregationLevel);
    return this;
  }

  /**
   * Updates the hierarchy key carried by this descriptor.
   *
   * @param hierarchyKey key that should identify this descriptor across hierarchy levels
   * @return {@code this} descriptor for fluent aggregation code
   */
  public AbstractFeature withHierarchyKey(String hierarchyKey) {
    this.hierarchyKey = hierarchyKey;
    return this;
  }

  /**
   * Enables exact object MBR-center moments for one finest-level descriptor.
   *
   * <p>This is invoked only when synopsis construction needs to preserve object-center statistics,
   * keeping the established partition paths free from unnecessary moment arithmetic.
   */
  public AbstractFeature withExactRepresentativeMoments() {
    initializeRepresentativeMomentsFromEnvelope(memberCount == 1L);
    return this;
  }

  /**
   * Maps a JTS runtime geometry class name to the project's stable {@link GeometryType} enum.
   *
   * @param geometry materialized geometry
   * @return normalized geometry type used by partitioning and complexity estimation
   */
  private static GeometryType resolveGeometryType(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
      return GeometryType.EMPTY;
    }
    String geometryTypeName = geometry.getGeometryType();
    if ("LinearRing".equalsIgnoreCase(geometryTypeName)) {
      return GeometryType.LINESTRING;
    }
    if ("Envelope".equalsIgnoreCase(geometryTypeName)) {
      return GeometryType.ENVELOPE;
    }
    try {
      return GeometryType.getGeometryType(geometryTypeName);
    } catch (IllegalArgumentException e) {
      return GeometryType.GEOMETRYCOLLECTION;
    }
  }

  /** Returns the stable feature identifier used later for binary-feature reload. */
  public long getOid() {
    return oid;
  }

  /** Returns the number of points represented by this lightweight descriptor. */
  public int getNumPoints() {
    return numPoints;
  }

  /** Returns the estimated serialized storage footprint in bytes. */
  public long getStorageSizeBytes() {
    return storageSizeBytes;
  }

  /** Returns the heap-oriented load estimate in bytes used by partitioning. */
  public long getHeapEstimateBytes() {
    return heapEstimateBytes;
  }

  /** Returns the per-feature computation-complexity estimate used during later job planning. */
  public double getComplexityEstimate() {
    return complexityEstimate;
  }

  /** Returns the area of the descriptor envelope. */
  public double getEnvelopeArea() {
    return envelopeArea;
  }

  /** Returns how many finest-level members are represented by this descriptor. */
  public long getMemberCount() {
    return memberCount;
  }

  /** Returns whether exact original-object representative-coordinate moments are available. */
  public boolean hasExactRepresentativeMoments(int axis) {
    return representativeMomentsExact && axis >= 0 && axis < Math.min(2, getCoordinateDimension());
  }

  /** Returns the original-object representative-coordinate mean for one axis. */
  public double getRepresentativeCoordinateMean(int axis) {
    if (axis == 0)
      return representativeMeanX;
    if (axis == 1)
      return representativeMeanY;
    throw new IllegalArgumentException("Representative moments are only available for 2D axes");
  }

  /** Returns the centered second moment of original-object representative coordinates. */
  public double getRepresentativeCoordinateM2(int axis) {
    if (axis == 0)
      return representativeM2X;
    if (axis == 1)
      return representativeM2Y;
    throw new IllegalArgumentException("Representative moments are only available for 2D axes");
  }

  /** Returns the hierarchy level at which this descriptor currently lives. */
  public int getAggregationLevel() {
    return aggregationLevel;
  }

  /** Returns the coerced geometry type represented by this descriptor. */
  public GeometryType getGeometryType() {
    return geometryType;
  }

  /** Returns the hierarchy key used to stitch adjacent aggregated levels together. */
  public String getHierarchyKey() {
    return hierarchyKey;
  }

  /**
   * Returns the primary load metric used by the current load-aware partitioning flow.
   *
   * <p>Heap estimate is intentionally prioritized because partition refinement is
   * driven mainly by in-memory computation pressure rather than only by feature count.
   *
   * @return feature load weight in bytes
   */
  public long getLoadWeight() {
    return heapEstimateBytes;
  }

  /** Initializes one-member moments from the object MBR center. */
  private void initializeRepresentativeMomentsFromEnvelope(boolean exact) {
    if (isEmpty() || getCoordinateDimension() == 0) {
      representativeMomentsExact = false;
      return;
    }
    representativeMeanX = getCenter(0);
    representativeM2X = 0.0d;
    if (getCoordinateDimension() > 1) {
      representativeMeanY = getCenter(1);
      representativeM2Y = 0.0d;
    }
    representativeMomentsExact = exact;
  }

  /** Merges two weighted centered-moment summaries using the parallel variance formula. */
  private static double[] mergeMoments(long leftCount,
                                       double leftMean,
                                       double leftM2,
                                       long rightCount,
                                       double rightMean,
                                       double rightM2) {
    double safeLeftCount = Math.max(1L, leftCount);
    double safeRightCount = Math.max(1L, rightCount);
    double totalCount = safeLeftCount + safeRightCount;
    double delta = rightMean - leftMean;
    double mergedMean = leftMean + delta * safeRightCount / totalCount;
    double mergedM2 =
        Math.max(0.0d, leftM2) +
            Math.max(0.0d, rightM2) +
            delta * delta * safeLeftCount * safeRightCount / totalCount;
    return new double[]{mergedMean, Math.max(0.0d, mergedM2)};
  }
}
