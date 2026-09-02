package cn.edu.whu.spatialJoin.geom.util;

import cn.edu.whu.spatialJoin.enums.GeometryType;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.WKBConstants;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reads lightweight geometry metadata directly from the project's serialized geometry bytes.
 *
 * <p>The parser now extracts enough information to support both lightweight
 * partition planning and a much tighter binary-path computation-complexity
 * estimate:
 * envelope, point count, geometry type, coordinate dimension, exact 2D area,
 * convex-hull area, and simple structural counters such as part/ring/hole
 * counts.
 */
public final class GeometryMetadataReader {

  /**
   * Controls how much work the metadata parser performs.
   *
   * <p>Only the EXACT level keeps all 2D points needed to compute the convex-hull area.
   * SUMMARY and BALANCED intentionally skip that collection so large polygonal geometries
   * can stay lightweight during partition planning and summary aggregation.
   */
  public enum DetailLevel {
    SUMMARY,
    BALANCED,
    EXACT
  }

  private static final long OBJECT_ALIGNMENT_BYTES = 8L;
  private static final long OBJECT_HEADER_BYTES = 16L;
  private static final long ARRAY_HEADER_BYTES = 24L;
  private static final long ENVELOPE_BYTES = 48L;

  private GeometryMetadataReader() {
  }

  /** Parses lightweight metadata from one serialized geometry payload. */
  public static Metadata read(byte[] bytes) {
    return read(bytes, DetailLevel.EXACT);
  }

  /** Parses geometry metadata with the requested detail level. */
  public static Metadata read(byte[] bytes, DetailLevel detailLevel) {
    if (bytes == null || bytes.length == 0)
      return Metadata.empty();
    try (DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return parse(dataInput, detailLevel == null ? DetailLevel.EXACT : detailLevel).toMetadata();
    } catch (IOException e) {
      throw new RuntimeException("Could not read geometry metadata from serialized bytes", e);
    }
  }

  /**
   * Estimates the JVM heap footprint of one geometry that is currently stored in
   * the project's serialized binary representation.
   *
   * <p>The estimate intentionally includes both:
   * - the serialized byte-array payload still retained by {@code Feature}
   * - the likely JTS-side object graph created once the geometry is materialized
   */
  public static int estimateHeapBytes(byte[] bytes) {
    if (bytes == null || bytes.length == 0)
      return 0;
    Metadata metadata = read(bytes);
    return estimateHeapBytes(metadata, bytes.length);
  }

  /** Estimates only the retained heap bytes of the serialized geometry payload. */
  public static int estimateRetainedBinaryHeapBytes(byte[] bytes) {
    return bytes == null ? 0 : safeToInt(align(ARRAY_HEADER_BYTES + Math.max(0L, bytes.length)));
  }

  /** Estimates only the materialized JTS-side heap bytes of one geometry payload. */
  public static int estimateMaterializedGeometryHeapBytes(byte[] bytes) {
    if (bytes == null || bytes.length == 0)
      return 0;
    return estimateMaterializedGeometryHeapBytes(read(bytes));
  }

  /** Estimates the heap footprint from already-parsed metadata plus serialized length. */
  public static int estimateHeapBytes(Metadata metadata, int serializedLength) {
    long retainedBinaryBytes = align(ARRAY_HEADER_BYTES + Math.max(0L, serializedLength));
    long materializedGeometryBytes = estimateMaterializedGeometryHeapBytes(metadata);
    return safeToInt(retainedBinaryBytes + materializedGeometryBytes);
  }

  /** Estimates only the materialized geometry-object bytes from already-parsed metadata. */
  public static int estimateMaterializedGeometryHeapBytes(Metadata metadata) {
    if (metadata == null || metadata.isEmpty())
      return 0;

    long coordinateDimension = Math.max(2L, metadata.getCoordinateDimension());
    long numPoints = Math.max(1L, metadata.getNumPoints());
    long coordinateArrayBytes = align(ARRAY_HEADER_BYTES + numPoints * coordinateDimension * Double.BYTES);
    long coordinateIndexBytes = align(ARRAY_HEADER_BYTES + numPoints * Long.BYTES);
    long geometryShellBytes = align(baseGeometryBytes(metadata.getGeometryType()));
    return safeToInt(align(ENVELOPE_BYTES + coordinateArrayBytes + coordinateIndexBytes + geometryShellBytes));
  }

  private static ParsedGeometry parse(DataInput dataInput, DetailLevel detailLevel) throws IOException {
    byte byteOrdering = dataInput.readByte();
    if (byteOrdering != WKBConstants.wkbXDR)
      throw new IllegalArgumentException("Only BIG ENDIAN serialized geometries are supported");

    int geometryTypeDimension = dataInput.readInt();
    boolean sridIncluded = (geometryTypeDimension & WKBConstants.wkbIncludeSRID) != 0;
    if (sridIncluded)
      dataInput.readInt();
    geometryTypeDimension &= ~WKBConstants.wkbIncludeSRID;

    int geometryType = geometryTypeDimension % 1000;
    Layout layout = resolveLayout(geometryTypeDimension - geometryType);
    switch (geometryType) {
      case WKBConstants.wkbEmpty:
        return ParsedGeometry.empty();
      case WKBConstants.wkbPoint:
        return parsePoint(dataInput, layout.dimension, layout.measure, detailLevel);
      case WKBConstants.wkbEnvelope:
        return parseEnvelope(dataInput, layout.dimension, detailLevel);
      case WKBConstants.wkbLineString:
        return parseCoordinateSequenceGeometry(dataInput, layout.dimension, layout.measure, GeometryType.LINESTRING, detailLevel);
      case WKBConstants.wkbPolygon:
        return parsePolygon(dataInput, layout.dimension, layout.measure, detailLevel);
      case WKBConstants.wkbMultiPoint:
        return parseCollection(dataInput, GeometryType.MULTIPOINT, detailLevel);
      case WKBConstants.wkbMultiLineString:
        return parseCollection(dataInput, GeometryType.MULTILINESTRING, detailLevel);
      case WKBConstants.wkbMultiPolygon:
        return parseCollection(dataInput, GeometryType.MULTIPOLYGON, detailLevel);
      case WKBConstants.wkbGeometryCollection:
        return parseCollection(dataInput, GeometryType.GEOMETRYCOLLECTION, detailLevel);
      default:
        throw new IllegalArgumentException("Unsupported geometry type " + geometryType);
    }
  }

  private static ParsedGeometry parsePoint(DataInput dataInput,
                                           int dimension,
                                           boolean measure,
                                           DetailLevel detailLevel) throws IOException {
    double[] coordinates = new double[dimension];
    boolean allNan = true;
    for (int d = 0; d < dimension; d++) {
      coordinates[d] = dataInput.readDouble();
      if (!Double.isNaN(coordinates[d]))
        allNan = false;
    }
    if (measure) {
      double m = dataInput.readDouble();
      if (!Double.isNaN(m))
        allNan = false;
    }
    if (allNan)
      return ParsedGeometry.empty();

    int effectiveDimension = resolveEffectiveDimension(coordinates);
    EnvelopeNDLite envelope = new EnvelopeNDLite(effectiveDimension);
    for (int d = 0; d < effectiveDimension; d++) {
      envelope.setMinCoord(d, coordinates[d]);
      envelope.setMaxCoord(d, coordinates[d]);
    }
    ParsedGeometry parsed = new ParsedGeometry(envelope, 1, GeometryType.POINT, effectiveDimension, detailLevel == DetailLevel.EXACT);
    if (effectiveDimension >= 2 && Double.isFinite(coordinates[0]) && Double.isFinite(coordinates[1]))
      parsed.pointCollector.add(coordinates[0], coordinates[1]);
    parsed.partCount = 1;
    return parsed.freeze(GeometryType.POINT);
  }

  private static ParsedGeometry parseEnvelope(DataInput dataInput, int dimension, DetailLevel detailLevel) throws IOException {
    double[] minCoord = new double[dimension];
    double[] maxCoord = new double[dimension];
    for (int d = 0; d < dimension; d++)
      minCoord[d] = dataInput.readDouble();
    for (int d = 0; d < dimension; d++)
      maxCoord[d] = dataInput.readDouble();
    EnvelopeNDLite envelope = new EnvelopeNDLite(minCoord, maxCoord);
    ParsedGeometry parsed = new ParsedGeometry(envelope, 4, GeometryType.ENVELOPE, dimension, detailLevel == DetailLevel.EXACT);
    parsed.partCount = 1;
    parsed.ringCount = 1;
    parsed.exactArea = envelopeArea2D(envelope);
    if (dimension >= 2 && !envelope.isEmpty()) {
      double minX = envelope.getMinCoord(0);
      double minY = envelope.getMinCoord(1);
      double maxX = envelope.getMaxCoord(0);
      double maxY = envelope.getMaxCoord(1);
      parsed.pointCollector.add(minX, minY);
      parsed.pointCollector.add(maxX, minY);
      parsed.pointCollector.add(maxX, maxY);
      parsed.pointCollector.add(minX, maxY);
    }
    return parsed.freeze(GeometryType.ENVELOPE);
  }

  private static ParsedGeometry parseCoordinateSequenceGeometry(DataInput dataInput,
                                                                int dimension,
                                                                boolean measure,
                                                                GeometryType geometryType,
                                                                DetailLevel detailLevel) throws IOException {
    SequenceMetadata sequence = parseCoordinateSequence(dataInput, dimension, measure, detailLevel);
    if (sequence.numPoints == 0)
      return ParsedGeometry.empty();
    ParsedGeometry parsed = new ParsedGeometry(
        sequence.envelope,
        sequence.numPoints,
        geometryType,
        sequence.coordinateDimension,
        detailLevel == DetailLevel.EXACT);
    parsed.partCount = 1;
    parsed.pointCollector.addAll(sequence.pointCollector);
    return parsed.freeze(geometryType);
  }

  private static ParsedGeometry parsePolygon(DataInput dataInput,
                                             int dimension,
                                             boolean measure,
                                             DetailLevel detailLevel) throws IOException {
    int numRings = dataInput.readInt();
    if (numRings == 0)
      return ParsedGeometry.empty();
    MutableMetadata metadata = new MutableMetadata(detailLevel == DetailLevel.EXACT);
    for (int ringIndex = 0; ringIndex < numRings; ringIndex++) {
      SequenceMetadata ring = parseCoordinateSequence(dataInput, dimension, measure, detailLevel);
      metadata.includeRing(ring, ringIndex == 0);
    }
    return metadata.freeze(GeometryType.POLYGON);
  }

  private static ParsedGeometry parseCollection(DataInput dataInput,
                                               GeometryType collectionType,
                                               DetailLevel detailLevel) throws IOException {
    int numGeometries = dataInput.readInt();
    if (numGeometries == 0)
      return ParsedGeometry.empty();
    MutableMetadata metadata = new MutableMetadata(detailLevel == DetailLevel.EXACT);
    for (int geometryIndex = 0; geometryIndex < numGeometries; geometryIndex++)
      metadata.include(parse(dataInput, detailLevel));
    return metadata.freeze(collectionType);
  }

  private static SequenceMetadata parseCoordinateSequence(DataInput dataInput,
                                                          int dimension,
                                                          boolean measure,
                                                          DetailLevel detailLevel) throws IOException {
    int numPoints = dataInput.readInt();
    double[] minCoord = new double[dimension];
    double[] maxCoord = new double[dimension];
    boolean[] hasFinite = new boolean[dimension];
    for (int d = 0; d < dimension; d++) {
      minCoord[d] = Double.POSITIVE_INFINITY;
      maxCoord[d] = Double.NEGATIVE_INFINITY;
    }
    PointCollector pointCollector = new PointCollector(detailLevel == DetailLevel.EXACT);
    double firstX = Double.NaN;
    double firstY = Double.NaN;
    double prevX = Double.NaN;
    double prevY = Double.NaN;
    boolean hasPrevious = false;
    double signedAreaTwice = 0.0d;

    for (int pointIndex = 0; pointIndex < numPoints; pointIndex++) {
      double x = Double.NaN;
      double y = Double.NaN;
      for (int d = 0; d < dimension; d++) {
        double coordinate = dataInput.readDouble();
        if (d == 0)
          x = coordinate;
        else if (d == 1)
          y = coordinate;
        if (Double.isFinite(coordinate)) {
          hasFinite[d] = true;
          minCoord[d] = Math.min(minCoord[d], coordinate);
          maxCoord[d] = Math.max(maxCoord[d], coordinate);
        }
      }
      if (measure)
        dataInput.readDouble();

      if (Double.isFinite(x) && Double.isFinite(y)) {
      pointCollector.add(x, y);
        if (!hasPrevious) {
          firstX = x;
          firstY = y;
          hasPrevious = true;
        } else {
          signedAreaTwice += prevX * y - x * prevY;
        }
        prevX = x;
        prevY = y;
      }
    }

    if (hasPrevious)
      signedAreaTwice += prevX * firstY - firstX * prevY;

    int effectiveDimension = resolveEffectiveDimension(hasFinite);
    EnvelopeNDLite envelope = new EnvelopeNDLite(effectiveDimension);
    for (int d = 0; d < effectiveDimension; d++) {
      envelope.setMinCoord(d, minCoord[d]);
      envelope.setMaxCoord(d, maxCoord[d]);
    }
    return new SequenceMetadata(envelope, numPoints, effectiveDimension, 0.5d * signedAreaTwice, pointCollector);
  }

  private static int resolveEffectiveDimension(double[] coordinates) {
    int effectiveDimension = 0;
    for (int d = 0; d < coordinates.length; d++) {
      if (Double.isFinite(coordinates[d]))
        effectiveDimension = d + 1;
    }
    return Math.max(0, effectiveDimension);
  }

  private static int resolveEffectiveDimension(boolean[] hasFinite) {
    int effectiveDimension = 0;
    for (int d = 0; d < hasFinite.length; d++) {
      if (hasFinite[d])
        effectiveDimension = d + 1;
    }
    return Math.max(0, effectiveDimension);
  }

  private static Layout resolveLayout(int dimensionMarker) {
    switch (dimensionMarker) {
      case WKBConstants.wkbMarkerXY:
        return new Layout(2, false);
      case WKBConstants.wkbMarkerXYZ:
        return new Layout(3, false);
      case WKBConstants.wkbMarkerXYM:
        return new Layout(2, true);
      case WKBConstants.wkbMarkerXYZM:
        return new Layout(3, true);
      default:
        return new Layout(dimensionMarker / 1000 - 3, false);
    }
  }

  private static long baseGeometryBytes(GeometryType geometryType) {
    if (geometryType == null)
      return OBJECT_HEADER_BYTES;
    switch (geometryType) {
      case POINT:
        return 96L;
      case LINESTRING:
      case MULTIPOINT:
        return 128L;
      case POLYGON:
      case MULTILINESTRING:
        return 160L;
      case MULTIPOLYGON:
      case GEOMETRYCOLLECTION:
        return 192L;
      case ENVELOPE:
        return 64L;
      case EMPTY:
      default:
        return 32L;
    }
  }

  private static long align(long value) {
    if (value <= 0L)
      return 0L;
    long remainder = value % OBJECT_ALIGNMENT_BYTES;
    return remainder == 0L ? value : value + (OBJECT_ALIGNMENT_BYTES - remainder);
  }

  private static int safeToInt(long value) {
    if (value <= 0L)
      return 0;
    return (int) Math.min(Integer.MAX_VALUE, value);
  }

  private static double envelopeArea2D(EnvelopeNDLite envelope) {
    if (envelope == null || envelope.getCoordinateDimension() < 2 || envelope.isEmpty())
      return 0.0d;
    double width = Math.max(0.0d, envelope.getMaxCoord(0) - envelope.getMinCoord(0));
    double height = Math.max(0.0d, envelope.getMaxCoord(1) - envelope.getMinCoord(1));
    return width * height;
  }

  private static double computeConvexHullArea(PointCollector collector) {
    if (collector == null || collector.size() < 3)
      return 0.0d;
    List<PointXY> points = new ArrayList<>(collector.toList());
    points.sort(Comparator.comparingDouble((PointXY p) -> p.x).thenComparingDouble(p -> p.y));
    List<PointXY> unique = new ArrayList<>(points.size());
    PointXY previous = null;
    for (PointXY point : points) {
      if (previous == null || point.x != previous.x || point.y != previous.y) {
        unique.add(point);
        previous = point;
      }
    }
    if (unique.size() < 3)
      return 0.0d;

    List<PointXY> lower = new ArrayList<>();
    for (PointXY point : unique) {
      while (lower.size() >= 2 && cross(lower.get(lower.size() - 2), lower.get(lower.size() - 1), point) <= 0.0d)
        lower.remove(lower.size() - 1);
      lower.add(point);
    }

    List<PointXY> upper = new ArrayList<>();
    for (int i = unique.size() - 1; i >= 0; i--) {
      PointXY point = unique.get(i);
      while (upper.size() >= 2 && cross(upper.get(upper.size() - 2), upper.get(upper.size() - 1), point) <= 0.0d)
        upper.remove(upper.size() - 1);
      upper.add(point);
    }

    List<PointXY> hull = new ArrayList<>(lower.size() + upper.size() - 2);
    hull.addAll(lower);
    for (int i = 1; i < upper.size() - 1; i++)
      hull.add(upper.get(i));
    return Math.abs(signedArea(hull));
  }

  private static double cross(PointXY origin, PointXY a, PointXY b) {
    return (a.x - origin.x) * (b.y - origin.y) - (a.y - origin.y) * (b.x - origin.x);
  }

  private static double signedArea(List<PointXY> ring) {
    if (ring == null || ring.size() < 3)
      return 0.0d;
    double areaTwice = 0.0d;
    for (int i = 0; i < ring.size(); i++) {
      PointXY current = ring.get(i);
      PointXY next = ring.get((i + 1) % ring.size());
      areaTwice += current.x * next.y - next.x * current.y;
    }
    return 0.5d * areaTwice;
  }

  /** Immutable lightweight geometry metadata. */
  public static final class Metadata {
    private final EnvelopeNDLite envelope;
    private final int numPoints;
    private final GeometryType geometryType;
    private final int coordinateDimension;
    private final double exactArea;
    private final double convexHullArea;
    private final int partCount;
    private final int ringCount;
    private final int holeCount;

    private Metadata(EnvelopeNDLite envelope,
                     int numPoints,
                     GeometryType geometryType,
                     int coordinateDimension,
                     double exactArea,
                     double convexHullArea,
                     int partCount,
                     int ringCount,
                     int holeCount) {
      this.envelope = envelope == null ? new EnvelopeNDLite() : new EnvelopeNDLite(envelope);
      this.numPoints = Math.max(0, numPoints);
      this.geometryType = geometryType == null ? GeometryType.EMPTY : geometryType;
      this.coordinateDimension = Math.max(0, coordinateDimension);
      this.exactArea = Math.max(0.0d, exactArea);
      this.convexHullArea = Math.max(0.0d, convexHullArea);
      this.partCount = Math.max(0, partCount);
      this.ringCount = Math.max(0, ringCount);
      this.holeCount = Math.max(0, holeCount);
    }

    /** Returns empty geometry metadata. */
    public static Metadata empty() {
      return new Metadata(new EnvelopeNDLite(), 0, GeometryType.EMPTY, 0, 0.0d, 0.0d, 0, 0, 0);
    }

    public EnvelopeNDLite getEnvelope() {
      return new EnvelopeNDLite(envelope);
    }

    public int getNumPoints() {
      return numPoints;
    }

    public GeometryType getGeometryType() {
      return geometryType;
    }

    public int getCoordinateDimension() {
      return coordinateDimension;
    }

    /** Returns the exact 2D area parsed directly from the serialized geometry. */
    public double getExactArea() {
      return exactArea;
    }

    /** Returns the exact 2D area of the convex hull computed from serialized coordinates. */
    public double getConvexHullArea() {
      return convexHullArea;
    }

    /** Returns the 2D area of the serialized geometry MBR. */
    public double getEnvelopeArea2D() {
      return envelopeArea2D(envelope);
    }

    /** Returns the number of geometry parts represented by this metadata. */
    public int getPartCount() {
      return partCount;
    }

    /** Returns the number of polygon rings represented by this metadata. */
    public int getRingCount() {
      return ringCount;
    }

    /** Returns the number of interior polygon holes represented by this metadata. */
    public int getHoleCount() {
      return holeCount;
    }

    public boolean isEmpty() {
      return geometryType == GeometryType.EMPTY || envelope.getCoordinateDimension() == 0 || envelope.isEmpty();
    }
  }

  private static final class Layout {
    private final int dimension;
    private final boolean measure;

    private Layout(int dimension, boolean measure) {
      this.dimension = Math.max(0, dimension);
      this.measure = measure;
    }
  }

  private static final class PointXY {
    private final double x;
    private final double y;

    private PointXY(double x, double y) {
      this.x = x;
      this.y = y;
    }
  }

  private static final class PointCollector {
    private final boolean enabled;
    private final List<PointXY> points;

    private PointCollector() {
      this(true);
    }

    private PointCollector(boolean enabled) {
      this.enabled = enabled;
      this.points = enabled ? new ArrayList<>() : null;
    }

    private void add(double x, double y) {
      if (enabled && Double.isFinite(x) && Double.isFinite(y))
        points.add(new PointXY(x, y));
    }

    private void addAll(PointCollector other) {
      if (enabled && other != null && other.enabled && !other.points.isEmpty())
        points.addAll(other.points);
    }

    private int size() {
      return enabled ? points.size() : 0;
    }

    private List<PointXY> toList() {
      return enabled ? points : java.util.Collections.emptyList();
    }
  }

  private static final class SequenceMetadata {
    private final EnvelopeNDLite envelope;
    private final int numPoints;
    private final int coordinateDimension;
    private final double signedArea;
    private final PointCollector pointCollector;

    private SequenceMetadata(EnvelopeNDLite envelope,
                             int numPoints,
                             int coordinateDimension,
                             double signedArea,
                             PointCollector pointCollector) {
      this.envelope = envelope;
      this.numPoints = Math.max(0, numPoints);
      this.coordinateDimension = Math.max(0, coordinateDimension);
      this.signedArea = signedArea;
      this.pointCollector = pointCollector == null ? new PointCollector() : pointCollector;
    }
  }

  private static final class ParsedGeometry {
    private EnvelopeNDLite envelope;
    private int numPoints;
    private GeometryType geometryType = GeometryType.EMPTY;
    private int coordinateDimension;
    private double exactArea;
    private double convexHullArea;
    private int partCount;
    private int ringCount;
    private int holeCount;
    private final PointCollector pointCollector;

    private ParsedGeometry() {
      this.pointCollector = new PointCollector(false);
    }

    private ParsedGeometry(EnvelopeNDLite envelope,
                           int numPoints,
                           GeometryType geometryType,
                           int coordinateDimension,
                           boolean collectHullPoints) {
      this.envelope = envelope == null ? new EnvelopeNDLite() : new EnvelopeNDLite(envelope);
      this.numPoints = Math.max(0, numPoints);
      this.geometryType = geometryType == null ? GeometryType.EMPTY : geometryType;
      this.coordinateDimension = Math.max(0, coordinateDimension);
      this.pointCollector = new PointCollector(collectHullPoints);
    }

    private static ParsedGeometry empty() {
      return new ParsedGeometry();
    }

    private ParsedGeometry freeze(GeometryType targetType) {
      if (envelope == null || envelope.getCoordinateDimension() == 0 || envelope.isEmpty())
        return empty();
      geometryType = targetType == null ? geometryType : targetType;
      convexHullArea = computeConvexHullArea(pointCollector);
      exactArea = Math.max(0.0d, exactArea);
      partCount = Math.max(0, partCount);
      ringCount = Math.max(0, ringCount);
      holeCount = Math.max(0, holeCount);
      return this;
    }

    private Metadata toMetadata() {
      if (envelope == null || envelope.getCoordinateDimension() == 0 || envelope.isEmpty())
        return Metadata.empty();
      return new Metadata(envelope, numPoints, geometryType, coordinateDimension, exactArea, convexHullArea, partCount, ringCount, holeCount);
    }
  }

  private static final class MutableMetadata {
    private EnvelopeNDLite envelope;
    private int numPoints;
    private GeometryType geometryType = GeometryType.EMPTY;
    private int coordinateDimension;
    private double exactArea;
    private int partCount;
    private int ringCount;
    private int holeCount;
    private final PointCollector pointCollector;

    private MutableMetadata(boolean collectHullPoints) {
      this.pointCollector = new PointCollector(collectHullPoints);
    }

    private void include(ParsedGeometry other) {
      if (other == null || other.envelope == null || other.envelope.getCoordinateDimension() == 0 || other.envelope.isEmpty())
        return;
      if (envelope == null || envelope.getCoordinateDimension() == 0 || envelope.isEmpty())
        envelope = new EnvelopeNDLite(other.envelope);
      else
        envelope.merge(other.envelope);
      numPoints += Math.max(0, other.numPoints);
      geometryType = geometryType.coerce(other.geometryType == null ? GeometryType.EMPTY : other.geometryType);
      coordinateDimension = Math.max(coordinateDimension, other.coordinateDimension);
      exactArea += Math.max(0.0d, other.exactArea);
      partCount += Math.max(0, other.partCount);
      ringCount += Math.max(0, other.ringCount);
      holeCount += Math.max(0, other.holeCount);
      pointCollector.addAll(other.pointCollector);
    }

    private void includeRing(SequenceMetadata ring, boolean shell) {
      if (ring == null || ring.envelope == null || ring.envelope.getCoordinateDimension() == 0 || ring.envelope.isEmpty())
        return;
      if (envelope == null || envelope.getCoordinateDimension() == 0 || envelope.isEmpty())
        envelope = new EnvelopeNDLite(ring.envelope);
      else
        envelope.merge(ring.envelope);
      numPoints += Math.max(0, ring.numPoints);
      coordinateDimension = Math.max(coordinateDimension, ring.coordinateDimension);
      pointCollector.addAll(ring.pointCollector);
      ringCount += 1;
      if (shell) {
        partCount += 1;
        exactArea += Math.abs(ring.signedArea);
      } else {
        holeCount += 1;
        exactArea -= Math.abs(ring.signedArea);
      }
      geometryType = geometryType.coerce(GeometryType.POLYGON);
    }

    private ParsedGeometry freeze(GeometryType targetType) {
      ParsedGeometry parsed = new ParsedGeometry();
      parsed.envelope = envelope == null ? new EnvelopeNDLite() : new EnvelopeNDLite(envelope);
      parsed.numPoints = numPoints;
      parsed.geometryType = targetType == null ? geometryType : targetType;
      parsed.coordinateDimension = coordinateDimension;
      parsed.exactArea = Math.max(0.0d, exactArea);
      parsed.partCount = partCount;
      parsed.ringCount = ringCount;
      parsed.holeCount = holeCount;
      parsed.pointCollector.addAll(pointCollector);
      return parsed.freeze(parsed.geometryType);
    }
  }
}
