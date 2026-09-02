/*
 * Copyright 2018 University of California, Riverside
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
package cn.edu.whu.spatialJoin.io.reader;

import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.geom.*;
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper;
import cn.edu.whu.spatialJoin.sql.GeometryDataType$;
import cn.edu.whu.spatialJoin.utils.OperationParam;
import cn.edu.whu.spatialJoin.utils.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * A record reader that reads CSV file with custom field delimiter
 */
@SpatialReaderMetadata(
    description = "Parses comma- or tab-separated text files which contains either point coordinates," +
        "envelope boundaries or WKT-encoded geometries",
    shortName = "csv",
    extension = ".csv"
)
public class CSVFeatureReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(CSVFeatureReader.class);
  /** Common WKT geometry column names recognized when callers use plain {@code wkt} with a header row. */
  private static final String[] DefaultGeometryColumnNames = new String[] {"geometry", "geom", "shape", "the_geom", "wkt"};

  /**Default quote characters including single and double quote characters*/
  public static final String DefaultQuoteCharacters = "''\"\"";

  @OperationParam(
      description = "The field separator for text files. Defaults to the tab character",
      defaultValue = "\t"
  )
  public static final String FieldSeparator = "separator";
  @OperationParam(
      description = "When reading a CSV file, skip the first line in the file. {true, false}",
      defaultValue = "false"
  )
  public static final String SkipHeader = "skipheader";

  @OperationParam(
      description = "Characters used to quote fields. Every pair of characters are used as start and end quote characters.",
      defaultValue = "''\"\""
  )
  public static final String QuoteCharacters = "quotes";

  @OperationParam(
      description = "Match geometry column names with case sensitivity (upper and lower cases matter)",
      defaultValue = "false",
      showInUsage = false
  )
  public static final String CaseSensitiveHeader = "csv.casesensitive";

  /**An underlying reader to read the input line-by-line*/
  protected final LineRecordReader lineReader = new LineRecordReader();

  /**Directs the record reader to skip the first line in the file*/
  protected boolean skipHeaderLine;

  /**The CSV feature*/
  protected Feature feature;

  /**A single character for separating fields in the input*/
  protected char fieldSeparator;

  /**A list of quote characters*/
  protected String quotes;

  /**The names or indexes of the columns where geometry WKT or coordinates are encoded*/
  protected String[] coordinateColumns;

  /**The index of columns that contain the coordinates in order*/
  protected int[] coordinateColumnIndexes;

  /**Type of geometry (TODO use an enumerated type instead)*/
  protected String geometryType;

  /**If the input contains wkt-encoded geometries, this is used to parse them*/
  protected WKTReader wktReader;

  /**An optional attributed to filter the geometries in the input file*/
  private EnvelopeND filterMBR;

  /**Path of the input file. Used mainly for reporting errors.*/
  protected Path filePath;

  /**Schema for features including the geometry attribute*/
  protected StructType schema;

  /**Match header columns with case sensitivity*/
  private boolean caseSensitive;

  /**List of attribute names as they appear in the header line*/
  private String[] fieldNames;

  /**
   * Whether the current CSV source effectively behaves as a single WKT column where the full line
   * should be parsed as geometry text rather than being split by the configured separator.
   */
  private boolean wholeLineWktPreferred;
  /** Whether the current record consumed the full input line as WKT. */
  private boolean currentRecordUsesWholeLineWkt;
  /** Cached first record when a split was initially treated as headered but later downgraded to data. */
  private Text pendingFirstDataLine;

  /**
   * Cached schemas for each length of a record to avoid recreating many schemas.
   */
  private final Map<Integer, StructType> cachedSchemas = new HashMap<>();

  public CSVFeatureReader() {
    // TODO take geometry factory configuration from the UserOptions
    // NOTE Using PackedArrayCoordinateSequence breaks the code since it uses 3 dimensions by default even if the
    // code explicitly creates two dimensions only
    this.wktReader = new WKTReader(DefaultGeometryFactory);
    // Leaving this flag as true results in returning CoordinateSequence of dimension 3 even if the input
    // has only two dimensions
    //this.wktReader.setIsOldJtsCoordinateSyntaxAllowed(false);
  }

  @Override
  /**
   * Initializes one CSV reader instance for a split and resolves all parsing metadata needed later.
   *
   * <p>The initialization phase performs the following work in order:
   * <ol>
   *   <li>Open the underlying line reader on the requested split.</li>
   *   <li>Read delimiter, quoting, and case-sensitivity options.</li>
   *   <li>Parse the user-facing geometry format hint such as {@code wkt(0)} or {@code point(2,3)}.</li>
   *   <li>If {@code skipheader=true}, load the header row and keep the attribute names.</li>
   *   <li>Resolve which CSV columns represent the geometry and remove those columns from the
   *       remaining attribute-name list.</li>
   *   <li>Optionally parse a filter MBR so later calls to {@link #nextKeyValue()} can skip rows
   *       outside the requested extent.</li>
   * </ol>
   *
   * <p>No feature objects are created here yet; this method only prepares enough metadata so every
   * later CSV row can be parsed deterministically and consistently across splits.
   *
   * @param inputSplit the Hadoop split to read from.
   * @param conf the spatial-join options whose CSV settings drive delimiter, quoting, header, and
   *             geometry-column resolution.
   * @throws IOException if the split or header cannot be opened and parsed.
   */
  public void initialize(InputSplit inputSplit, SpatialJoinOptions conf) throws IOException {
    Configuration hadoopConf = conf.loadIntoHadoopConf(null);
    lineReader.initialize(inputSplit, new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID()));
    if (inputSplit instanceof FileSplit)
      filePath = ((FileSplit)inputSplit).getPath();
    this.pendingFirstDataLine = null;
    this.fieldSeparator = conf.getString(FieldSeparator, "\t").charAt(0);
    this.quotes = conf.getString(QuoteCharacters, "''\"\"");
    this.caseSensitive = conf.getBoolean(CaseSensitiveHeader, false);
    configureGeometryColumnsFromFormat(conf.getString(SpatialFileRDD.InputFormat()));
    this.skipHeaderLine = conf.getBoolean(SkipHeader, false);
    ArrayList<String> attributeNames = null;
    if (this.skipHeaderLine) {
      Text headerLine;
      attributeNames = new ArrayList<>();
      if (inputSplit instanceof FileSplit && ((FileSplit)inputSplit).getStart() != 0) {
        // Must read the header line separately
        FileSplit fsplit = (FileSplit) inputSplit;
        // TODO read the header line as the file splits are generated and propagate it to the record readers
        try (LineRecordReader otherReader = new LineRecordReader()) {
          FileSplit otherFileSplit = new FileSplit(fsplit.getPath(), 0, 1, null);
          otherReader.initialize(otherFileSplit, new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID()));
          if (!otherReader.nextKeyValue())
            throw new RuntimeException("Could not read header line from split " + otherFileSplit);
          headerLine = otherReader.getCurrentValue();
        }
      } else {
        // Reading first split, the header line is the first line in the input
        lineReader.nextKeyValue();
        headerLine = lineReader.getCurrentValue();
      }
      String originalHeaderLine = headerLine.toString();
      // Extract attribute names from the header line
      while (headerLine.getLength() > 0)
        attributeNames.add(normalizeHeaderField(deleteAttribute(headerLine, fieldSeparator, 0, quotes)));
      if (shouldTreatConfiguredHeaderAsData(originalHeaderLine, attributeNames)) {
        LOG.warn(String.format("CSV file '%s' was configured with skipHeader=true, but the first line looks like WKT " +
            "data. Falling back to headerless parsing for this reader.", filePath));
        attributeNames = null;
        this.skipHeaderLine = false;
        if (!(inputSplit instanceof FileSplit) || ((FileSplit) inputSplit).getStart() == 0)
          this.pendingFirstDataLine = new Text(originalHeaderLine);
      }
    }
    setCoordinateColumnIndexes(attributeNames);
    this.wholeLineWktPreferred = detectWholeLineWktPreference(attributeNames);
    this.currentRecordUsesWholeLineWkt = false;
    String filterMBRStr = conf.getString(SpatialFileRDD.FilterMBR());
    if (filterMBRStr != null) {
      String[] parts = filterMBRStr.split(",");
      double[] dblParts = new double[parts.length];
      for (int i = 0; i < parts.length; i++)
        dblParts[i] = Double.parseDouble(parts[i]);
      this.filterMBR = new EnvelopeND(DefaultGeometryFactory, dblParts.length/2, dblParts);
    }
    cachedSchemas.clear();
  }

  /**
   * Resolves the geometry columns either from explicit numeric indexes or from
   * header names when the CSV carries a header row.
   *
   * <p>The method fills {@code coordinateColumnIndexes} so later row parsing can extract geometry
   * values without repeatedly reinterpreting the user-supplied format string. It supports:
   * <ul>
   *   <li>explicit column numbers such as {@code wkt(0)}</li>
   *   <li>explicit header names such as {@code wkt(geometry)}</li>
   *   <li>implicit auto-detection such as plain {@code wkt} with a header line</li>
   * </ul>
   *
   * <p>When a header is available, geometry columns are also removed from {@code columnNames} so the
   * remaining names line up with the non-geometry attribute values appended later in
   * {@link #nextKeyValue()}.
   *
   * @param columnNames the header columns, or {@code null} when the input has no header row.
   */
  protected void setCoordinateColumnIndexes(ArrayList<String> columnNames) {
    // Initialize coordinate column indexes by either parsing the names as integer or locating them in the header
    coordinateColumnIndexes = new int[coordinateColumns.length];
    for (int $i = 0; $i < coordinateColumnIndexes.length; $i++) {
      if (coordinateColumns[$i] == null) {
        // If nothing is set by the user, first try to infer a geometry column name from the header for WKT inputs.
        int inferredColumnIndex = inferImplicitGeometryColumnIndex(columnNames, $i);
        coordinateColumnIndexes[$i] = inferredColumnIndex >= 0 ? inferredColumnIndex : ($i == 0 ? 0 : coordinateColumnIndexes[$i - 1] + 1);
      } else {
        try {
          // Try to parse it as integer
          coordinateColumnIndexes[$i] = Integer.parseInt(coordinateColumns[$i]);
        } catch (NumberFormatException e) {
          if (columnNames == null)
            throw new RuntimeException(
                String.format("Attribute '%s' is not an integer and -skipheader is not set in file '%s'",
                    coordinateColumns[$i], filePath));
          // If it's not an integer, it could be a column name

          if (caseSensitive)
            coordinateColumnIndexes[$i] = columnNames.indexOf(coordinateColumns[$i]);
          else {
            // Compare with case insensitivity
            int columnIndex = 0;
            while (columnIndex < columnNames.size() && !columnNames.get(columnIndex).equalsIgnoreCase(coordinateColumns[$i]))
              columnIndex++;
            coordinateColumnIndexes[$i] = columnIndex < columnNames.size() ? columnIndex : -1;
          }
          if (coordinateColumnIndexes[$i] == -1)
            throw new RuntimeException(String.format("Column '%s' not found in the header line of file '%s'",
                coordinateColumns[$i], filePath));
        }
      }
    }
    // Adjust coordinate column indexes to be in the correct order to parse
    adjustCoordinateColumnIndexes(this.coordinateColumnIndexes, columnNames);
    this.fieldNames = columnNames != null? columnNames.toArray(new String[0]) : null;
  }

  /**
   * Detects the common case where the file contains only one geometry column and the configured
   * separator would otherwise split the WKT text itself.
   */
  private boolean detectWholeLineWktPreference(List<String> originalHeaderColumns) {
    if (!"wkt".equalsIgnoreCase(this.geometryType))
      return false;
    if (coordinateColumnIndexes == null || coordinateColumnIndexes.length != 1 || coordinateColumnIndexes[0] != 0)
      return false;
    if (fieldNames != null && fieldNames.length > 0)
      return false;
    return originalHeaderColumns != null && originalHeaderColumns.size() == 1;
  }

  /** Normalizes one header field and preserves empty columns without crashing the reader. */
  private String normalizeHeaderField(String headerValue) {
    return headerValue == null ? "" : headerValue.trim();
  }

  /**
   * Detects a common misconfiguration where callers enable {@code skipHeader=true} for a CSV file that
   * actually starts directly with WKT data.
   */
  private boolean shouldTreatConfiguredHeaderAsData(String originalHeaderLine, List<String> parsedHeaderColumns) {
    if (!this.skipHeaderLine || !"wkt".equalsIgnoreCase(this.geometryType) || originalHeaderLine == null)
      return false;
    Text probeLine = new Text(originalHeaderLine);
    String firstField = deleteAttribute(probeLine, this.fieldSeparator, 0, quotes);
    if (looksLikeWholeLineWkt(firstField))
      return true;
    if (!looksLikeWholeLineWkt(originalHeaderLine))
      return false;
    if (parsedHeaderColumns == null || parsedHeaderColumns.isEmpty())
      return true;
    for (String parsedHeaderColumn : parsedHeaderColumns) {
      if (parsedHeaderColumn != null && !parsedHeaderColumn.isEmpty())
        return false;
    }
    return true;
  }

  /** Parses WKT either from the extracted geometry field or, when appropriate, from the whole line. */
  private Geometry parseWktGeometry(Text line) {
    String rawLine = line == null ? null : line.toString();
    String extractedWkt = deleteAttribute(line, this.fieldSeparator, this.coordinateColumnIndexes[0], quotes);
    if (wholeLineWktPreferred) {
      currentRecordUsesWholeLineWkt = true;
      return parseWkt(rawLine, rawLine, true);
    }
    if (extractedWkt == null)
      return EmptyGeometry.instance;
    try {
      currentRecordUsesWholeLineWkt = false;
      return parseWkt(extractedWkt, rawLine, false);
    } catch (RuntimeException primaryFailure) {
      if (looksLikeWholeLineWkt(rawLine) && coordinateColumnIndexes != null && coordinateColumnIndexes.length == 1 &&
          coordinateColumnIndexes[0] == 0 && (fieldNames == null || fieldNames.length == 0)) {
        currentRecordUsesWholeLineWkt = true;
        return parseWkt(rawLine, rawLine, true);
      }
      throw primaryFailure;
    }
  }

  /** Parses one WKT string into a geometry while preserving the original line for diagnostics. */
  private Geometry parseWkt(String wkt, String originalLine, boolean wholeLineMode) {
    if (wkt == null)
      return EmptyGeometry.instance;
    try {
      return wktReader.read(wkt);
    } catch (ParseException e) {
      throw new RuntimeException(
          String.format("Error parsing line '%s' wkt '%s' in file '%s'%s",
              originalLine,
              wkt,
              filePath,
              wholeLineMode ? " (whole-line WKT mode)" : ""),
          e);
    }
  }

  /** Lightweight check for lines that already look like full WKT payloads. */
  private boolean looksLikeWholeLineWkt(String lineText) {
    if (lineText == null)
      return false;
    String trimmed = lineText.trim();
    return trimmed.regionMatches(true, 0, "POINT", 0, "POINT".length()) ||
        trimmed.regionMatches(true, 0, "LINESTRING", 0, "LINESTRING".length()) ||
        trimmed.regionMatches(true, 0, "POLYGON", 0, "POLYGON".length()) ||
        trimmed.regionMatches(true, 0, "MULTIPOINT", 0, "MULTIPOINT".length()) ||
        trimmed.regionMatches(true, 0, "MULTILINESTRING", 0, "MULTILINESTRING".length()) ||
        trimmed.regionMatches(true, 0, "MULTIPOLYGON", 0, "MULTIPOLYGON".length()) ||
        trimmed.regionMatches(true, 0, "GEOMETRYCOLLECTION", 0, "GEOMETRYCOLLECTION".length());
  }

  /**
   * Infers an implicit geometry column from a header row when the caller uses
   * plain {@code wkt} without naming or indexing the geometry field.
   */
  private int inferImplicitGeometryColumnIndex(List<String> columnNames, int coordinateOrdinal) {
    if (coordinateOrdinal != 0 || columnNames == null || !"wkt".equalsIgnoreCase(this.geometryType))
      return -1;
    for (String geometryColumnName : DefaultGeometryColumnNames) {
      int columnIndex = findColumnIndex(columnNames, geometryColumnName);
      if (columnIndex >= 0)
        return columnIndex;
    }
    return -1;
  }

  /** Finds a header column by name while honoring the configured case-sensitivity mode. */
  private int findColumnIndex(List<String> columnNames, String desiredName) {
    if (columnNames == null || desiredName == null)
      return -1;
    for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
      String columnName = columnNames.get(columnIndex);
      if (columnName == null)
        continue;
      if (caseSensitive) {
        if (columnName.equals(desiredName))
          return columnIndex;
      } else if (columnName.equalsIgnoreCase(desiredName)) {
        return columnIndex;
      }
    }
    return -1;
  }

  /**
   * An initializer that can be used outside the regular MapReduce context.
   * @param inputFile the input file name
   * @param conf the system configuration
   * @throws IOException if an error happens while trying to open the input file
   */
  public void initialize(Path inputFile, SpatialJoinOptions conf) throws IOException {
    FileSystem fileSystem = inputFile.getFileSystem(conf.loadIntoHadoopConf(null));
    FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
    FileSplit fileSplit = new FileSplit(inputFile, 0, fileStatus.getLen(), null);
    this.initialize(fileSplit, conf);
  }

  /**
   * Infers the geometry type and column indexes from the given user-friendly format
   * @param userFriendlyFormat the user-given input format
   */
  protected void configureGeometryColumnsFromFormat(String userFriendlyFormat) {
    parseGeometryColumnsFromFormat(userFriendlyFormat);
  }

  /** Backward-compatible alias kept for older subclasses. */
  protected void setGeometryTypeAndColumnIndexes(String userFriendlyFormat) {
    configureGeometryColumnsFromFormat(userFriendlyFormat);
  }

  /**
   * Parses formats such as {@code wkt(1)}, {@code wkt(geometry)}, {@code wkt},
   * or {@code point(0,1)} into the geometry-type label plus the CSV column
   * positions that encode geometry data.
   */
  protected void parseGeometryColumnsFromFormat(String userFriendlyFormat) {
    if (!caseSensitive)
      userFriendlyFormat = userFriendlyFormat.toLowerCase();
    String[] args; // The arguments between the parentheses or an empty array if not parentheses
    int openParenthesis = userFriendlyFormat.indexOf('(');
    int closeParenthesis = userFriendlyFormat.indexOf(')', openParenthesis + 1);

    if (openParenthesis != -1) {
      geometryType = userFriendlyFormat.substring(0, openParenthesis).toLowerCase();
      args = userFriendlyFormat.substring(openParenthesis + 1, closeParenthesis).split(",");
    } else {
      geometryType = userFriendlyFormat.toLowerCase();
      args = new String[0];
    }
    // The indexes of the columns that contain the geometry
    int numCoords;
    int skipFirst = 0;
    switch (this.geometryType) {
      case "point":
        // A two-dimensional point
        numCoords = 2;
        break;
      case "envelope":
        // A two-dimensional envelope
        numCoords = 4;
        break;
      case "pointk":
        // A k-dimensional point where the first integer indicates the number of dimensions
        this.geometryType = "point";
        numCoords = Integer.parseInt(args[0]);
        skipFirst = 1;
        break;
      case "envelopek":
        // A k-dimensional envelope where the first integer indicates the number of dimensions
        this.geometryType = "envelope";
        numCoords = Integer.parseInt(args[0]) * 2;
        skipFirst = 1;
        break;
      case "wkt":
        this.geometryType = "wkt";
        numCoords = 1;
        break;
      case "nogeom":
        this.geometryType = "nogeom";
        numCoords = 0;
        break;
      default:
        throw new RuntimeException("Unrecognized geometry type "+this.geometryType);
    }
    this.coordinateColumns = new String[numCoords];
    System.arraycopy(args, skipFirst, this.coordinateColumns, 0, args.length - skipFirst);
  }

  @Override
  public boolean isRecognized(String iformat) {
    String geometryType;
    int openParenthesis = iformat.indexOf('(');

    if (openParenthesis != -1) {
      geometryType = iformat.substring(0, openParenthesis).toLowerCase();
    } else {
      geometryType = iformat.toLowerCase();
    }
    // TODO Include these in a set to make it easier to test
    switch (geometryType) {
      case "point":
      case "pointk":
      case "envelope":
      case "envelopek":
      case "wkt":
      case "nogeom":
        return true;
      default:
        return false;
    }
  }

  /** Suggests close geometry-format spellings when the caller mistypes the CSV geometry format. */
  @Override
  public String[] iformatCorrections(String iformat) {
    int indexOfParanthesis = iformat.indexOf('(');
    if (indexOfParanthesis != -1)
      iformat = iformat.substring(0, indexOfParanthesis);
    iformat = iformat.toLowerCase();
    List<String> corrections = new ArrayList<>();
    corrections.addAll(Arrays.asList("point", "pointk", "envelope", "envelopek", "wkt", "nogeom"));
    int i = 0;
    while (i < corrections.size()) {
      if (StringUtil.levenshteinDistance(iformat, corrections.get(i)) <= 2)
        i++;
      else
        corrections.remove(i);
    }
    return corrections.isEmpty()? null : corrections.toArray(new String[0]);
  }

  /** Static shortcut used by auto-detection and tests to validate a CSV geometry format label. */
  public static boolean recognize(String iformat) {
    String geometryType;
    int openParenthesis = iformat.indexOf('(');

    if (openParenthesis != -1) {
      geometryType = iformat.substring(0, openParenthesis).toLowerCase();
    } else {
      geometryType = iformat.toLowerCase();
    }
    // TODO Include these in a set to make it easier to test
    switch (geometryType) {
      case "point":
      case "pointk":
      case "envelope":
      case "envelopek":
      case "wkt":
      case "nogeom":
        return true;
      default:
        return false;
    }
  }

  @Override
  /**
   * Reads one CSV record, parses its geometry according to the configured format,
   * and materializes a binary-geometry `Feature`.
   *
   * Geometry bytes are serialized here so the downstream pipeline can keep the
   * feature in its lazy binary form until a later computation explicitly asks
   * for `getGeometry()`.
   *
   * <p>For every input line, the method:
   * <ol>
   *   <li>extracts the geometry columns according to the resolved format,</li>
   *   <li>builds a JTS geometry object for the current row,</li>
   *   <li>optionally skips the row when its geometry MBR falls outside {@code filterMBR},</li>
   *   <li>serializes the geometry into the project's binary geometry format, and</li>
   *   <li>packs geometry plus remaining attributes into a fresh immutable {@link Feature}.</li>
   * </ol>
   *
   * <p>The returned feature always stores its geometry in binary form at this stage. This is why
   * later partition construction and execution can carry the row around cheaply and only deserialize
   * geometry on demand.
   *
   * @return {@code true} when a valid record was parsed into {@link #feature}, or {@code false}
   *         after the split is exhausted.
   */
  public boolean nextKeyValue() throws IOException {
    boolean recordExists;
    Text line;
    if (pendingFirstDataLine != null) {
      line = pendingFirstDataLine;
      pendingFirstDataLine = null;
      recordExists = true;
    } else {
      recordExists = lineReader.nextKeyValue();
      if (!recordExists)
        return false;
      line = lineReader.getCurrentValue();
    }
    if (!recordExists)
      return false;
    // Parse the next line into CSV and skip if it does not overlap the filter MBR
    // Read the geometry part
    while (recordExists) {
      currentRecordUsesWholeLineWkt = false;
      Geometry geom;
      switch (this.geometryType) {
        case "nogeom":
          geom = EmptyGeometry.instance;
          break;
        case "wkt":
          geom = parseWktGeometry(line);
          break;
        case "point":
          double[] coords = new double[this.coordinateColumnIndexes.length];
          Arrays.fill(coords, Double.NaN);
          for (int iCoord = 0; iCoord < this.coordinateColumnIndexes.length; iCoord++) {
            String val = deleteAttribute(line, this.fieldSeparator, this.coordinateColumnIndexes[iCoord], quotes);
            try {
              coords[iCoord] = (val == null || val.length() == 0) ? Double.NaN : Double.parseDouble(val);
            } catch (NumberFormatException e) {
              throw new RuntimeException(String.format("Error parsing dimension #%d column #%d, value '%s', text line '%s' in file '%s'",
                  iCoord, this.coordinateColumnIndexes[iCoord], val, line, filePath), e);
            }
          }
          if (coords.length == 2) {
            if (Double.isNaN(coords[0]) || Double.isNaN(coords[1]))
              geom = DefaultGeometryFactory.createPoint();
            else
              geom = DefaultGeometryFactory.createPoint(new CoordinateXY(coords[0], coords[1]));
          } else
            geom = new PointND(DefaultGeometryFactory, coords);
          break;
        case "envelope":
          EnvelopeND env = new EnvelopeND(DefaultGeometryFactory, this.coordinateColumnIndexes.length / 2);
          for (int iCoord = 0; iCoord < this.coordinateColumnIndexes.length; iCoord++) {
            String val = deleteAttribute(line, this.fieldSeparator, this.coordinateColumnIndexes[iCoord], quotes);
            try {
              double coordValue = (val == null || val.length() == 0) ? Double.NaN : Double.parseDouble(val);
              if (iCoord < env.getCoordinateDimension())
                env.setMinCoord(iCoord % env.getCoordinateDimension(), coordValue);
              else
                env.setMaxCoord(iCoord % env.getCoordinateDimension(), coordValue);
            } catch (NumberFormatException e) {
              throw new RuntimeException(String.format("Error parsing dimension #%d column #%d, value '%s', text line '%s' in file '%s'",
                  iCoord, this.coordinateColumnIndexes[iCoord], val, line, filePath), e);
            }
          }
          geom = env.getCoordinateDimension() == 2 ? env.getEnvelope() : env;
          break;
        default:
          throw new RuntimeException("Unrecognized geometry type "+this.geometryType);
      }
      // Create a new value since Spark cannot work with mutable objects
      EnvelopeND geometryMBR = new EnvelopeND(geom.getFactory()).merge(geom);
      if (filterMBR == null || filterMBR.intersectsEnvelope(geometryMBR)) {
        List<Object> values = new ArrayList<>();
        values.add(GeometryDataType$.MODULE$.serialize(geom));
        if (!currentRecordUsesWholeLineWkt) {
          while (line.getLength() > 0) {
            String v = deleteAttribute(line, fieldSeparator, 0, quotes);
            if (v != null)
              v = v.replaceAll("\\\\n", "\n");
            values.add(v);
          }
        }
        // Append null values to match the length of the names
        while (fieldNames != null && values.size() < fieldNames.length)
          values.add(null);
        StructType schema = getOrCreateSchema(values.size());
        this.feature = new Feature(values.toArray(), schema);
        return true;
      }
      // Skip to the next record
      recordExists = lineReader.nextKeyValue();
      if (recordExists)
        line = lineReader.getCurrentValue();
    }
    return false;
  }

  /**
   * Fetches or creates a schema of the given length. If an existing schema has been created earlier, it will
   * be returned. Otherwise, it will create a new schema. The created schema will always contain the given number
   * of attributes. The names will be taken from the header line if a corresponding entry exists. Otherwise,
   * a place holder $i will be added where i is the index of the attribute. The geometry will always have the name g.
   * @param length the number of attributes in the schema.
   * @return a schema of the given length.
   */
  /** Returns a cached schema for one CSV row width, creating it on first use. */
  private StructType getOrCreateSchema(int length) {
    StructType schema = cachedSchemas.get(length);
    if (schema == null) {
      StructField[] attributes = new StructField[length];
      attributes[0] = new StructField("g", GeometryHelper.GeometryType, true, null);
      for (int i = 1; i < length; i++) {
        attributes[i] = new StructField(fieldNames != null && fieldNames.length > i-1? fieldNames[i-1] : "$"+i, DataTypes.StringType, true, null);
      }
      schema = new StructType(attributes);
      cachedSchemas.put(length, schema);
    }
    return schema;
  }

  /**
   * Adjust the coordinate column indexes so that they can be retrieved from a line in order.
   * If a coordinate index is followed by another index that appears after it, then this function
   * reduces the value of the second coordinate. For example, if the input is
   * [1, 2, 3], the output is [1, 1, 1], because removing attributes in this order from an input line
   * will produce the correct result. In other words, after removing attribute #1, then attribute #2 is now #1.
   * Similarly, after removing the second attribute, the third attribute also becomes at position #1.
   * On the other hand, if the input is [3, 2, 1], the output is also [3, 2, 1] because removing the first attribute
   * does not modify the position of the other two attributes because they appear earlier in the line.
   * @param coordinateColumnIndexes array of coordinate column indexes to adjust
   * @param names names in the header lines to adjust accordingly, i.e., remove coordinate columns
   */
  protected static void adjustCoordinateColumnIndexes(int[] coordinateColumnIndexes, List<String> names) {
    for (int $i = 0; $i < coordinateColumnIndexes.length; $i++) {
      if (names != null && names.size() > coordinateColumnIndexes[$i])
        names.remove(coordinateColumnIndexes[$i]);
      for (int $j = $i + 1; $j < coordinateColumnIndexes.length; $j++) {
        if (coordinateColumnIndexes[$j] > coordinateColumnIndexes[$i])
          coordinateColumnIndexes[$j]--;
      }
    }
  }

  @Override
  public IFeature getCurrentValue() {
    return feature;
  }

  @Override
  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }

  /**
   * Holds some meta information about columns that can be used to autodetect the format of an input shape
   */
  protected static class ColumnMetadata {
    enum ColumnType {Numeric, WKT, String}
    /**Name of the column or its index if the input file contains no names*/
    String name;
    /**The type of the column*/
    ColumnType type;
    /**Only for numeric columns, the range of values for that column*/
    double minValue, maxValue;

    public ColumnMetadata() {
      // Start with the most restrictive type
      this.type = ColumnType.Numeric;
      // Initialize the range to inverse infinite range
      this.minValue = Double.POSITIVE_INFINITY;
      this.maxValue = Double.NEGATIVE_INFINITY;
    }
  }

  @Override
  /**
   * Tries to infer a usable CSV geometry format by sampling the beginning of the file.
   *
   * The detector looks for a stable field separator first, then inspects column
   * names and values to distinguish between WKT and coordinate-column layouts.
   */
  public SpatialJoinOptions autoDetect(SpatialJoinOptions conf, String input) {
    // Detect by reading the first few lines of the input;
    try {
      Path inputPath = new Path(input);
      Configuration hadoopConf = conf.loadIntoHadoopConf(null);
      FileSystem fs = inputPath.getFileSystem(hadoopConf);
      FileStatus fileStatus = fs.getFileStatus(inputPath);
      if (fileStatus.isDirectory()) {
        FileStatus[] contents = fs.listStatus(inputPath, SpatialFileRDD.HiddenFileFilter());
        if (contents.length == 0)
          return null;
        fileStatus = contents[0];
      }
      long length = fileStatus.getLen();
      FileSplit split = new FileSplit(fileStatus.getPath(), 0, Math.min(length, 8192), new String[0]);
      lineReader.initialize(split, new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID()));
      List<String> sampleInput = new ArrayList<>();
      while (lineReader.nextKeyValue())
        sampleInput.add(lineReader.getCurrentValue().toString());
      // If the sample input contains non-printable characters, fail to avoid misinterpretation of binary files
      for (String line : sampleInput) {
        for (int i = 0; i < line.length(); i++)
          if (line.charAt(i) < 0x20 && "\t\r\n".indexOf(line.charAt(i)) == -1)
            return null;
      }
      // Use the sample lines to detect the geometry, the separator, and whether a header line exists or not

      // 1- Detect the field separator by trying the common ones and finding the one that gives a consistent
      // number of columns for all lines
      List<Character> candidateSeparators;
      if (conf.contains(FieldSeparator)) {
        // If the user already provided a field separator, stick to it
        char fieldSeparator = conf.getString(FieldSeparator).charAt(0);
        candidateSeparators = Collections.singletonList(fieldSeparator);
      } else {
        // Consider all the common field separators
        candidateSeparators = new ArrayList<>(Arrays.asList(' ', ',', '\t', ':', ';'));
      }
      for (char candidateSeparator : candidateSeparators) {
        // Collect some metadata about the columns to use in auto-detection with and without a header line
        boolean useHeaderLine = false;
        do {
          // Collect column metadata
          ColumnMetadata[] columnsMetadata = collectColumnMetadata(sampleInput, candidateSeparator, useHeaderLine);
          // Now let's see if we can infer the input format from the column metadata
          // Try to infer from the combination of column names and types
          int longitudeColumn = -1;
          int latitudeColumn = -1;
          int wktColumn = -1;
          for (int $iAtt = 0; $iAtt < columnsMetadata.length; $iAtt++) {
            ColumnMetadata columnMetadata = columnsMetadata[$iAtt];
            if (columnMetadata.type == ColumnMetadata.ColumnType.WKT)
              wktColumn = $iAtt;
            if (columnMetadata.type == ColumnMetadata.ColumnType.Numeric &&
                Double.isFinite(columnMetadata.minValue) &&
                Double.isFinite(columnMetadata.maxValue)) {
              if (columnMetadata.name.toLowerCase().contains("latitude") ||
                  columnMetadata.name.equalsIgnoreCase("lat") ||
                  columnMetadata.name.equalsIgnoreCase("y"))
                latitudeColumn = $iAtt;
              else if (columnMetadata.name.toLowerCase().contains("longitude") ||
                  columnMetadata.name.equalsIgnoreCase("long") ||
                  columnMetadata.name.equalsIgnoreCase("x"))
                longitudeColumn = $iAtt;
              else if (longitudeColumn == -1)
                longitudeColumn = $iAtt;
              else if (latitudeColumn == -1)
                latitudeColumn = $iAtt;
            }
          }
          String detectedFormat = null;
          if (wktColumn != -1) {
            // Found a WKT column, use it
            detectedFormat = String.format("wkt(%d)", wktColumn);
          } else if (longitudeColumn != -1 && latitudeColumn != -1) {
            // Found two columns for longitude and latitude
            detectedFormat = String.format("point(%d,%d)", longitudeColumn, latitudeColumn);
          }
          if (detectedFormat != null) {
            SpatialJoinOptions opts = new SpatialJoinOptions(false)
                .set(SpatialFileRDD.InputFormat(), detectedFormat)
                .set(FieldSeparator, Character.toString(candidateSeparator));
            if (useHeaderLine)
              opts.set(SkipHeader, true);
            return opts;
          }

          useHeaderLine = !useHeaderLine;
        } while (useHeaderLine);
      }
      // Could not detect the input format with any of the candidate separators
      return null;
    } catch (IOException e) {
      // Could not open the file for autodetection
      LOG.warn(String.format("Could not open the input file '%s' for autodetection", input));
      return null;
    } finally {
      try {
        lineReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Collects some metadata about the columns in the given sample input to help in input format auto detection.
   * @param sampleLines a set of sample lines from the input
   * @param separator the field separator (delimiter) to try to use
   * @param useHeaderLine whether to treat the first line as header o rnot
   * @return an array of column information according to the given input
   */
  private static ColumnMetadata[] collectColumnMetadata(List<String> sampleLines, char separator, boolean useHeaderLine) {
    WKTReader wktReader = new WKTReader();
    Map<Integer, ColumnMetadata> columnsMetadata = new TreeMap<>();
    for (int $iLine = 0; $iLine < sampleLines.size(); $iLine++) {
      Text line = new Text(sampleLines.get($iLine));
      int $iAttr = 0;
      if (useHeaderLine && $iLine == 0) {
        // Parse as header line
        while (line.getLength() > 0) {
          String name = deleteAttribute(line, separator, 0, DefaultQuoteCharacters);
          ColumnMetadata metadata = new ColumnMetadata();
          metadata.name = name;
          columnsMetadata.put($iAttr, metadata);
          $iAttr++;
        }
      } else {
        // Parse as data line
        while (line.getLength() > 0) {
          ColumnMetadata columnMetata  = columnsMetadata.get($iAttr);
          if (columnMetata == null) {
            columnMetata = new ColumnMetadata();
            columnMetata.name = "$attr"+$iAttr;
            columnsMetadata.put($iAttr, columnMetata);
          }

          String value = deleteAttribute(line, separator, 0, DefaultQuoteCharacters);
          if (value != null) {
            if (columnMetata.type == ColumnMetadata.ColumnType.Numeric) {
              // Try to consider this column as numeric
              try {
                double numericValue = Double.parseDouble(value);
                columnMetata.minValue = Double.min(columnMetata.minValue, numericValue);
                columnMetata.maxValue = Double.max(columnMetata.maxValue, numericValue);
              } catch (NumberFormatException e) {
                // Move to the more relaxed format of WKT
                columnMetata.type = ColumnMetadata.ColumnType.WKT;
              }
            }
            if (columnMetata.type == ColumnMetadata.ColumnType.WKT) {
              // Try to parse it as a WKT object
              try {
                StringReader str = new StringReader(value);
                wktReader.read(str);
                // If the string was not fully read, it might indicate that it is not WKT
                if (str.read() != -1)
                  columnMetata.type = ColumnMetadata.ColumnType.String;
              } catch (ParseException e) {
                // Not a valid WKT format. Demote it to a regular string
                columnMetata.type = ColumnMetadata.ColumnType.String;
              } catch (IOException e) {
                throw new RuntimeException("Unexpected error", e);
              }
            }
          }
          $iAttr++;
        }
      }
    }
    return columnsMetadata.values().toArray(new ColumnMetadata[0]);
  }


  /**
   * Drops an attribute from a line that represents a CSV line
   * @param line the text line to delete the attribute from
   * @param fieldSeparator the field separator
   * @param fieldToDrop the index of the field to drop (0-based)
   * @param quotes characters to use as quoting characters. Every pair of characters denote start and end quotes
   * @return the contents of the field that has been deleted without field separators
   */
  public static String deleteAttribute(Text line, char fieldSeparator, int fieldToDrop, String quotes) {
    if (line.getLength() == 0)
      return null;
    // Delete the desired field from the given text line and return it as a string
    int i1 = 0;
    int iField = 0;
    byte[] bytes = line.getBytes();
    String fieldValue = null;
    do {
      // i1 is the offset of the first character in the field.
      // i2 iterates until reaching the first index after then last character in the attribute
      int i2 = i1;
      boolean quoted = false;
      if (quotes != null) {
        int iQuote = quotes.indexOf(bytes[i1]);
        if (iQuote >= 0 && (iQuote & 1) == 0) {
          quoted = true;
          // Quoted field. Skip until the closing quote
          byte endQuote = (byte) quotes.charAt(iQuote + 1);
          do
            i2++;
          while (i2 < line.getLength() && bytes[i2] != endQuote);
        }
      }
      // Non-quoted field, skip until the field separator
      while (i2 < line.getLength() && bytes[i2] != fieldSeparator)
        i2++;
      if (iField == fieldToDrop) {
        // Check an empty range
        if (i1 == i2) {
          // Remove the field separator
          System.arraycopy(bytes, i2 + 1, bytes, i1, line.getLength() - i2 - 1);
          line.set(bytes, 0, line.getLength() - 1);
          return null;
        }
        // Parse and remove the desired field
        if (quoted)
          fieldValue = new String(bytes, i1 + 1, i2 - i1 - 2); // Quoted field
        else
          fieldValue = new String(bytes, i1, i2 - i1);
        if (i2 < line.getLength()) {
          System.arraycopy(bytes, i2 + 1, bytes, i1, line.getLength() - i2 - 1);
          line.set(bytes, 0, line.getLength() - (i2 - i1 + 1));
        } else if (i1 != 0) {
          line.set(bytes, 0, i1-1);
        } else {
          line.clear();
        }
      }
      i1 = i2+1;
      iField++;
    } while (i1 < line.getLength() && iField <= fieldToDrop);
    if (iField == fieldToDrop && i1 == line.getLength())
      fieldValue = "";
    return fieldValue;
  }

  /** Fills in omitted coordinate-column indexes by continuing the provided sequence. */
  public static int[] createColumnIndexes(int numDimensionColumns, int ... columns) {
    if (columns.length == numDimensionColumns)
      return columns;
    int[] columnIndexes = new int[numDimensionColumns];
    System.arraycopy(columns, 0, columnIndexes, 0, columns.length);
    for (int $i = columns.length; $i < columnIndexes.length; $i++) {
      columnIndexes[$i] = $i == 0 ? 0 : (columnIndexes[$i-1] + 1);
    }
    return columnIndexes;
  }
}
