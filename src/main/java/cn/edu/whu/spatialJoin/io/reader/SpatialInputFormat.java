package cn.edu.whu.spatialJoin.io.reader;

/**
 * Strongly-typed spatial input formats used by the unified data access layer.
 */
public enum SpatialInputFormat {
  AUTO("*auto*"),
  CSV("csv"),
  GEOJSON("geojson"),
  SHAPEFILE("shapefile");

  private final String formatName;

  SpatialInputFormat(String formatName) {
    this.formatName = formatName;
  }

  public String formatName() {
    return formatName;
  }

  public static SpatialInputFormat from(String value) {
    if (value == null || value.trim().isEmpty() || "*auto*".equalsIgnoreCase(value.trim())) {
      return AUTO;
    }
    for (SpatialInputFormat format : values()) {
      if (format.formatName.equalsIgnoreCase(value.trim())) {
        return format;
      }
    }
    throw new IllegalArgumentException("Unsupported spatial input format: " + value);
  }
}
