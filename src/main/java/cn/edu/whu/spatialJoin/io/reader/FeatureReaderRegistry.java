package cn.edu.whu.spatialJoin.io.reader;

import cn.edu.whu.spatialJoin.io.reader.shapefile.ShapefileFeatureReader;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Explicit registry for all feature readers supported by the current project.
 *
 * <p>This replaces the previous XML-based discovery flow so data access can stay
 * independent from CLI/operation configuration.</p>
 */
public final class FeatureReaderRegistry {

  private static volatile Map<String, Class<? extends FeatureReader>> registeredReaders;

  private FeatureReaderRegistry() {
  }

  public static Map<String, Class<? extends FeatureReader>> getRegisteredReaders() {
    if (registeredReaders == null) {
      synchronized (FeatureReaderRegistry.class) {
        if (registeredReaders == null) {
          Map<String, Class<? extends FeatureReader>> readers = new LinkedHashMap<>();
          for (Class<? extends FeatureReader> readerClass : defaultReaders()) {
            SpatialReaderMetadata metadata = readerClass.getAnnotation(SpatialReaderMetadata.class);
            if (metadata == null) {
              throw new IllegalStateException("Reader class is missing @SpatialReaderMetadata: " + readerClass.getName());
            }
            readers.put(metadata.shortName(), readerClass);
          }
          registeredReaders = Collections.unmodifiableMap(readers);
        }
      }
    }
    return registeredReaders;
  }

  private static List<Class<? extends FeatureReader>> defaultReaders() {
    return Arrays.asList(
        CSVFeatureReader.class,
        GeoJSONFeatureReader.class,
        ShapefileFeatureReader.class
    );
  }
}
