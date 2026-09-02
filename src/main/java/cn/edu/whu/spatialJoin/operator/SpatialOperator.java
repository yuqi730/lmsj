package cn.edu.whu.spatialJoin.operator;

import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Unified entry point for operator-style execution in the current project.
 *
 * <p>This class replaces the old command-line/operation discovery stack with a thin,
 * explicit facade that:
 * 1. parses spark-submit style arguments into {@link SpatialJoinOptions}
 * 2. exposes focused read / partition / join helpers
 * 3. treats read and partition as preparation steps for the final spatial join
 */
public final class SpatialOperator implements Serializable {

  private static final long serialVersionUID = 1L;

  private final SpatialJoinOptions options;

  public SpatialOperator() {
    this(new SpatialJoinOptions());
  }

  public SpatialOperator(SpatialJoinOptions options) {
    this.options = options == null ? new SpatialJoinOptions() : new SpatialJoinOptions(options);
  }

  public SpatialOperator(String[] args) {
    this(parseSubmitArgs(args));
  }

  /**
   * Parses spark-submit style arguments into {@link SpatialJoinOptions}.
   *
   * <p>Supported forms include:
   * {@code --key=value}, {@code --key value}, {@code key:value}, {@code -flag},
   * {@code --no-flag} and {@code -no-flag}.
   *
   * @param args raw command-line style arguments
   * @return parsed options object
   */
  public static SpatialJoinOptions parseSubmitArgs(String[] args) {
    SpatialJoinOptions parsed = new SpatialJoinOptions();
    if (args == null) {
      return parsed;
    }

    for (int i = 0; i < args.length; i++) {
      String rawArg = args[i];
      if (rawArg == null) {
        continue;
      }
      String arg = rawArg.trim();
      if (arg.isEmpty()) {
        continue;
      }

      if (arg.startsWith("--no-")) {
        parsed.set(arg.substring(5), false);
        continue;
      }
      if (arg.startsWith("-no-")) {
        parsed.set(arg.substring(4), false);
        continue;
      }
      if (arg.startsWith("--")) {
        String token = arg.substring(2);
        int separator = findArgumentValueSeparator(token);
        if (separator > 0) {
          parsed.set(token.substring(0, separator), token.substring(separator + 1));
        } else if (i + 1 < args.length && looksLikeStandaloneValue(args[i + 1])) {
          parsed.set(token, args[++i]);
        } else {
          parsed.set(token, true);
        }
        continue;
      }
      if (arg.startsWith("-")) {
        String token = arg.substring(1);
        int separator = findArgumentValueSeparator(token);
        if (separator > 0) {
          parsed.set(token.substring(0, separator), token.substring(separator + 1));
        } else {
          parsed.set(token, true);
        }
        continue;
      }

      int separator = findArgumentValueSeparator(arg);
      if (separator > 0) {
        parsed.set(arg.substring(0, separator), arg.substring(separator + 1));
      }
    }
    return parsed;
  }

  /** Returns a defensive copy of the parsed operator options. */
  public SpatialJoinOptions getOptions() {
    return new SpatialJoinOptions(options);
  }

  /** Creates the read-focused helper bound to the current option set. */
  public ReadOperator read() {
    return new ReadOperator(options);
  }

  /** Creates the partition-focused helper bound to the current option set. */
  public PartitionOperator partition() {
    return new PartitionOperator(options);
  }

  /** Creates the spatial-join helper bound to the current option set. */
  public SpatialJoinOperator spatialJoin() {
    return new SpatialJoinOperator(options);
  }

  /**
   * Executes the spatial join pipeline in a Spark-backed context.
   *
   * @param sparkContext active Spark context
   * @return spatial join result
   */
  public SpatialJoinResult execute(JavaSparkContext sparkContext) {
    return spatialJoin().execute(sparkContext);
  }

  /** Finds the separator that splits one command-line token into key and value. */
  private static int findArgumentValueSeparator(String token) {
    int equalsIndex = token.indexOf('=');
    int colonIndex = token.indexOf(':');
    if (equalsIndex < 0) {
      return colonIndex;
    }
    if (colonIndex < 0) {
      return equalsIndex;
    }
    return Math.min(equalsIndex, colonIndex);
  }

  /** Returns true when the token should be treated as the value of the preceding option. */
  private static boolean looksLikeStandaloneValue(String token) {
    if (token == null) {
      return false;
    }
    String trimmed = token.trim();
    if (trimmed.isEmpty()) {
      return false;
    }
    if (!trimmed.startsWith("-")) {
      return true;
    }
    return trimmed.matches("-?\\d+(\\.\\d+)?");
  }
}
