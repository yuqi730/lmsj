package cn.edu.whu.spatialJoin.core.partitioning.helper;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A multi-level hierarchy of aggregated abstract features.
 *
 * <p>Level 0 is the coarsest level and the last level is the finest level made available
 * to the driver-side partitioner construction logic. Adjacent levels can optionally carry
 * explicit parent-to-children index links so partitioners can descend the hierarchy without
 * having to re-match cells by envelope intersection alone.
 */
public class AbstractFeatureHierarchy implements Serializable {

  private static final long serialVersionUID = 1L;

  private final AbstractFeature[][] levels;
  private final int[][][] childIndexesByLevel;

  /** Creates a hierarchy without explicit parent-to-child index links. */
  public AbstractFeatureHierarchy(AbstractFeature[][] levels) {
    this(levels, null);
  }

  /** Creates a hierarchy together with explicit parent-to-child level links. */
  public AbstractFeatureHierarchy(AbstractFeature[][] levels, int[][][] childIndexesByLevel) {
    this.levels = levels == null ? new AbstractFeature[0][] : levels;
    this.childIndexesByLevel = childIndexesByLevel == null ? new int[0][][] : childIndexesByLevel;
  }

  /** Returns how many hierarchy levels are available to the driver-side partitioner. */
  public int numLevels() {
    return levels.length;
  }

  /** Returns whether the hierarchy contains any aggregated level at all. */
  public boolean isEmpty() {
    return levels.length == 0;
  }

  /** Returns one specific hierarchy level by index, where 0 is the coarsest level. */
  public AbstractFeature[] getLevel(int levelIndex) {
    return levels[levelIndex];
  }

  /** Returns the coarsest aggregated level used to seed refinement. */
  public AbstractFeature[] getCoarsestLevel() {
    return isEmpty() ? new AbstractFeature[0] : levels[0];
  }

  /** Returns the finest available level, often one descriptor per original feature. */
  public AbstractFeature[] getFinestLevel() {
    return isEmpty() ? new AbstractFeature[0] : levels[levels.length - 1];
  }

  /** Returns a shallow copy of all hierarchy levels. */
  public AbstractFeature[][] getLevels() {
    return Arrays.copyOf(levels, levels.length);
  }

  /** Returns whether one parent level has an explicitly linked child level below it. */
  public boolean hasChildLevel(int parentLevelIndex) {
    return parentLevelIndex >= 0 &&
        parentLevelIndex < childIndexesByLevel.length &&
        parentLevelIndex + 1 < levels.length;
  }

  /** Returns the child indexes of one parent feature on the next finer level. */
  public int[] getChildIndexes(int parentLevelIndex, int parentFeatureIndex) {
    if (!hasChildLevel(parentLevelIndex) ||
        parentFeatureIndex < 0 ||
        parentFeatureIndex >= childIndexesByLevel[parentLevelIndex].length) {
      return new int[0];
    }
    int[] childIndexes = childIndexesByLevel[parentLevelIndex][parentFeatureIndex];
    return childIndexes == null ? new int[0] : Arrays.copyOf(childIndexes, childIndexes.length);
  }

  /** Returns the merged child indexes of several parent features on the next finer level. */
  public int[] getChildIndexes(int parentLevelIndex, int[] parentFeatureIndexes) {
    if (!hasChildLevel(parentLevelIndex) || parentFeatureIndexes == null || parentFeatureIndexes.length == 0) {
      return new int[0];
    }
    int totalSize = 0;
    for (int parentFeatureIndex : parentFeatureIndexes) {
      if (parentFeatureIndex >= 0 && parentFeatureIndex < childIndexesByLevel[parentLevelIndex].length) {
        int[] childIndexes = childIndexesByLevel[parentLevelIndex][parentFeatureIndex];
        if (childIndexes != null) {
          totalSize += childIndexes.length;
        }
      }
    }
    if (totalSize == 0) {
      return new int[0];
    }
    int[] mergedChildIndexes = new int[totalSize];
    int offset = 0;
    for (int parentFeatureIndex : parentFeatureIndexes) {
      if (parentFeatureIndex >= 0 && parentFeatureIndex < childIndexesByLevel[parentLevelIndex].length) {
        int[] childIndexes = childIndexesByLevel[parentLevelIndex][parentFeatureIndex];
        if (childIndexes != null && childIndexes.length > 0) {
          System.arraycopy(childIndexes, 0, mergedChildIndexes, offset, childIndexes.length);
          offset += childIndexes.length;
        }
      }
    }
    if (offset == mergedChildIndexes.length) {
      return mergedChildIndexes;
    }
    return Arrays.copyOf(mergedChildIndexes, offset);
  }
}
