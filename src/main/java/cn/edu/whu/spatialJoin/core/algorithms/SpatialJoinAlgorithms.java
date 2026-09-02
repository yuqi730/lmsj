package cn.edu.whu.spatialJoin.core.algorithms;

import cn.edu.whu.spatialJoin.geom.EnvelopeND;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper;
import cn.edu.whu.spatialJoin.geom.util.GeometryMetadataReader;
import cn.edu.whu.spatialJoin.geom.IFeature;
import cn.edu.whu.spatialJoin.utils.IntArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.locationtech.jts.geom.*;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.util.LongAccumulator;

/**
 * A utility class that contains implementations of spatial join algorithms.
 */
public class SpatialJoinAlgorithms {
  /**Logger for this class*/
  private static final Log LOG = LogFactory.getLog(SpatialJoinAlgorithms.class);
  /** Small epsilon used when deriving diagonal slopes from degenerate envelopes. */
  private static final double EPSILON = 1.0e-12;

  /**Enumerated type for the distributed spatial join algorithms*/
  public enum ESJDistributedAlgorithm {BNLJ, PBSM, SJMR, DJ, REPJ}

  /**Enumerated type for the spatial join predicate*/
  public enum ESJPredicate {Intersects, MBRIntersects, Contains}

  public interface SJResult {
    void accept(int i, int j, double[] refPoint);
  }

  /** Supported scan-axis modes for the configurable plane-sweep implementation. */
  public enum PlaneSweepAxis {X, Y, AUTO, DIAGONAL}

  /** Supported implementation variants for the configurable plane-sweep implementation. */
  public enum PlaneSweepImplementation {CLASSIC, SLIDING_WINDOW}

  /** Optional observer that receives the effective scan axis resolved for one plane-sweep invocation. */
  public interface PlaneSweepObserver {
    void onResolvedAxis(PlaneSweepAxis axis, PlaneSweepImplementation implementation);
  }

  /**
   * Computes the spatial join between two sets of rectangles and reports all overlapping pairs of indexes.
   *
   * @param minCoord1 an array of coordinates for the lower corners of the first set of rectangles
   * @param maxCoord1 an array of coordinates for the upper corners of the first set of rectangles
   * @param minCoord2 an array of coordinates for the lower corners of the second set of rectangles
   * @param maxCoord2 an array of coordinates for the upper corners of the second set of rectangles
   * @param results the output is reported by calling this function with the indexes of the two rectangles that
   *                overlap. If {@code null}, results are not reported; only the total count is.
   * @param mbrTests an accumulator that counts number of MBR tests done while running the plane-sweep algorithm
   * @return the total number of results found.
   */
  public static int planeSweepRectangles(double[][] minCoord1, double[][] maxCoord1,
                                         double[][] minCoord2, double[][] maxCoord2,
                                         SJResult results, LongAccumulator mbrTests) {
    long t1 = System.nanoTime();
    assert minCoord1.length == maxCoord1.length;
    assert minCoord1.length == minCoord2.length;
    assert minCoord1.length == maxCoord2.length;
    int count = 0;
    int numDimensions = minCoord1.length;
    int n1 = minCoord1[0].length;
    int n2 = minCoord2[0].length;
    LOG.debug(String.format("Running the plane sweep join algorithm between lists sizes of %d and %d", n1, n2));
    // Generate indexes to report the answer
    int[] indexes1 = new int[n1];
    int[] indexes2 = new int[n2];
    for (int $i = 0; $i < n1; $i++)
      indexes1[$i] = $i;
    for (int $i = 0; $i < n2; $i++)
      indexes2[$i] = $i;
    long localMBRTests = 0;
    // Sort by the first coordinate and keep track of the array index for each entry for reporting
    IndexedSortable sortable1 = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        return (int) Math.signum(minCoord1[0][i] - minCoord1[0][j]);
      }

      @Override
      public void swap(int i, int j) {
        if (i == j)
          return;
        // Swap indexes
        indexes1[i] ^= indexes1[j];
        indexes1[j] ^= indexes1[i];
        indexes1[i] ^= indexes1[j];
        // Swap coordinates
        for (int $d = 0; $d < numDimensions; $d++) {
          double t = minCoord1[$d][i];
          minCoord1[$d][i] = minCoord1[$d][j];
          minCoord1[$d][j] = t;
          t = maxCoord1[$d][i];
          maxCoord1[$d][i] = maxCoord1[$d][j];
          maxCoord1[$d][j] = t;
        }
      }
    };
    IndexedSortable sortable2 = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        return (int) Math.signum(minCoord2[0][i] - minCoord2[0][j]);
      }

      @Override
      public void swap(int i, int j) {
        if (i == j)
          return;
        // Swap indexes
        indexes2[i] ^= indexes2[j];
        indexes2[j] ^= indexes2[i];
        indexes2[i] ^= indexes2[j];
        // Swap coordinates
        for (int $d = 0; $d < numDimensions; $d++) {
          double t = minCoord2[$d][i];
          minCoord2[$d][i] = minCoord2[$d][j];
          minCoord2[$d][j] = t;
          t = maxCoord2[$d][i];
          maxCoord2[$d][i] = maxCoord2[$d][j];
          maxCoord2[$d][j] = t;
        }
      }
    };
    QuickSort sorter = new QuickSort();
    sorter.sort(sortable1, 0, n1);
    sorter.sort(sortable2, 0, n2);

    // Now, run the planesweep algorithm
    int lastReportedProgress = 0;
    long lastReportedProgressTime = System.currentTimeMillis();
    int i = 0, j = 0;
    double[] refPoint = new double[numDimensions];
    while (i < n1 && j < n2) {
      if (minCoord1[0][i] < minCoord2[0][j]) {
        // R1[i] is the left-most rectangle. Activate it and compare to all rectangles R2 until passing the right end
        // of R1[i]
        int jj = j;
        while (jj < n2 && minCoord2[0][jj] <= maxCoord1[0][i]) {
          localMBRTests++;
          // Compare the two rectangles R1[i] and R2[jj] and report if needed
          if (rectanglesOverlap(minCoord1, maxCoord1, i, minCoord2, maxCoord2, jj)) {
            // Found an overlap
            count++;
            if (results != null) {
              for (int d = 0; d < numDimensions; d++)
                refPoint[d] = Math.max(minCoord1[d][i], minCoord2[d][jj]);
              results.accept(indexes1[i], indexes2[jj], refPoint);
            }
          }
          jj++;
        }
        // Skip until the first record that might produce a result from i
        do {
          i++;
        } while (i < n1 && maxCoord1[0][i] < minCoord2[0][j]);
      } else {
        // R2[j] is the left-most rectangle. Activate it and compare to all rectangles of R1 until passing the right
        // end of R2[j]
        int ii = i;
        while (ii < n1 && minCoord1[0][ii] <= maxCoord2[0][j]) {
          // Compare the two rectangles R1[ii] and R2[j] and report if needed
          localMBRTests++;
          if (rectanglesOverlap(minCoord1, maxCoord1, ii, minCoord2, maxCoord2, j)) {
            // Found an overlap
            count++;
            if (results != null) {
              for (int d = 0; d < numDimensions; d++)
                refPoint[d] = Math.max(minCoord1[d][ii], minCoord2[d][j]);
              results.accept(indexes1[ii], indexes2[j], refPoint);
            }
          }
          ii++;
        }
        // Skip until the first record that might produce a result from j
        do {
          j++;
        } while (j < n2 && maxCoord2[0][j] < minCoord1[0][i]);
      }
      if (System.currentTimeMillis() - lastReportedProgressTime > 60000) {
        // Report the result every 60 seconds if there is at least 1% change
        int progress = Math.min(100, (i * 10 / n1) * 10 + (j * n2 / 10));
        if (progress != lastReportedProgress) {
          lastReportedProgress = progress;
          LOG.info(String.format("Spatial join progress %d%% (i=%d/%d, j=%d/%d)", lastReportedProgress, i, n1, j, n2));
        }
        lastReportedProgressTime = System.currentTimeMillis();
      }
    }
    if (mbrTests != null)
      mbrTests.add(localMBRTests);
    long t2 = System.nanoTime();
    return count;
  }

  /**
   * Configurable plane-sweep variant that can choose the scan axis and implementation style.
   *
   * <p>The current project keeps {@link #planeSweepRectangles(double[][], double[][], double[][], double[][], SJResult, LongAccumulator)}
   * unchanged as the classic x-axis baseline. This overload supplements it with two extra controls:
   * <ul>
   *   <li>the scan axis can be {@code x}, {@code y}, {@code auto}, or {@code diagonal}, and</li>
   *   <li>the implementation can be the current {@code classic} scan or a {@code sliding-window}
   *       variant that keeps the current candidate window in primitive index arrays.</li>
   * </ul>
   *
   * <p>Regardless of the chosen mode, the method still applies the same exact 2D rectangle-overlap
   * test before reporting candidates. Only the one-dimensional ordering and candidate-window
   * maintenance strategy change.
   *
   * @param minCoord1 lower corners of the first rectangle set
   * @param maxCoord1 upper corners of the first rectangle set
   * @param minCoord2 lower corners of the second rectangle set
   * @param maxCoord2 upper corners of the second rectangle set
   * @param results optional callback for candidate pairs
   * @param mbrTests optional accumulator for the number of MBR overlap tests
   * @param axisMode configured sweep axis: x, y, auto, or diagonal
   * @param implementation configured implementation: classic or sliding-window
   * @return total number of overlapping pairs
   */
  public static int planeSweepRectangles(double[][] minCoord1, double[][] maxCoord1,
                                         double[][] minCoord2, double[][] maxCoord2,
                                         SJResult results, LongAccumulator mbrTests,
                                         String axisMode, String implementation) {
    return planeSweepRectangles(
        minCoord1,
        maxCoord1,
        minCoord2,
        maxCoord2,
        results,
        mbrTests,
        axisMode,
        implementation,
        null);
  }

  /**
   * Configurable plane-sweep variant with an optional observer for the finally resolved scan axis.
   *
   * <p>This is primarily useful for diagnostics of {@code auto} mode, where callers may want to
   * know whether the local join ultimately swept along x or y.
   */
  public static int planeSweepRectangles(double[][] minCoord1, double[][] maxCoord1,
                                         double[][] minCoord2, double[][] maxCoord2,
                                         SJResult results, LongAccumulator mbrTests,
                                         String axisMode, String implementation,
                                         PlaneSweepObserver observer) {
    PlaneSweepAxis resolvedAxis = resolvePlaneSweepAxis(minCoord1, maxCoord1, minCoord2, maxCoord2, axisMode);
    PlaneSweepImplementation resolvedImplementation = normalizePlaneSweepImplementation(implementation);
    if (observer != null)
      observer.onResolvedAxis(resolvedAxis, resolvedImplementation);
    if (resolvedImplementation == PlaneSweepImplementation.CLASSIC && resolvedAxis == PlaneSweepAxis.X) {
      return planeSweepRectangles(minCoord1, maxCoord1, minCoord2, maxCoord2, results, mbrTests);
    }
    double diagonalSlope = resolvedAxis == PlaneSweepAxis.DIAGONAL
        ? resolveDiagonalSlope(minCoord1, maxCoord1, minCoord2, maxCoord2)
        : 0.0d;
    double[] minProjection1 = computeProjectionMinimums(minCoord1, maxCoord1, resolvedAxis, diagonalSlope);
    double[] maxProjection1 = computeProjectionMaximums(minCoord1, maxCoord1, resolvedAxis, diagonalSlope);
    double[] minProjection2 = computeProjectionMinimums(minCoord2, maxCoord2, resolvedAxis, diagonalSlope);
    double[] maxProjection2 = computeProjectionMaximums(minCoord2, maxCoord2, resolvedAxis, diagonalSlope);
    if (resolvedImplementation == PlaneSweepImplementation.SLIDING_WINDOW) {
      return planeSweepRectanglesSlidingWindow(
          minCoord1, maxCoord1, minCoord2, maxCoord2,
          minProjection1, maxProjection1, minProjection2, maxProjection2,
          results, mbrTests);
    }
    return planeSweepRectanglesProjectedClassic(
        minCoord1, maxCoord1, minCoord2, maxCoord2,
        minProjection1, maxProjection1, minProjection2, maxProjection2,
        results, mbrTests);
  }

  /**
   * Test if two rectangles overlap R1[i] and R2[j].
   * @param minCoord1 the lower corners of the rectangles in the first set
   * @param maxCoord1 the upper corners of the rectangles in the first set
   * @param i the index of the first rectangle
   * @param minCoord2 the lower corners of the rectangles in the second set
   * @param maxCoord2 the upper corners of the rectangles in the second set
   * @param j the index of the second rectangle
   * @return {@code true} if the rectangles overlap, i.e., not disjoint, and false if they are disjoint
   */
  static final boolean rectanglesOverlap(double[][] minCoord1, double[][] maxCoord1, int i,
                                   double[][] minCoord2, double[][] maxCoord2, int j) {
    assert minCoord1.length == 2;
    return !(minCoord1[0][i] > maxCoord2[0][j] || minCoord2[0][j] > maxCoord1[0][i] ||
        minCoord1[1][i] > maxCoord2[1][j] || minCoord2[1][j] > maxCoord1[1][i]);
  }

  /** Returns the effective scan axis after resolving {@code auto} into either x or y. */
  private static PlaneSweepAxis resolvePlaneSweepAxis(double[][] minCoord1, double[][] maxCoord1,
                                                      double[][] minCoord2, double[][] maxCoord2,
                                                      String axisMode) {
    PlaneSweepAxis configured = normalizePlaneSweepAxis(axisMode);
    if (configured != PlaneSweepAxis.AUTO) {
      return configured;
    }
    double totalXSpan = totalSpan(minCoord1, maxCoord1, 0) + totalSpan(minCoord2, maxCoord2, 0);
    double totalYSpan = totalSpan(minCoord1, maxCoord1, 1) + totalSpan(minCoord2, maxCoord2, 1);
    return totalXSpan <= totalYSpan ? PlaneSweepAxis.X : PlaneSweepAxis.Y;
  }

  /** Normalizes one user-facing scan-axis string. */
  private static PlaneSweepAxis normalizePlaneSweepAxis(String axisMode) {
    if (axisMode == null || axisMode.trim().isEmpty()) {
      return PlaneSweepAxis.AUTO;
    }
    String normalized = axisMode.trim().toLowerCase();
    if ("x".equals(normalized)) {
      return PlaneSweepAxis.X;
    }
    if ("y".equals(normalized)) {
      return PlaneSweepAxis.Y;
    }
    if ("diagonal".equals(normalized)) {
      return PlaneSweepAxis.DIAGONAL;
    }
    return PlaneSweepAxis.AUTO;
  }

  /** Normalizes one user-facing implementation string. */
  private static PlaneSweepImplementation normalizePlaneSweepImplementation(String implementation) {
    if (implementation == null || implementation.trim().isEmpty()) {
      return PlaneSweepImplementation.CLASSIC;
    }
    String normalized = implementation.trim().toLowerCase();
    if ("sliding-window".equals(normalized)) {
      return PlaneSweepImplementation.SLIDING_WINDOW;
    }
    return PlaneSweepImplementation.CLASSIC;
  }

  /** Computes the summed span along one axis for all rectangles in one MBR array pair. */
  private static double totalSpan(double[][] minCoord, double[][] maxCoord, int axis) {
    if (minCoord == null || maxCoord == null || minCoord.length <= axis || maxCoord.length <= axis) {
      return 0.0d;
    }
    double total = 0.0d;
    for (int i = 0; i < minCoord[axis].length; i++) {
      total += Math.max(0.0d, maxCoord[axis][i] - minCoord[axis][i]);
    }
    return total;
  }

  /** Derives the slope of the main diagonal of the current object space. */
  private static double resolveDiagonalSlope(double[][] minCoord1, double[][] maxCoord1,
                                             double[][] minCoord2, double[][] maxCoord2) {
    double minX = Math.min(arrayMinimum(minCoord1[0]), arrayMinimum(minCoord2[0]));
    double maxX = Math.max(arrayMaximum(maxCoord1[0]), arrayMaximum(maxCoord2[0]));
    double minY = Math.min(arrayMinimum(minCoord1[1]), arrayMinimum(minCoord2[1]));
    double maxY = Math.max(arrayMaximum(maxCoord1[1]), arrayMaximum(maxCoord2[1]));
    double width = Math.max(0.0d, maxX - minX);
    double height = Math.max(0.0d, maxY - minY);
    if (width <= EPSILON) {
      return 1.0d;
    }
    return Math.max(0.0d, height / width);
  }

  /** Computes the minimum projected coordinate of every rectangle under the chosen sweep axis. */
  private static double[] computeProjectionMinimums(double[][] minCoord, double[][] maxCoord,
                                                    PlaneSweepAxis axis, double diagonalSlope) {
    double[] projection = new double[minCoord[0].length];
    for (int i = 0; i < projection.length; i++) {
      projection[i] = projectMinimum(minCoord, maxCoord, i, axis, diagonalSlope);
    }
    return projection;
  }

  /** Computes the maximum projected coordinate of every rectangle under the chosen sweep axis. */
  private static double[] computeProjectionMaximums(double[][] minCoord, double[][] maxCoord,
                                                    PlaneSweepAxis axis, double diagonalSlope) {
    double[] projection = new double[maxCoord[0].length];
    for (int i = 0; i < projection.length; i++) {
      projection[i] = projectMaximum(minCoord, maxCoord, i, axis, diagonalSlope);
    }
    return projection;
  }

  /** Projects one rectangle minimum onto the configured sweep axis. */
  private static double projectMinimum(double[][] minCoord, double[][] maxCoord, int index,
                                       PlaneSweepAxis axis, double diagonalSlope) {
    switch (axis) {
      case Y:
        return minCoord[1][index];
      case DIAGONAL:
        return minCoord[0][index] + diagonalSlope * minCoord[1][index];
      case X:
      default:
        return minCoord[0][index];
    }
  }

  /** Projects one rectangle maximum onto the configured sweep axis. */
  private static double projectMaximum(double[][] minCoord, double[][] maxCoord, int index,
                                       PlaneSweepAxis axis, double diagonalSlope) {
    switch (axis) {
      case Y:
        return maxCoord[1][index];
      case DIAGONAL:
        return maxCoord[0][index] + diagonalSlope * maxCoord[1][index];
      case X:
      default:
        return maxCoord[0][index];
    }
  }

  /** Sorts rectangle indexes by their projected minimum coordinate. */
  private static int[] sortIndexesByProjection(double[] minProjection) {
    int[] order = new int[minProjection.length];
    for (int i = 0; i < order.length; i++) {
      order[i] = i;
    }
    IndexedSortable sortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        return Double.compare(minProjection[order[i]], minProjection[order[j]]);
      }

      @Override
      public void swap(int i, int j) {
        if (i == j) {
          return;
        }
        int temp = order[i];
        order[i] = order[j];
        order[j] = temp;
      }
    };
    new QuickSort().sort(sortable, 0, order.length);
    return order;
  }

  /** Configurable classic plane-sweep implementation that keeps the original scan structure. */
  private static int planeSweepRectanglesProjectedClassic(double[][] minCoord1, double[][] maxCoord1,
                                                          double[][] minCoord2, double[][] maxCoord2,
                                                          double[] minProjection1, double[] maxProjection1,
                                                          double[] minProjection2, double[] maxProjection2,
                                                          SJResult results, LongAccumulator mbrTests) {
    int[] order1 = sortIndexesByProjection(minProjection1);
    int[] order2 = sortIndexesByProjection(minProjection2);
    int count = 0;
    long localMBRTests = 0L;
    int numDimensions = minCoord1.length;
    double[] refPoint = new double[numDimensions];
    int i = 0, j = 0;
    while (i < order1.length && j < order2.length) {
      int index1 = order1[i];
      int index2 = order2[j];
      if (minProjection1[index1] < minProjection2[index2]) {
        int jj = j;
        while (jj < order2.length && minProjection2[order2[jj]] <= maxProjection1[index1]) {
          int candidate2 = order2[jj];
          localMBRTests++;
          if (rectanglesOverlap(minCoord1, maxCoord1, index1, minCoord2, maxCoord2, candidate2)) {
            count++;
            if (results != null) {
              for (int d = 0; d < numDimensions; d++) {
                refPoint[d] = Math.max(minCoord1[d][index1], minCoord2[d][candidate2]);
              }
              results.accept(index1, candidate2, refPoint);
            }
          }
          jj++;
        }
        do {
          i++;
        } while (i < order1.length && maxProjection1[order1[i]] < minProjection2[order2[j]]);
      } else {
        int ii = i;
        while (ii < order1.length && minProjection1[order1[ii]] <= maxProjection2[index2]) {
          int candidate1 = order1[ii];
          localMBRTests++;
          if (rectanglesOverlap(minCoord1, maxCoord1, candidate1, minCoord2, maxCoord2, index2)) {
            count++;
            if (results != null) {
              for (int d = 0; d < numDimensions; d++) {
                refPoint[d] = Math.max(minCoord1[d][candidate1], minCoord2[d][index2]);
              }
              results.accept(candidate1, index2, refPoint);
            }
          }
          ii++;
        }
        do {
          j++;
        } while (j < order2.length && maxProjection2[order2[j]] < minProjection1[order1[i]]);
      }
    }
    if (mbrTests != null) {
      mbrTests.add(localMBRTests);
    }
    return count;
  }

  /**
   * Sliding-window plane sweep that keeps the current candidate window in primitive index arrays.
   *
   * <p>This variant processes rectangle start events in projected order. The side whose next start
   * event comes first is compared against the opposite side's current sliding window. The window is
   * maintained as an in-place compacted integer array of active rectangle indexes, which avoids
   * object-heavy container churn in the hottest inner loop.
   *
   * <p>Compared with a naive active-window implementation, this version intentionally combines
   * "expire old window entries" and "compare against surviving entries" into one single pass over
   * the opposite-side window. After the current object has been compared against all still-active
   * opposite objects, it is only inserted into its own window if some future opposite object can
   * still start before the current object expires on the sweep axis.
   */
  private static int planeSweepRectanglesSlidingWindow(double[][] minCoord1, double[][] maxCoord1,
                                                       double[][] minCoord2, double[][] maxCoord2,
                                                       double[] minProjection1, double[] maxProjection1,
                                                       double[] minProjection2, double[] maxProjection2,
                                                       SJResult results, LongAccumulator mbrTests) {
    int[] order1 = sortIndexesByProjection(minProjection1);
    int[] order2 = sortIndexesByProjection(minProjection2);
    int[] active1 = new int[order1.length];
    int[] active2 = new int[order2.length];
    int activeSize1 = 0;
    int activeSize2 = 0;
    int next1 = 0;
    int next2 = 0;
    int count = 0;
    long localMBRTests = 0L;
    int numDimensions = minCoord1.length;
    double[] refPoint = new double[numDimensions];

    while (next1 < order1.length || next2 < order2.length) {
      boolean processFirstSet = next2 >= order2.length ||
          (next1 < order1.length && minProjection1[order1[next1]] <= minProjection2[order2[next2]]);
      if (processFirstSet) {
        int index1 = order1[next1++];
        int nextActiveSize2 = 0;
        for (int i = 0; i < activeSize2; i++) {
          int index2 = active2[i];
          if (maxProjection2[index2] >= minProjection1[index1]) {
            active2[nextActiveSize2++] = index2;
            localMBRTests++;
            if (rectanglesOverlap(minCoord1, maxCoord1, index1, minCoord2, maxCoord2, index2)) {
              count++;
              if (results != null) {
                for (int d = 0; d < numDimensions; d++) {
                  refPoint[d] = Math.max(minCoord1[d][index1], minCoord2[d][index2]);
                }
                results.accept(index1, index2, refPoint);
              }
            }
          }
        }
        activeSize2 = nextActiveSize2;
        boolean mayOverlapFutureSecondSet = next2 < order2.length && minProjection2[order2[next2]] <= maxProjection1[index1];
        if (mayOverlapFutureSecondSet)
          active1[activeSize1++] = index1;
      } else {
        int index2 = order2[next2++];
        int nextActiveSize1 = 0;
        for (int i = 0; i < activeSize1; i++) {
          int index1 = active1[i];
          if (maxProjection1[index1] >= minProjection2[index2]) {
            active1[nextActiveSize1++] = index1;
            localMBRTests++;
            if (rectanglesOverlap(minCoord1, maxCoord1, index1, minCoord2, maxCoord2, index2)) {
              count++;
              if (results != null) {
                for (int d = 0; d < numDimensions; d++) {
                  refPoint[d] = Math.max(minCoord1[d][index1], minCoord2[d][index2]);
                }
                results.accept(index1, index2, refPoint);
              }
            }
          }
        }
        activeSize1 = nextActiveSize1;
        boolean mayOverlapFutureFirstSet = next1 < order1.length && minProjection1[order1[next1]] <= maxProjection2[index2];
        if (mayOverlapFutureFirstSet)
          active2[activeSize2++] = index2;
      }
    }
    if (mbrTests != null) {
      mbrTests.add(localMBRTests);
    }
    return count;
  }

  /** Returns the smallest value in one coordinate array. */
  private static double arrayMinimum(double[] values) {
    double minimum = Double.POSITIVE_INFINITY;
    for (double value : values) {
      minimum = Math.min(minimum, value);
    }
    return minimum;
  }

  /** Returns the largest value in one coordinate array. */
  private static double arrayMaximum(double[] values) {
    double maximum = Double.NEGATIVE_INFINITY;
    for (double value : values) {
      maximum = Math.max(maximum, value);
    }
    return maximum;
  }

  /**
   * Runs an in-memory filter-refine join over two geometry lists using plane sweep on their MBRs.
   *
   * <p>The method performs three logical steps:
   * <ol>
   *   <li>Drop empty/non-2D geometries that cannot participate in a 2D overlap join.</li>
   *   <li>Compute the MBR arrays of the remaining geometries and run a one-dimensional plane sweep
   *       ordered by the minimum x coordinate.</li>
   *   <li>For every candidate MBR overlap reported by the sweep, call the exact JTS
   *       {@link Geometry#intersects(Geometry)} refine step before emitting a result.</li>
   * </ol>
   *
   * <p>This method works on the geometries exactly as they are provided. If callers want to break
   * large geometries into smaller pieces first, they should do so before invoking this method.
   *
   * @param r the left geometry list to scan. Each entry is treated as one sweep object after unusable
   *          geometries are filtered out.
   * @param s the right geometry list to scan. Each entry is treated as one sweep object after unusable
   *          geometries are filtered out.
   * @param results an optional callback invoked for every exact intersection that survives the refine
   *                step. The callback receives the indexes within the filtered sweep inputs, not a copy
   *                of the geometries themselves.
   * @param mbrTests an optional accumulator that counts how many candidate MBR overlap tests were
   *                 executed inside the plane-sweep phase.
   * @return the total number of exact intersecting pairs found after refine.
   */
  public static int spatialJoinIntersectsPlaneSweep(List<Geometry> r, List<Geometry> s, SJResult results,
                                                    LongAccumulator mbrTests) {
    if (r.size() == 0 || s.size() == 0)
      return 0;
    List<Geometry> filteredR = retainUsable2DGeometries(r);
    List<Geometry> filteredS = retainUsable2DGeometries(s);
    if (filteredR.isEmpty() || filteredS.isEmpty())
      return 0;
    assert GeometryHelper.getCoordinateDimension(filteredR.get(0)) == 2 : "R should be 2D geometries";
    assert GeometryHelper.getCoordinateDimension(filteredS.get(0)) == 2 : "S should be 2D geometries";
    double[][] coordsR = getMBRs(filteredR);
    double[][] coordsS = getMBRs(filteredS);
    return planeSweepRectangles(new double[][]{coordsR[0], coordsR[1]}, new double[][]{coordsR[2], coordsR[3]},
        new double[][]{coordsS[0], coordsS[1]}, new double[][]{coordsS[2], coordsS[3]}, (i, j, refPoint) -> {
          // Run the refine step
          if (intersectsSafely(filteredR.get(i), filteredS.get(j)) && results != null)
            results.accept(i, j, refPoint);
        }, mbrTests);
  }

  /**
   * Configurable geometry-level plane-sweep join.
   *
   * <p>This overload supplements the baseline x-axis implementation with configurable scan-axis and
   * implementation choices while preserving the same exact-intersects semantics.
   */
  public static int spatialJoinIntersectsPlaneSweep(List<Geometry> r, List<Geometry> s, SJResult results,
                                                    LongAccumulator mbrTests,
                                                    String axisMode,
                                                    String implementation) {
    return spatialJoinIntersectsPlaneSweep(r, s, results, mbrTests, axisMode, implementation, null);
  }

  /**
   * Configurable geometry-level plane-sweep join with optional reporting of the effective resolved
   * scan axis.
   */
  public static int spatialJoinIntersectsPlaneSweep(List<Geometry> r, List<Geometry> s, SJResult results,
                                                    LongAccumulator mbrTests,
                                                    String axisMode,
                                                    String implementation,
                                                    PlaneSweepObserver observer) {
    if (r.size() == 0 || s.size() == 0)
      return 0;
    List<Geometry> filteredR = retainUsable2DGeometries(r);
    List<Geometry> filteredS = retainUsable2DGeometries(s);
    if (filteredR.isEmpty() || filteredS.isEmpty())
      return 0;
    double[][] coordsR = getMBRs(filteredR);
    double[][] coordsS = getMBRs(filteredS);
    return planeSweepRectangles(
        new double[][]{coordsR[0], coordsR[1]},
        new double[][]{coordsR[2], coordsR[3]},
        new double[][]{coordsS[0], coordsS[1]},
        new double[][]{coordsS[2], coordsS[3]},
        (i, j, refPoint) -> {
          if (intersectsSafely(filteredR.get(i), filteredS.get(j)) && results != null)
            results.accept(i, j, refPoint);
        },
        mbrTests,
        axisMode,
        implementation,
        observer);
  }

  /**
   * Computes the minimum bounding rectangles of all given geometries as primitive arrays.
   * The returned 2D array stores the minimum and maximum coordinate along each dimension.
   * @param geometries the list of geometries to get their MBRs
   * @return the computed coordinates
   */
  public static double[][] getMBRs(List<Geometry> geometries) {
    int numDimensions = GeometryHelper.getCoordinateDimension(geometries.get(0));
    double[][] coords = new double[numDimensions*2][geometries.size()];
    EnvelopeND mbr = new EnvelopeND(geometries.get(0).getFactory(), numDimensions);
    for (int iGeom = 0; iGeom < geometries.size(); iGeom++) {
      mbr.setEmpty();
      mbr.merge(geometries.get(iGeom));
      for (int d = 0; d < numDimensions; d++) {
        coords[d][iGeom] = mbr.getMinCoord(d);
        coords[d + numDimensions][iGeom] = mbr.getMaxCoord(d);
      }
    }
    return coords;
  }

  /**
   * Computes the minimum bounding rectangles of all given geometries as primitive arrays.
   * The returned 2D array stores the minimum and maximum coordinate along each dimension.
   * @param geometries the list of features to get their MBRs
   * @return the coordinates of the MBRs
   */
  public static double[][] getMBRs2(List<IFeature> geometries) {
    int numDimensions = GeometryHelper.getCoordinateDimension(geometries.get(0).getGeometry());
    double[][] coords = new double[numDimensions*2][geometries.size()];
    EnvelopeND mbr = new EnvelopeND(geometries.get(0).getGeometry().getFactory(), numDimensions);
    for (int iGeom = 0; iGeom < geometries.size(); iGeom++) {
      mbr.setEmpty();
      mbr.merge(geometries.get(iGeom).getGeometry());
      for (int d = 0; d < numDimensions; d++) {
        coords[d][iGeom] = mbr.getMinCoord(d);
        coords[d + numDimensions][iGeom] = mbr.getMaxCoord(d);
      }
    }
    return coords;
  }

  /**
   * Runs a feature-level filter-refine join while keeping geometry payloads serialized until
   * the refine step actually needs them.
   *
   * <p>This path is intended for the disk-backed execution mode. It reads MBRs from serialized
   * geometry metadata, runs plane sweep on primitive coordinate arrays, and only materializes
   * JTS geometries for candidate pairs that survive the MBR test.
   */
  public static int spatialJoinIntersectsBinaryFilterRefine(List<IFeature> r, List<IFeature> s,
                                                            SJResult results, LongAccumulator mbrTests) {
    return spatialJoinIntersectsBinaryFilterRefine(r, s, results, mbrTests, "auto", "classic", null);
  }

  /**
   * Configurable binary-first feature-level filter-refine join.
   */
  public static int spatialJoinIntersectsBinaryFilterRefine(List<IFeature> r, List<IFeature> s,
                                                            SJResult results, LongAccumulator mbrTests,
                                                            String axisMode, String implementation) {
    return spatialJoinIntersectsBinaryFilterRefine(r, s, results, mbrTests, axisMode, implementation, null);
  }

  /**
   * Configurable binary-first feature-level filter-refine join with optional reporting of the
   * effective resolved scan axis.
   */
  public static int spatialJoinIntersectsBinaryFilterRefine(List<IFeature> r, List<IFeature> s,
                                                            SJResult results, LongAccumulator mbrTests,
                                                            String axisMode, String implementation,
                                                            PlaneSweepObserver observer) {
    if (r == null || s == null || r.isEmpty() || s.isEmpty())
      return 0;

    BinaryFeatureMBRs left = extractBinaryMBRs(r);
    BinaryFeatureMBRs right = extractBinaryMBRs(s);
    if (left.size == 0 || right.size == 0)
      return 0;

    final int[] exactMatches = {0};
    planeSweepRectangles(
        new double[][]{left.minX, left.minY},
        new double[][]{left.maxX, left.maxY},
        new double[][]{right.minX, right.minY},
        new double[][]{right.maxX, right.maxY},
        (i, j, refPoint) -> {
          int leftIndex = left.originalIndexes[i];
          int rightIndex = right.originalIndexes[j];
          if (intersectsSafely(r.get(leftIndex).getGeometry(), s.get(rightIndex).getGeometry())) {
            exactMatches[0]++;
            if (results != null)
              results.accept(leftIndex, rightIndex, refPoint);
          }
        },
        mbrTests,
        axisMode,
        implementation,
        observer);
    return exactMatches[0];
  }

  /**
   * Runs an {@code intersects} join after recursively splitting large geometries into smaller pieces.
   *
   * <p>The word "simplification" here does not mean an approximate geometry simplifier such as
   * Douglas-Peucker. Instead, each input geometry is decomposed by {@link #quadSplit(Geometry, int)}
   * into a set of smaller exact pieces whose union still represents the original geometry. The goal
   * is to replace one loose MBR around a complex geometry with several tighter MBRs so that the
   * plane-sweep filter phase generates fewer false candidates.
   *
   * <p>The method then:
   * <ol>
   *   <li>builds the split pieces for the left and right inputs,</li>
   *   <li>records which original geometry each split piece came from,</li>
   *   <li>runs plane sweep on the split pieces,</li>
   *   <li>maps every matching split-piece pair back to the original geometry pair, and</li>
   *   <li>uses a duplicate-elimination set so the same original pair is only reported once.</li>
   * </ol>
   *
   * @param r the original left geometry list. Reported matches always refer back to indexes in this list.
   * @param s the original right geometry list. Reported matches always refer back to indexes in this list.
   * @param threshold the maximum allowed number of points in one split piece. Geometries with at most this
   *                  many points are kept as-is; larger geometries are recursively cut into quadrants.
   * @param results an optional callback that receives the indexes of the original left/right geometries
   *                once a pair has been proven to intersect. The callback never receives split fragments.
   * @param mbrTests an optional accumulator for counting MBR overlap tests executed during the sweep phase.
   * @return the number of distinct original geometry pairs that intersect.
   */
  public static int spatialJoinIntersectsWithSimplification(List<Geometry> r, List<Geometry> s, int threshold,
                                                            SJResult results, LongAccumulator mbrTests) {
    GeometryFactory geometryFactory = r.get(0).getFactory();
    // A set of reported results to run duplicate elimination
    Set<Long> reportedPairs = new HashSet<>();
    // Run quad split for both inputs
    List<Geometry> simplifiedR = new ArrayList<>();
    // Map each part to its main
    IntArray mappingR = new IntArray();
    for (int iGeom = 0; iGeom < r.size(); iGeom++) {
      List<Geometry> parts = quadSplitIfUsable(r.get(iGeom), threshold);
      simplifiedR.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingR.add(iGeom);
    }
    // Simplify the second dataset (s)
    List<Geometry> simplifiedS = new ArrayList<>();
    IntArray mappingS = new IntArray();
    for (int iGeom = 0; iGeom < s.size(); iGeom++) {
      List<Geometry> parts = quadSplitIfUsable(s.get(iGeom), threshold);
      simplifiedS.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingS.add(iGeom);
    }
    if (simplifiedR.isEmpty() || simplifiedS.isEmpty()) {
      return 0;
    }
    // Find overlaps between the simplified lists
    final EnvelopeND mbr1 = new EnvelopeND(geometryFactory, 2);
    final EnvelopeND mbr2 = new EnvelopeND(geometryFactory, 2);
    spatialJoinIntersectsPlaneSweep(simplifiedR, simplifiedS, (i, j, refPoint) -> {
      // Retrieve the indexes of the original geometries
      i = mappingR.get(i);
      j = mappingS.get(j);
      long pair = encodePair(i, j);
      if (!reportedPairs.contains(pair)) {
        // First time to find this pair, report it and add it to the list of reported pairs
        reportedPairs.add(pair);
        if (results != null) {
          // Compute the correct reference point based on the original polygons
          mbr1.setEmpty();
          mbr1.merge(r.get(i));
          mbr2.setEmpty();
          mbr2.merge(s.get(j));
          refPoint[0] = Math.max(mbr1.getMinCoord(0), mbr2.getMinCoord(0));
          refPoint[1] = Math.max(mbr1.getMinCoord(1), mbr2.getMinCoord(1));
          results.accept(i, j, refPoint);
        }
      }
    }, mbrTests);

    return reportedPairs.size();
  }

  /**
   * Feature-level wrapper around {@link #spatialJoinIntersectsWithSimplification(List, List, int, SJResult, LongAccumulator)}.
   *
   * <p>The join semantics are identical to the geometry-level overload, but this version accepts
   * {@link IFeature} objects and internally uses their original geometries for splitting, filtering,
   * duplicate elimination, and reference-point computation. Even though the filter phase works on
   * split geometry fragments, the callback indexes always refer back to the original feature lists
   * supplied in {@code r} and {@code s}.
   *
   * @param r the original left feature list for one partition-local join task.
   * @param s the original right feature list for one partition-local join task.
   * @param threshold the maximum point count allowed in one split fragment before recursive quadrant
   *                  decomposition is applied.
   * @param results an optional callback that receives indexes into the original feature lists. The
   *                caller can safely return or count {@code r.get(i)} and {@code s.get(j)} as the
   *                final match pair; no split fragment is exposed.
   * @param mbrTests an optional accumulator for counting plane-sweep MBR tests.
   * @return the number of distinct original feature pairs that intersect.
   */
  public static int spatialJoinIntersectsWithSimplification2(List<IFeature> r, List<IFeature> s, int threshold,
                                                            SJResult results, LongAccumulator mbrTests) {
    GeometryFactory geometryFactory = r.get(0).getGeometry().getFactory();
    // A set of reported results to run duplicate elimination
    Set<Long> reportedPairs = new HashSet<>();
    // Run quad split for both inputs
    List<Geometry> simplifiedR = new ArrayList<>();
    // Map each part to its main
    IntArray mappingR = new IntArray();
    for (int iGeom = 0; iGeom < r.size(); iGeom++) {
      List<Geometry> parts = quadSplitIfUsable(r.get(iGeom).getGeometry(), threshold);
      simplifiedR.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingR.add(iGeom);
    }
    // Simplify the second dataset (s)
    List<Geometry> simplifiedS = new ArrayList<>();
    IntArray mappingS = new IntArray();
    for (int iGeom = 0; iGeom < s.size(); iGeom++) {
      List<Geometry> parts = quadSplitIfUsable(s.get(iGeom).getGeometry(), threshold);
      simplifiedS.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingS.add(iGeom);
    }
    if (simplifiedR.isEmpty() || simplifiedS.isEmpty()) {
      return 0;
    }
    // Find overlaps between the simplified lists
    final EnvelopeND mbr1 = new EnvelopeND(geometryFactory, 2);
    final EnvelopeND mbr2 = new EnvelopeND(geometryFactory, 2);
    spatialJoinIntersectsPlaneSweep(simplifiedR, simplifiedS, (i, j, refPoint) -> {
      // Retrieve the indexes of the original geometries
      i = mappingR.get(i);
      j = mappingS.get(j);
      long pair = encodePair(i, j);
      if (!reportedPairs.contains(pair)) {
        // First time to find this pair, report it and add it to the list of reported pairs
        reportedPairs.add(pair);
        if (results != null) {
          // Compute the correct reference point based on the original polygons
          mbr1.setEmpty();
          mbr1.merge(r.get(i).getGeometry());
          mbr2.setEmpty();
          mbr2.merge(s.get(j).getGeometry());
          refPoint[0] = Math.max(mbr1.getMinCoord(0), mbr2.getMinCoord(0));
          refPoint[1] = Math.max(mbr1.getMinCoord(1), mbr2.getMinCoord(1));
          results.accept(i, j, refPoint);
        }
      }
    }, mbrTests);

    return reportedPairs.size();
  }

  /**
   * Configurable feature-level wrapper around the split-then-sweep intersects join.
   *
   * <p>The split/de-duplicate logic is identical to the baseline implementation. The only difference is
   * that the sweep phase can now choose its scan axis and implementation style.
   *
   * @param r original left features for one partition-local join task
   * @param s original right features for one partition-local join task
   * @param threshold split threshold forwarded to {@link #quadSplit(Geometry, int)}
   * @param results optional callback receiving indexes into the original feature lists
   * @param mbrTests optional accumulator for MBR test counts
   * @param axisMode configured sweep axis: x, y, auto, or diagonal
   * @param implementation configured implementation: classic or sliding-window
   * @return number of distinct original feature pairs that intersect
   */
  public static int spatialJoinIntersectsWithSimplification2(List<IFeature> r, List<IFeature> s, int threshold,
                                                             SJResult results, LongAccumulator mbrTests,
                                                             String axisMode, String implementation) {
    return spatialJoinIntersectsWithSimplification2(
        r,
        s,
        threshold,
        results,
        mbrTests,
        axisMode,
        implementation,
        null);
  }

  /**
   * Configurable feature-level wrapper around the split-then-sweep intersects join with optional
   * reporting of the effective resolved scan axis.
   */
  public static int spatialJoinIntersectsWithSimplification2(List<IFeature> r, List<IFeature> s, int threshold,
                                                             SJResult results, LongAccumulator mbrTests,
                                                             String axisMode, String implementation,
                                                             PlaneSweepObserver observer) {
    GeometryFactory geometryFactory = r.get(0).getGeometry().getFactory();
    Set<Long> reportedPairs = new HashSet<>();
    List<Geometry> simplifiedR = new ArrayList<>();
    IntArray mappingR = new IntArray();
    for (int iGeom = 0; iGeom < r.size(); iGeom++) {
      List<Geometry> parts = quadSplitIfUsable(r.get(iGeom).getGeometry(), threshold);
      simplifiedR.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingR.add(iGeom);
    }
    List<Geometry> simplifiedS = new ArrayList<>();
    IntArray mappingS = new IntArray();
    for (int iGeom = 0; iGeom < s.size(); iGeom++) {
      List<Geometry> parts = quadSplitIfUsable(s.get(iGeom).getGeometry(), threshold);
      simplifiedS.addAll(parts);
      for (int i = 0; i < parts.size(); i++)
        mappingS.add(iGeom);
    }
    if (simplifiedR.isEmpty() || simplifiedS.isEmpty()) {
      return 0;
    }
    final EnvelopeND mbr1 = new EnvelopeND(geometryFactory, 2);
    final EnvelopeND mbr2 = new EnvelopeND(geometryFactory, 2);
    spatialJoinIntersectsPlaneSweep(simplifiedR, simplifiedS, (i, j, refPoint) -> {
      i = mappingR.get(i);
      j = mappingS.get(j);
      long pair = encodePair(i, j);
      if (!reportedPairs.contains(pair)) {
        reportedPairs.add(pair);
        if (results != null) {
          mbr1.setEmpty();
          mbr1.merge(r.get(i).getGeometry());
          mbr2.setEmpty();
          mbr2.merge(s.get(j).getGeometry());
          refPoint[0] = Math.max(mbr1.getMinCoord(0), mbr2.getMinCoord(0));
          refPoint[1] = Math.max(mbr1.getMinCoord(1), mbr2.getMinCoord(1));
          results.accept(i, j, refPoint);
        }
      }
    }, mbrTests, axisMode, implementation, observer);

    return reportedPairs.size();
  }

  private static Polygon createRectangle(double minx, double miny, double maxx, double maxy, GeometryFactory f) {
    CoordinateSequence cs = f.getCoordinateSequenceFactory().create(5, 2);
    cs.setOrdinate(0, 0, minx);
    cs.setOrdinate(0, 1, miny);
    cs.setOrdinate(1, 0, maxx);
    cs.setOrdinate(1, 1, miny);
    cs.setOrdinate(2, 0, maxx);
    cs.setOrdinate(2, 1, maxy);
    cs.setOrdinate(3, 0, minx);
    cs.setOrdinate(3, 1, maxy);
    cs.setOrdinate(4, 0, minx);
    cs.setOrdinate(4, 1, miny);
    return f.createPolygon(cs);
  }

  /**
   * Recursively decomposes one geometry into smaller exact pieces by intersecting it with MBR quadrants.
   *
   * <p>This helper is used to tighten the MBRs seen by the plane-sweep filter. It is <strong>not</strong>
   * an approximate simplifier: the returned pieces are exact intersections between the original geometry
   * and recursively generated quadrant rectangles, and their union still represents the original geometry.
   *
   * <p>The algorithm works as follows:
   * <ol>
   *   <li>Start with the original geometry as the only pending piece.</li>
   *   <li>If a pending piece has at most {@code threshold} points, keep it.</li>
   *   <li>Otherwise compute that piece's MBR center, split the MBR into four rectangles, and intersect
   *       the geometry piece with each quadrant.</li>
   *   <li>Discard empty intersections and continue recursively on the non-empty ones.</li>
   * </ol>
   *
   * <p>The returned list therefore contains only non-empty 2D geometries. These pieces are used only
   * for the filter phase; callers map any hit back to the original geometry before reporting results.
   *
   * @param geometry the original 2D geometry to decompose.
   * @param threshold the maximum number of points allowed in one returned piece before further splitting.
   * @return a list of non-empty geometry pieces whose union equals the original geometry.
   */
  public static List<Geometry> quadSplit(Geometry geometry, int threshold) {
    assert GeometryHelper.getCoordinateDimension(geometry) == 2 : "This function only works with 2D geometries";
    Geometry esrGeometry = (geometry);
    List<Geometry> parts = new ArrayList<>();
    int numGeomsToCheck = 1;
    parts.add(esrGeometry);
    // Convert the threshold to an estimated memory size for simplicity to use with Esri API
    while (numGeomsToCheck > 0) {
      Geometry geomToCheck = parts.remove(0);
      numGeomsToCheck--;
      if (geomToCheck.getNumPoints() <= threshold) {
        // Already simple. Add to results
        appendUsableGeometry(parts, geomToCheck);
      } else {
        // A complex geometry, split into four
        Envelope mbr = geomToCheck.getEnvelopeInternal();
        double centerx = (mbr.getMinX() + mbr.getMaxX()) / 2.0;
        double centery = (mbr.getMinY() + mbr.getMaxY()) / 2.0;
        // First quadrant
        Polygon quadrant = createRectangle(mbr.getMinX(), mbr.getMinY(), centerx, centery, geomToCheck.getFactory());
        if (appendUsableGeometry(parts, safeIntersection(geomToCheck, quadrant)))
          numGeomsToCheck++;
        // Second quadrant
        quadrant = createRectangle(centerx, mbr.getMinY(), mbr.getMaxX(), centery, geomToCheck.getFactory());
        if (appendUsableGeometry(parts, safeIntersection(geomToCheck, quadrant)))
          numGeomsToCheck++;
        // Third quadrant
        quadrant = createRectangle(centerx, centery, mbr.getMaxX(), mbr.getMaxY(), geomToCheck.getFactory());
        if (appendUsableGeometry(parts, safeIntersection(geomToCheck, quadrant)))
          numGeomsToCheck++;
        // Fourth quadrant
        quadrant = createRectangle(mbr.getMinX(), centery, centerx, mbr.getMaxY(), geomToCheck.getFactory());
        if (appendUsableGeometry(parts, safeIntersection(geomToCheck, quadrant)))
          numGeomsToCheck++;
      }
    }
    // Convert all parts back to lite geometry
    List<Geometry> results = new ArrayList<>();
    for (int i = 0; i < parts.size(); i++) {
      results.add((parts.get(i)));
    }
    return results;
  }

  /**
   * Quad-splits a geometry with hard safety guards for the preparation-time split path.
   *
   * <p>When the fragment budget or depth budget would be exceeded, the current geometry is kept as
   * one final fragment instead of being split further. This avoids runaway fragmentation while
   * preserving input coverage.
   */
  public static List<Geometry> quadSplitBounded(Geometry geometry,
                                                int threshold,
                                                int maxFragments,
                                                int maxDepth) {
    if (!isUsable2DGeometry(geometry))
      return Collections.emptyList();
    int safeThreshold = Math.max(1, threshold);
    int safeMaxFragments = Math.max(1, maxFragments);
    int safeMaxDepth = Math.max(0, maxDepth);
    List<Geometry> results = new ArrayList<>();
    Deque<GeometrySplitNode> pending = new ArrayDeque<>();
    pending.addLast(new GeometrySplitNode(geometry, 0));
    while (!pending.isEmpty()) {
      GeometrySplitNode node = pending.removeFirst();
      Geometry geomToCheck = node.geometry;
      if (!isUsable2DGeometry(geomToCheck))
        continue;
      if (geomToCheck.getNumPoints() <= safeThreshold ||
          node.depth >= safeMaxDepth ||
          results.size() + pending.size() + 1 >= safeMaxFragments) {
        appendUsableGeometry(results, geomToCheck);
        continue;
      }
      List<Geometry> children = splitIntoQuadrants(geomToCheck);
      if (children.isEmpty() ||
          results.size() + pending.size() + children.size() > safeMaxFragments) {
        appendUsableGeometry(results, geomToCheck);
        continue;
      }
      for (Geometry child : children) {
        pending.addLast(new GeometrySplitNode(child, node.depth + 1));
      }
    }
    return results;
  }

  private static List<Geometry> splitIntoQuadrants(Geometry geometry) {
    List<Geometry> children = new ArrayList<>(4);
    Envelope mbr = geometry.getEnvelopeInternal();
    double centerx = (mbr.getMinX() + mbr.getMaxX()) / 2.0;
    double centery = (mbr.getMinY() + mbr.getMaxY()) / 2.0;
    Polygon quadrant = createRectangle(mbr.getMinX(), mbr.getMinY(), centerx, centery, geometry.getFactory());
    appendUsableGeometry(children, safeIntersection(geometry, quadrant));
    quadrant = createRectangle(centerx, mbr.getMinY(), mbr.getMaxX(), centery, geometry.getFactory());
    appendUsableGeometry(children, safeIntersection(geometry, quadrant));
    quadrant = createRectangle(centerx, centery, mbr.getMaxX(), mbr.getMaxY(), geometry.getFactory());
    appendUsableGeometry(children, safeIntersection(geometry, quadrant));
    quadrant = createRectangle(mbr.getMinX(), centery, centerx, mbr.getMaxY(), geometry.getFactory());
    appendUsableGeometry(children, safeIntersection(geometry, quadrant));
    return children;
  }

  private static final class GeometrySplitNode {
    private final Geometry geometry;
    private final int depth;

    private GeometrySplitNode(Geometry geometry, int depth) {
      this.geometry = geometry;
      this.depth = depth;
    }
  }

  /** Returns true only for non-empty 2D geometries that can participate in plane sweep. */
  private static boolean isUsable2DGeometry(Geometry geometry) {
    return geometry != null && !geometry.isEmpty() && GeometryHelper.getCoordinateDimension(geometry) == 2;
  }

  /** Returns false instead of propagating topology failures during the exact refine step. */
  private static boolean intersectsSafely(Geometry left, Geometry right) {
    if (!isUsable2DGeometry(left) || !isUsable2DGeometry(right)) {
      return false;
    }
    try {
      return left.intersects(right);
    } catch (RuntimeException e) {
      return false;
    }
  }

  /** Skips null/empty/non-2D source geometries before the split-based filter phase starts. */
  private static List<Geometry> quadSplitIfUsable(Geometry geometry, int threshold) {
    if (!isUsable2DGeometry(geometry)) {
      return Collections.emptyList();
    }
    return quadSplit(geometry, threshold);
  }

  /** Returns null instead of bubbling up topology failures while clipping split quadrants. */
  private static Geometry safeIntersection(Geometry geometry, Geometry clip) {
    if (geometry == null || clip == null) {
      return null;
    }
    try {
      return geometry.intersection(clip);
    } catch (RuntimeException e) {
      return null;
    }
  }

  /** Appends a geometry only when it survives clipping as a non-empty 2D shape. */
  private static boolean appendUsableGeometry(List<Geometry> target, Geometry geometry) {
    if (!isUsable2DGeometry(geometry)) {
      return false;
    }
    target.add(geometry);
    return true;
  }

  /** Drops empty split fragments so downstream MBR code always sees usable 2D geometries. */
  private static List<Geometry> retainUsable2DGeometries(List<Geometry> geometries) {
    List<Geometry> filtered = null;
    for (int i = 0; i < geometries.size(); i++) {
      Geometry geometry = geometries.get(i);
      if (!isUsable2DGeometry(geometry)) {
        if (filtered == null) {
          filtered = new ArrayList<>(geometries.size() - 1);
          for (int j = 0; j < i; j++) {
            filtered.add(geometries.get(j));
          }
        }
      } else if (filtered != null) {
        filtered.add(geometry);
      }
    }
    return filtered == null ? geometries : filtered;
  }

  private static BinaryFeatureMBRs extractBinaryMBRs(List<IFeature> features) {
    List<BinaryFeatureMBR> usable = new ArrayList<>(features.size());
    for (int i = 0; i < features.size(); i++) {
      BinaryFeatureMBR mbr = extractBinaryMBR(features.get(i), i);
      if (mbr != null)
        usable.add(mbr);
    }
    if (usable.isEmpty())
      return BinaryFeatureMBRs.empty();

    int size = usable.size();
    int[] originalIndexes = new int[size];
    double[] minX = new double[size];
    double[] minY = new double[size];
    double[] maxX = new double[size];
    double[] maxY = new double[size];
    for (int i = 0; i < size; i++) {
      BinaryFeatureMBR mbr = usable.get(i);
      originalIndexes[i] = mbr.originalIndex;
      minX[i] = mbr.minX;
      minY[i] = mbr.minY;
      maxX[i] = mbr.maxX;
      maxY[i] = mbr.maxY;
    }
    return new BinaryFeatureMBRs(originalIndexes, minX, minY, maxX, maxY);
  }

  private static BinaryFeatureMBR extractBinaryMBR(IFeature feature, int originalIndex) {
    if (feature == null)
      return null;
    byte[] geometryBytes = feature.getGeometryBytes();
    if (geometryBytes != null && geometryBytes.length > 0) {
      GeometryMetadataReader.Metadata metadata =
          GeometryMetadataReader.read(geometryBytes, GeometryMetadataReader.DetailLevel.SUMMARY);
      if (metadata == null || metadata.isEmpty() || metadata.getCoordinateDimension() < 2)
        return null;
      EnvelopeNDLite envelope = metadata.getEnvelope();
      if (envelope == null || envelope.isEmpty() || envelope.getCoordinateDimension() < 2)
        return null;
      return new BinaryFeatureMBR(
          originalIndex,
          envelope.getMinCoord(0),
          envelope.getMinCoord(1),
          envelope.getMaxCoord(0),
          envelope.getMaxCoord(1));
    }

    Geometry geometry = feature.getGeometry();
    if (!isUsable2DGeometry(geometry))
      return null;
    Envelope envelope = geometry.getEnvelopeInternal();
    return new BinaryFeatureMBR(
        originalIndex,
        envelope.getMinX(),
        envelope.getMinY(),
        envelope.getMaxX(),
        envelope.getMaxY());
  }

  private static boolean envelopesIntersect(double leftMinX,
                                            double leftMinY,
                                            double leftMaxX,
                                            double leftMaxY,
                                            double rightMinX,
                                            double rightMinY,
                                            double rightMaxX,
                                            double rightMaxY) {
    return leftMinX <= rightMaxX &&
        leftMaxX >= rightMinX &&
        leftMinY <= rightMaxY &&
        leftMaxY >= rightMinY;
  }

  private static double[] computeReferencePoint(double leftMinX,
                                                double leftMinY,
                                                double leftMaxX,
                                                double leftMaxY,
                                                double rightMinX,
                                                double rightMinY,
                                                double rightMaxX,
                                                double rightMaxY) {
    return new double[]{
        Math.max(leftMinX, rightMinX),
        Math.max(leftMinY, rightMinY)
    };
  }

  /** Encodes one left/right index pair without collisions when indexes exceed 5 bits. */
  private static long encodePair(int leftIndex, int rightIndex) {
    return (((long) leftIndex) << 32) | (rightIndex & 0xffffffffL);
  }

  private static final class BinaryFeatureMBRs {
    private static final BinaryFeatureMBRs EMPTY =
        new BinaryFeatureMBRs(new int[0], new double[0], new double[0], new double[0], new double[0]);

    private final int[] originalIndexes;
    private final double[] minX;
    private final double[] minY;
    private final double[] maxX;
    private final double[] maxY;
    private final int size;

    private BinaryFeatureMBRs(int[] originalIndexes, double[] minX, double[] minY, double[] maxX, double[] maxY) {
      this.originalIndexes = originalIndexes;
      this.minX = minX;
      this.minY = minY;
      this.maxX = maxX;
      this.maxY = maxY;
      this.size = originalIndexes == null ? 0 : originalIndexes.length;
    }

    private static BinaryFeatureMBRs empty() {
      return EMPTY;
    }
  }

  private static final class BinaryFeatureMBR {
    private final int originalIndex;
    private final double minX;
    private final double minY;
    private final double maxX;
    private final double maxY;

    private BinaryFeatureMBR(int originalIndex, double minX, double minY, double maxX, double maxY) {
      this.originalIndex = originalIndex;
      this.minX = minX;
      this.minY = minY;
      this.maxX = maxX;
      this.maxY = maxY;
    }
  }

}
