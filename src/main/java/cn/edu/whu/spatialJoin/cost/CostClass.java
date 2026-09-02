package cn.edu.whu.spatialJoin.cost;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * High/normal complexity split result for effective partitions.
 */
public final class CostClass implements Serializable {

  private static final long serialVersionUID = 1L;

  private final double medianIntensity;
  private final double quantileIntensity;
  private final double threshold;
  private final List<PartCost> highCosts;
  private final List<PartCost> normalCosts;

  public CostClass(double medianIntensity,
                   double quantileIntensity,
                   double threshold,
                   List<PartCost> highCosts,
                   List<PartCost> normalCosts) {
    this.medianIntensity = medianIntensity;
    this.quantileIntensity = quantileIntensity;
    this.threshold = threshold;
    this.highCosts = Collections.unmodifiableList(new ArrayList<>(highCosts));
    this.normalCosts = Collections.unmodifiableList(new ArrayList<>(normalCosts));
  }

  /** Returns the median partition cost used by the classifier. */
  public double getMedianIntensity() {
    return medianIntensity;
  }

  /** Returns the configured quantile value used by the classifier. */
  public double getQuantileIntensity() {
    return quantileIntensity;
  }

  /** Returns the final threshold used to split HIGH and NORMAL partitions. */
  public double getThreshold() {
    return threshold;
  }

  /** Returns the partitions classified as HIGH complexity. */
  public List<PartCost> getHighCosts() {
    return highCosts;
  }

  /** Returns the partitions classified as NORMAL complexity. */
  public List<PartCost> getNormalCosts() {
    return normalCosts;
  }
}
