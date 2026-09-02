package cn.edu.whu.spatialJoin.cost;

import cn.edu.whu.spatialJoin.enums.PartitionComplexityType;

import java.io.Serializable;

/**
 * Computation-intensity score for one effective partition profile.
 */
public final class PartCost implements Comparable<PartCost>, Serializable {

  private static final long serialVersionUID = 1L;

  private final PartProfile profile;
  private final double score;
  private final PartitionComplexityType complexityType;

  public PartCost(PartProfile profile, double score) {
    this(profile, score, PartitionComplexityType.NORMAL);
  }

  public PartCost(PartProfile profile, double score, PartitionComplexityType complexityType) {
    this.profile = profile;
    this.score = Math.max(0.0d, score);
    this.complexityType = complexityType == null ? PartitionComplexityType.NORMAL : complexityType;
  }

  /** Returns the underlying per-partition execution profile. */
  public PartProfile getProfile() {
    return profile;
  }

  /** Returns the computed cost score used during job grouping. */
  public double getScore() {
    return score;
  }

  /** Returns the complexity label assigned after threshold classification. */
  public PartitionComplexityType getComplexityType() {
    return complexityType;
  }

  /** Returns a copy of this score object with a new complexity label. */
  public PartCost withComplexityType(PartitionComplexityType type) {
    return new PartCost(profile, score, type);
  }

  /** Sorts partitions from highest to lowest score, then by partition id. */
  @Override
  public int compareTo(PartCost other) {
    int scoreCompare = Double.compare(other.score, this.score);
    if (scoreCompare != 0) {
      return scoreCompare;
    }
    return Integer.compare(this.profile.getPartitionId(), other.profile.getPartitionId());
  }
}
