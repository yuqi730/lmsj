package cn.edu.whu.spatialJoin.operator;

import cn.edu.whu.spatialJoin.core.partitioning.GridPartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.HCurvePartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.KDTreePartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.LAHilbertPartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.SpatialPartitioner;
import cn.edu.whu.spatialJoin.core.partitioning.STRPartitioner;
import cn.edu.whu.spatialJoin.enums.PartitionerType;

/**
 * Central factory for turning a configured partitioner name into a concrete implementation.
 *
 * <p>The factory deliberately supports only the partitioners that are already wired into
 * the current project. Unsupported historical names are rejected explicitly.
 */
public final class SpatialPartitionerFactory {

  private SpatialPartitionerFactory() {
  }

  /**
   * Resolves a configured partitioner name and creates the corresponding implementation.
   *
   * @param typeName configured partitioner name
   * @return a new partitioner instance
   */
  public static SpatialPartitioner create(String typeName) {
    PartitionerType type = PartitionerType.fromName(typeName);
    if (type == null) {
      throw new IllegalArgumentException("Unsupported partitioner type: " + typeName);
    }
    return create(type);
  }

  /**
   * Creates a concrete partitioner for the resolved enum value.
   *
   * @param type resolved partitioner type
   * @return a new partitioner instance
   */
  public static SpatialPartitioner create(PartitionerType type) {
    switch (type) {
      case GRID:
      case EQUALGRID:
        return new GridPartitioner();
      case HCURVE:
        return new HCurvePartitioner();
      case STR:
        return new STRPartitioner();
      case KDBTREE:
        return new KDTreePartitioner();
      case LAHILBERT:
        return new LAHilbertPartitioner();
      default:
        throw new IllegalArgumentException("Partitioner is not implemented in the current project: " + type);
    }
  }
}
