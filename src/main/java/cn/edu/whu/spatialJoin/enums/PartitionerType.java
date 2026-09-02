package cn.edu.whu.spatialJoin.enums;

import java.io.Serializable;

/**
 * The Enum GridType.
 */
public enum PartitionerType
        implements Serializable {
    /**
     * EQUAL GRID partitioning
     */
    EQUALGRID,

    /**
     * Uniform grid partitioning
     */
    GRID,

    /**
     * Hilbert-curve partitioning
     */
    HCURVE,

    /**
     * Sort-tile-recursive partitioning
     */
    STR,

    /**
     * Historical enum name kept for the project's KD-tree partitioner.
     *
     * <p>The current codebase uses a KD-tree partitioner implementation, while older notes
     * and external systems sometimes refer to the same family as KDB-tree.
     */
    KDBTREE,

    /**
     * Load-aware Hilbert partitioning
     */
    LAHILBERT;

    public static PartitionerType fromName(String str) {
        if (str == null || str.trim().isEmpty()) {
            return null;
        }
        String normalized = str.trim().replace("-", "").replace("_", "").toUpperCase();
        switch (normalized) {
            case "EQUALGRID":
            case "GRID":
                return GRID;
            case "HCURVE":
            case "HILBERT":
                return HCURVE;
            case "STR":
                return STR;
            case "LAHILBERT":
            case "LOADAWAREHILBERT":
                return LAHILBERT;
            case "KDBTREE":
            case "KDTREE":
                return KDBTREE;
            default:
                return null;
        }
    }
}
