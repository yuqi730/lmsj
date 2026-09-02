package cn.edu.whu.spatialJoin.utils;

public final class MathUtil {

    private MathUtil() {
    }

    public static int nextPowerOfTwo(int value) {
        if (value <= 1) {
            return 1;
        }
        return Integer.highestOneBit(value - 1) << 1;
    }
}
