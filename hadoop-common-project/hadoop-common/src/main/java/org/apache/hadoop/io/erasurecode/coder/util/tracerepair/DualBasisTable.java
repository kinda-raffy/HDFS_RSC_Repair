package org.apache.hadoop.io.erasurecode.coder.util.tracerepair;

public class DualBasisTable {
    private final byte[][] Scheme_9_6 = {
        {78, -109, -44, 102, 26, -82, -72, -101},
        {-127, -115, -59, 81, 107, -27, 10, 79},
        {36, -96, 25, -55, 100, 3, 82, -13},
        {97, -86, -57, -32, -102, -11, 51, 89},
        {-88, 122, 63, -40, 56, -64, 116, -30},
        {10, 79, -23, 15, 3, 103, -122, -107},
        {-14, 119, -74, -85, -76, 26, 78, -109},
        {123, 4, 33, 9, 124, 28, -14, 119},
        { -32, 39, 117, 52, -33, -1, 96, 124 }
    };

    public byte[] getRow_9_6(int row) {
        return Scheme_9_6[row];
    }

    public byte getByte_9_6(int row, int column) {
        return Scheme_9_6[row][column];
    }
}
