package org.apache.hadoop.io.erasurecode.coder.util.tracerepair;

import java.util.ArrayList;
import java.util.List;

public class DualBasisTable96String {
    private static List<String> DBTable_9_6 = new ArrayList<>();

    public DualBasisTable96String() {
        DBTable_9_6.add("78, 147, 212, 102, 26, 174, 184, 155");
        DBTable_9_6.add("129, 141, 197, 81, 107, 229, 10, 79");
        DBTable_9_6.add("36, 160, 25, 201, 100, 3, 82, 243");
        DBTable_9_6.add("97, 170, 199, 224, 154, 245, 51, 89");
        DBTable_9_6.add("168, 122, 63, 216, 56, 192, 116, 226");
        DBTable_9_6.add("10, 79, 233, 15, 3, 103, 134, 149");
        DBTable_9_6.add("242, 119, 182, 171, 180, 26, 78, 147");
        DBTable_9_6.add("123, 4, 33, 9, 124, 28, 242, 119");
        DBTable_9_6.add("224, 39, 117, 52, 223, 255, 96, 124");
    }

    public Object getElement(int i){
        return DBTable_9_6.get(i);
    }
}