package io.datadynamics.hive.udf.fastuitl.ints;

import java.util.Comparator;

public interface IntComparator extends Comparator<Integer> {
    int compare(int var1, int var2);
}
