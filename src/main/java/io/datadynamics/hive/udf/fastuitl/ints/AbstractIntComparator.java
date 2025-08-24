package io.datadynamics.hive.udf.fastuitl.ints;

public abstract class AbstractIntComparator implements IntComparator {

    protected AbstractIntComparator() {
    }

    public int compare(Integer ok1, Integer ok2) {
        return this.compare(ok1.intValue(), ok2.intValue());
    }

    public abstract int compare(int var1, int var2);
}
