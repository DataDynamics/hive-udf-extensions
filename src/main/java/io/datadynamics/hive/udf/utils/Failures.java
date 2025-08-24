package io.datadynamics.hive.udf.utils;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import static java.lang.String.format;

public class Failures {
    private Failures() {
    }

    public static void checkCondition(boolean condition, String formatString, Object... args) throws HiveException {
        if (!condition) {
            throw new HiveException(format(formatString, args));
        }
    }
}
