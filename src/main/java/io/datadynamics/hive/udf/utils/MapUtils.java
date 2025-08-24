package io.datadynamics.hive.udf.utils;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.Map;


public final class MapUtils {
    public static <K, V> boolean mapEquals(Map<K, V> left, Map<K, V> right) {
        if (left == null || right == null) {
            return left == null && right == null;
        }

        if (left.size() != right.size()) {
            return false;
        }

        for (K key : left.keySet()) {
            if (!left.get(key).equals(right.get(key))) {
                return false;
            }
        }

        return true;
    }

    public static boolean mapEquals(Map left, Map right, ObjectInspector valueOI) {
        if (left == null || right == null) {
            return left == null && right == null;
        }

        if (left.size() != right.size()) {
            return false;
        }

        for (Object key : left.keySet()) {
            if (ObjectInspectorUtils.compare(left.get(key), valueOI, right.get(key), valueOI) != 0) {
                return false;
            }
        }

        return true;
    }
}
