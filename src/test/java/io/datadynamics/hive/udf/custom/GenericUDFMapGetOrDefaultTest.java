package io.datadynamics.hive.udf.custom;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class GenericUDFMapGetOrDefaultTest {

    /**
     * 편의 메서드: Standard Map OI 생성
     */
    private MapObjectInspector mapOI(ObjectInspector keyOI, ObjectInspector valOI) {
        return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valOI);
    }

    /**
     * 편의 메서드: UDF 초기화
     */
    private GenericUDFMapGetOrDefault initUDF(ObjectInspector mapOI, ObjectInspector keyArgOI, ObjectInspector defArgOI) throws Exception {
        GenericUDFMapGetOrDefault udf = new GenericUDFMapGetOrDefault();
        udf.initialize(new ObjectInspector[]{mapOI, keyArgOI, defArgOI});
        return udf;
    }

    @Test
    public void stringString_hit_returnsValue() throws Exception {
        // map<string,string>
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI, keyOI, valOI);

        Map<String, String> m = new HashMap<>();
        m.put("UNITNM", "KG");

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject("None")
        });

        assertEquals("KG", out);
    }

    @Test
    public void stringString_miss_returnsDefault() throws Exception {
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI, keyOI, valOI);

        Map<String, String> m = new HashMap<>();
        m.put("X", "Y");

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject("None")
        });

        assertEquals("None", out);
    }

    @Test
    public void nullMap_returnsDefault() throws Exception {
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI, keyOI, valOI);

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(null),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject("None")
        });

        assertEquals("None", out);
    }

    @Test
    public void presentButNull_returnsDefault() throws Exception {
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI, keyOI, valOI);

        Map<String, String> m = new HashMap<>();
        m.put("UNITNM", null);

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject("None")
        });

        assertEquals("None", out);
    }

    @Test
    public void intString_valueInt_defaultFromStringConvertible() throws Exception {
        // map<string,int>
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        // key arg: string, default arg: string (e.g., "42")
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        Map<String, Integer> m = new HashMap<>();
        // intentionally leave empty -> miss -> default "42" -> converted to Integer 42
        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject("42")
        });

        assertTrue(out instanceof Integer);
        assertEquals(42, ((Integer) out).intValue());
    }

    @Test
    public void intString_valueInt_defaultNotConvertible_returnsNull() throws Exception {
        // map<string,int>
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        // default arg: string "None" -> cannot convert to int => NULL default
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        Map<String, Integer> m = new HashMap<>();
        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject("None")
        });

        assertNull(out); // default가 변환 실패 => NULL
    }

    @Test
    public void numericKeyCoercion_stringToIntKey() throws Exception {
        // map<int,string>, key arg는 string("10") -> int 10으로 변환되어 조회
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        Map<Integer, String> m = new HashMap<>();
        m.put(10, "TEN");

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("10"),
                new DeferredJavaObject("None")
        });

        assertEquals("TEN", out);
    }

    @Test
    public void numericKeyCoercion_fail_returnsDefault() throws Exception {
        // map<int,string>, key arg "abc" -> int 변환 실패 => default
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        Map<Integer, String> m = new HashMap<>();
        m.put(10, "TEN");

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("abc"),
                new DeferredJavaObject("None")
        });

        assertEquals("None", out);
    }

    @Test
    public void defaultIsNull_returnsNullOnMiss() throws Exception {
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI, keyOI, valOI);

        Map<String, String> m = new HashMap<>();

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject(null) // default null
        });

        assertNull(out);
    }

    @Test
    public void decimalValueAndDefault() throws Exception {
        // map<string,decimal>
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        // default arg: string "0.50"
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        Map<String, HiveDecimal> m = new HashMap<>();
        // miss -> default 0.50
        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject("0.5")
        });

        assertTrue(out instanceof HiveDecimal);
        assertEquals("0.5", ((HiveDecimal) out).toString());
    }

    @Test
    public void decimalHitOverridesDefault() throws Exception {
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        Map<String, HiveDecimal> m = new HashMap<>();
        m.put("UNITNM", HiveDecimal.create("12.34"));

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject("0.50")
        });

        assertTrue(out instanceof HiveDecimal);
        assertEquals("12.34", ((HiveDecimal) out).toString());
    }

    @Test
    public void dateValueAndDefault() throws Exception {
        // map<string,date>
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaDateObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector); // default as string

        Map<String, Date> m = new HashMap<>();
        // miss -> default "2024-12-31"
        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("D"),
                new DeferredJavaObject("2024-12-31")
        });

        assertTrue(out instanceof org.apache.hadoop.hive.common.type.Date);
        assertEquals("2024-12-31", out.toString());
    }

    @Test
    public void timestampValueAndDefault() throws Exception {
        // map<string,timestamp>
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        Map<String, Timestamp> m = new HashMap<>();
        // miss -> default "2024-01-02 03:04:05"
        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("TS"),
                new DeferredJavaObject("2024-01-02 03:04:05")
        });

        assertTrue(out instanceof org.apache.hadoop.hive.common.type.Timestamp);
        assertEquals("2024-01-02 03:04:05", out.toString());
    }

    @Test
    public void defaultNumericToStringWhenValueTypeIsString() throws Exception {
        // map<string,string>, default가 숫자 0 -> "0"으로 변환되어 반환
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector); // default INT

        Map<String, String> m = new HashMap<>();

        Object out = udf.evaluate(new GenericUDF.DeferredObject[]{
                new DeferredJavaObject(m),
                new DeferredJavaObject("UNITNM"),
                new DeferredJavaObject(0) // default int
        });

        assertEquals("0", out);
    }

    @Test
    public void initializeRejectsNonMapFirstArg() {
        try {
            ObjectInspector notMap = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            new GenericUDFMapGetOrDefault().initialize(new ObjectInspector[]{
                    notMap,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector
            });
            fail("Expected UDFArgumentTypeException for non-map first argument");
        } catch (Exception expected) {
            // pass
        }
    }

    @Test
    public void initializeRejectsNonPrimitiveMapKeyOrValue() {
        try {
            // map<array<int>, string> : key가 비-primitive
            ObjectInspector badKeyOI = ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            MapObjectInspector mOI = mapOI(badKeyOI, valOI);

            new GenericUDFMapGetOrDefault().initialize(new ObjectInspector[]{
                    mOI,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector
            });
            fail("Expected UDFArgumentTypeException for non-primitive map key");
        } catch (Exception expected) {
            // pass
        }
    }

    @Test
    public void displayString_isFriendly() throws Exception {
        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        MapObjectInspector mOI = mapOI(keyOI, valOI);
        GenericUDFMapGetOrDefault udf = initUDF(mOI, keyOI, valOI);

        String s = udf.getDisplayString(new String[]{"m", "k", "d"});
        assertTrue(s.contains("map_get_or_default"));
    }
}