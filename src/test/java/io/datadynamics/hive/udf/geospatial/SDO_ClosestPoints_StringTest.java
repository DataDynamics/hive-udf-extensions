package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SDO_ClosestPoints_StringTest {

    private SDO_ClosestPoints_String udf;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_ClosestPoints_String();
        ObjectInspector[] arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_ValidWKT() throws HiveException {
        String wkt1 = "POINT (0 0)";
        String wkt2 = "POINT (10 0)";

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(wkt1),
                new GenericUDF.DeferredJavaObject(wkt2)
        };
        String result = (String) udf.evaluate(args);

        assertNotNull(result);
        assertTrue(result.contains("LINESTRING"));
        assertTrue(result.contains("0 0"));
        assertTrue(result.contains("10 0"));
    }

    @Test
    public void testEvaluate_LineAndPoint() throws HiveException {
        String wkt1 = "LINESTRING (0 0, 10 0)";
        String wkt2 = "POINT (5 5)";

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(wkt1),
                new GenericUDF.DeferredJavaObject(wkt2)
        };
        String result = (String) udf.evaluate(args);

        assertNotNull(result);
        // (5,0)과 (5,5) 사이의 최단 거리
        assertTrue(result.contains("5 0"));
        assertTrue(result.contains("5 5"));
    }

    @Test
    public void testEvaluate_NullOrEmpty() throws HiveException {
        assertNull(udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject("POINT (0 0)")
        }));
        assertNull(udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject("POINT (0 0)"),
                new GenericUDF.DeferredJavaObject(null)
        }));
        assertNull(udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(""),
                new GenericUDF.DeferredJavaObject("POINT (0 0)")
        }));
        assertNull(udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject("POINT (0 0)"),
                new GenericUDF.DeferredJavaObject("")
        }));
    }

    @Test
    public void testEvaluate_InvalidWKT() throws HiveException {
        String invalidWkt = "INVALID(0 0)";
        String validWkt = "POINT (10 0)";

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(invalidWkt),
                new GenericUDF.DeferredJavaObject(validWkt)
        };
        assertNull(udf.evaluate(args));
    }

    @Test
    public void testEvaluate_3D() throws HiveException {
        // JTS DistanceOp는 기본적으로 2D 평면 거리 기준이지만, GeometryUtils에서 3D WKT를 지원하는지 확인
        String wkt1 = "POINT Z (0 0 0)";
        String wkt2 = "POINT Z (10 0 0)";

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(wkt1),
                new GenericUDF.DeferredJavaObject(wkt2)
        };
        String result = (String) udf.evaluate(args);

        assertNotNull(result);
        // GeometryUtils.geometryToString(3)이 적용되어 Z 좌표가 보존되는지 확인
        assertTrue(result.contains("0 0 0"));
        assertTrue(result.contains("10 0 0"));
    }
}
