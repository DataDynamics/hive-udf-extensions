package io.datadynamics.hive.udf.geospatial;

import org.junit.Test;
import static org.junit.Assert.*;

public class SDO_ClosestPoints_StringTest {

    private final SDO_ClosestPoints_String udf = new SDO_ClosestPoints_String();

    @Test
    public void testEvaluate_ValidWKT() {
        String wkt1 = "POINT (0 0)";
        String wkt2 = "POINT (10 0)";
        
        String result = udf.evaluate(wkt1, wkt2);
        
        assertNotNull(result);
        assertTrue(result.contains("LINESTRING"));
        assertTrue(result.contains("0 0"));
        assertTrue(result.contains("10 0"));
    }

    @Test
    public void testEvaluate_LineAndPoint() {
        String wkt1 = "LINESTRING (0 0, 10 0)";
        String wkt2 = "POINT (5 5)";
        
        String result = udf.evaluate(wkt1, wkt2);
        
        assertNotNull(result);
        // (5,0)과 (5,5) 사이의 최단 거리
        assertTrue(result.contains("5 0"));
        assertTrue(result.contains("5 5"));
    }

    @Test
    public void testEvaluate_NullOrEmpty() {
        assertNull(udf.evaluate(null, "POINT (0 0)"));
        assertNull(udf.evaluate("POINT (0 0)", null));
        assertNull(udf.evaluate("", "POINT (0 0)"));
        assertNull(udf.evaluate("POINT (0 0)", ""));
    }

    @Test
    public void testEvaluate_InvalidWKT() {
        String invalidWkt = "INVALID(0 0)";
        String validWkt = "POINT (10 0)";
        
        assertNull(udf.evaluate(invalidWkt, validWkt));
    }

    @Test
    public void testEvaluate_3D() {
        // JTS DistanceOp는 기본적으로 2D 평면 거리 기준이지만, GeometryUtils에서 3D WKT를 지원하는지 확인
        String wkt1 = "POINT Z (0 0 0)";
        String wkt2 = "POINT Z (10 0 0)";
        
        String result = udf.evaluate(wkt1, wkt2);
        
        assertNotNull(result);
        // GeometryUtils.geometryToString(3)이 적용되어 Z 좌표가 보존되는지 확인
        assertTrue(result.contains("0 0 0"));
        assertTrue(result.contains("10 0 0"));
    }
}
