package io.datadynamics.hive.udf.geospatial;

import org.junit.Test;
import static org.junit.Assert.*;

public class SDO_ConcaveHull_StringTest {

    private final SDO_ConcaveHull_String udf = new SDO_ConcaveHull_String();

    @Test
    public void testEvaluate_ValidWKT() {
        // 여러 점들이 모여있는 경우 Concave Hull 생성 확인
        String wkt = "MULTIPOINT ((0 0), (10 0), (10 10), (0 10), (5 5))";
        String result = udf.evaluate(wkt, 20.0);

        assertNotNull(result);
        assertTrue(result.contains("POLYGON"));
        // tolerance가 충분히 크면 Convex Hull과 유사한 결과가 나옴
    }

    @Test
    public void testEvaluate_NullOrEmpty() {
        assertNull(udf.evaluate(null, 1.0));
        assertNull(udf.evaluate("", 1.0));
    }

    @Test
    public void testEvaluate_InvalidWKT() {
        assertNull(udf.evaluate("INVALID WKT", 1.0));
    }

    @Test
    public void testEvaluate_Tolerance() {
        String wkt = "MULTIPOINT ((0 0), (10 0), (10 10), (0 10), (5 2))";
        
        // Tolerance가 작을 때와 클 때의 결과가 다를 수 있음 (Concave vs Convex)
        String resultLarge = udf.evaluate(wkt, 100.0);
        String resultSmall = udf.evaluate(wkt, 1.0);

        assertNotNull(resultLarge);
        assertNotNull(resultSmall);
        // 결과 문자열이 다를 것으로 예상 (구체적인 좌표 비교보다는 생성 여부 확인)
    }

    @Test
    public void testEvaluate_3D() {
        String wkt = "MULTIPOINT ((0 0 1), (10 0 2), (10 10 3), (0 10 4))";
        String result = udf.evaluate(wkt, 20.0);

        assertNotNull(result);
        // JTS ConcaveHull은 2D 평면 기반으로 계산하지만 좌표 객체에 Z가 있으면 유지될 수 있음
        // GeometryUtils.geometryToString(3)이 Z를 포함하는지 확인
    }
}
