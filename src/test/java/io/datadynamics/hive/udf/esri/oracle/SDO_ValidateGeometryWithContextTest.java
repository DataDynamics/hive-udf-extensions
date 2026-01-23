package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCPolygon;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SDO_ValidateGeometryWithContextTest {

    private SDO_ValidateGeometryWithContext udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_ValidateGeometryWithContext();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_ValidPolygon() {
        // OGC 기준에 맞는 유효한 폴리곤 (정규화된 형태)
        String wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
        OGCPolygon ogcPoly = (OGCPolygon) OGCPolygon.fromText(wkt);
        ogcPoly.setSpatialReference(sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        String result = udf.evaluate(geom);
        assertEquals("TRUE", result);
    }

    @Test
    public void testEvaluate_SelfIntersectingPolygon() {
        // 8자 형태의 자가 교차 폴리곤 WKT
        String wkt = "POLYGON ((0 0, 10 10, 0 10, 10 0, 0 0))";
        OGCPolygon ogcPoly = (OGCPolygon) OGCPolygon.fromText(wkt);
        ogcPoly.setSpatialReference(sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        String result = udf.evaluate(geom);
        System.out.println("Result: " + result);
        assertTrue(result.startsWith("FALSE:"));
    }

    @Test
    public void testEvaluate_NullInput() {
        String result = udf.evaluate(null);
        assertEquals("NULL GEOMETRY", result);
    }

    @Test
    public void testEvaluate_EmptyInput() {
        String result = udf.evaluate(new BytesWritable(new byte[0]));
        assertEquals("NULL GEOMETRY", result);
    }
}
