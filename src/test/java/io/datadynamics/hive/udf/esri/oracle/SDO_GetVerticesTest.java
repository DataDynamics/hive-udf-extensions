package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.utils.PrettyHexDump;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SDO_GetVerticesTest {

    private SDO_GetVertices udf;
    private SpatialReference sr;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_GetVertices();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_Polyline() {
        Polyline line = new Polyline();
        line.startPath(0, 0);
        line.lineTo(10, 10);
        line.lineTo(20, 0);

        OGCLineString ogcLine = new OGCLineString(line, 0, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine);

        System.out.println(ogcLine.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom.getBytes()));

        String json = udf.evaluate(geom);
        assertNotNull(json);
        System.out.println("JSON: " + json);

        System.out.println(PrettyHexDump.prettyHexDump(json.getBytes()));

        // JSON 형식 검증 (단순 문자열 포함 여부 확인)
        assertTrue(json.contains("\"x\":0.0"));
        assertTrue(json.contains("\"y\":0.0"));
        assertTrue(json.contains("\"id\":1"));
        assertTrue(json.contains("\"x\":10.0"));
        assertTrue(json.contains("\"y\":10.0"));
        assertTrue(json.contains("\"id\":2"));
    }

    @Test
    public void testEvaluate_Polygon() {
        Polygon poly = new Polygon();
        poly.startPath(0, 0);
        poly.lineTo(10, 0);
        poly.lineTo(10, 10);
        poly.lineTo(0, 10);
        poly.closePathWithLine();

        OGCPolygon ogcPoly = new OGCPolygon(poly, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        String json = udf.evaluate(geom);
        assertNotNull(json);
        System.out.println("JSON: " + json);

        assertTrue(json.contains("\"id\":1"));
        assertTrue(json.contains("\"id\":4"));
    }

    @Test
    public void testEvaluate_MultiPoint() {
        MultiPoint mp = new MultiPoint();
        mp.add(5, 5);
        mp.add(15, 15);

        OGCMultiPoint ogcMP = new OGCMultiPoint(mp, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcMP);

        String json = udf.evaluate(geom);
        assertNotNull(json);
        System.out.println("JSON: " + json);

        assertTrue(json.contains("\"x\":5.0"));
        assertTrue(json.contains("\"x\":15.0"));
    }
}
