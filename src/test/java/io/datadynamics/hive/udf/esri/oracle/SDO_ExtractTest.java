package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.*;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.utils.PrettyHexDump;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SDO_ExtractTest {

    private SDO_Extract udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_Extract();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_MultiPoint() {
        MultiPoint mp = new MultiPoint();
        mp.add(0, 0);
        mp.add(10, 10);
        
        OGCMultiPoint ogcMP = new OGCMultiPoint(mp, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcMP);

        System.out.println(ogcMP.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom.getBytes()));

        // 첫 번째 점 추출
        BytesWritable result1 = udf.evaluate(geom, 1);
        assertNotNull(result1);

        OGCGeometry ogcResult1 = GeometryUtils.geometryFromEsriShape(result1);
        assertEquals("Point", ogcResult1.geometryType());

        Point p1 = (Point) ogcResult1.getEsriGeometry();
        assertEquals(0.0, p1.getX(), 0.001);

        System.out.println(ogcResult1.asText());
        System.out.println(PrettyHexDump.prettyHexDump(result1.getBytes()));

        // 두 번째 점 추출
        BytesWritable result2 = udf.evaluate(geom, 2);
        assertNotNull(result2);

        OGCGeometry ogcResult2 = GeometryUtils.geometryFromEsriShape(result2);
        assertEquals("Point", ogcResult2.geometryType());

        Point p2 = (Point) ogcResult2.getEsriGeometry();
        assertEquals(10.0, p2.getX(), 0.001);

        System.out.println(ogcResult2.asText());
        System.out.println(PrettyHexDump.prettyHexDump(result2.getBytes()));
    }

    @Test
    public void testEvaluate_PolygonRings() {
        Polygon poly = new Polygon();
        // Outer ring
        poly.startPath(0, 0);
        poly.lineTo(10, 0);
        poly.lineTo(10, 10);
        poly.lineTo(0, 10);
        poly.closePathWithLine();
        
        // Inner ring (hole)
        poly.startPath(2, 2);
        poly.lineTo(2, 4);
        poly.lineTo(4, 4);
        poly.lineTo(4, 2);
        poly.closePathWithLine();

        OGCPolygon ogcPoly = new OGCPolygon(poly, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        // Outer Ring 추출 (ESRI Path 0)
        BytesWritable result1 = udf.evaluate(geom, 1);
        assertNotNull(result1);
        OGCGeometry ogcResult1 = GeometryUtils.geometryFromEsriShape(result1);
        assertEquals("Polygon", ogcResult1.geometryType());
        Polygon resPoly1 = (Polygon) ogcResult1.getEsriGeometry();
        assertEquals(1, resPoly1.getPathCount());
        assertEquals(100.0, Math.abs(resPoly1.calculateArea2D()), 0.001);

        // Inner Ring 추출 (ESRI Path 1) - ringIndex 사용
        BytesWritable result2 = udf.evaluate(geom, 1, 2);
        assertNotNull(result2);
        OGCGeometry ogcResult2 = GeometryUtils.geometryFromEsriShape(result2);
        assertEquals("Polygon", ogcResult2.geometryType());
        Polygon resPoly2 = (Polygon) ogcResult2.getEsriGeometry();
        assertEquals(1, resPoly2.getPathCount());
        assertEquals(4.0, Math.abs(resPoly2.calculateArea2D()), 0.001);
    }

    @Test
    public void testEvaluate_NullAndInvalid() {
        assertNull(udf.evaluate(null, 1));
        
        Point p = new Point(0, 0);
        OGCGeometry ogcP = OGCGeometry.createFromEsriGeometry(p, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcP);
        
        assertNull(udf.evaluate(geom, 2)); // Index out of bounds
    }
}
