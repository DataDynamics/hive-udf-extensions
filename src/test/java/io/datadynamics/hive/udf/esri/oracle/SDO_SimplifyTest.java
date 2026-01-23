package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPolygon;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.utils.PrettyHexDump;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SDO_SimplifyTest {

    private SDO_Simplify udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_Simplify();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_SelfIntersectingPolygon() {
        // Create a figure-8 polygon (self-intersecting)
        // 0,0 --- 10,10
        //  |       |
        // 0,10 --- 10,0
        Polygon poly = new Polygon();
        poly.startPath(0, 0);
        poly.lineTo(10, 10);
        poly.lineTo(0, 10);
        poly.lineTo(10, 0);
        poly.closePathWithLine();

        OGCPolygon ogcPoly = new OGCPolygon(poly, sr);

        // ESRI's OGCPolygon.asText() might fail on invalid geometries
        // BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);
        // Use GeometryUtils to convert directly to avoid OGC-level checks if possible
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        System.out.println(PrettyHexDump.prettyHexDump(geom.getBytes()));

        BytesWritable result = udf.evaluate(geom);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);

        System.out.println(ogcResult.asText());
        System.out.println(PrettyHexDump.prettyHexDump(result.getBytes()));

        // After simplification, it should be valid (isSimple == true)
        assertTrue(ogcResult.isSimple());
        
        System.out.println("Rectified: " + ogcResult.asText());
        
        assertNotEquals("POLYGON ((0 0, 10 10, 0 10, 10 0, 0 0))", ogcResult.asText());
    }

    @Test
    public void testEvaluate_NullAndEmpty() {
        assertNull(udf.evaluate(null));
        assertNull(udf.evaluate(new BytesWritable(new byte[0])));
    }

    @Test
    public void testEvaluate_WithTolerance() {
        Polygon poly = new Polygon();
        poly.startPath(0, 0);
        poly.lineTo(10, 0);
        poly.lineTo(10, 10);
        poly.lineTo(0, 10);
        poly.closePathWithLine();

        OGCPolygon ogcPoly = new OGCPolygon(poly, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        System.out.println(ogcPoly.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom.getBytes()));

        // Should work with tolerance argument
        BytesWritable result = udf.evaluate(geom, 0.001);
        assertNotNull(result);
        
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertEquals("Polygon", ogcResult.geometryType());

        System.out.println(ogcResult.asText());
        System.out.println(PrettyHexDump.prettyHexDump(result.getBytes()));
    }
}
