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
import static org.junit.Assert.assertNull;

public class SDO_MaxMbrOrdinateTest {

    private SDO_MaxMbrOrdinate udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_MaxMbrOrdinate();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_Success() {
        // Create a square from (0,0) to (10,20)
        Polygon poly = new Polygon();
        poly.startPath(0, 0);
        poly.lineTo(10, 0);
        poly.lineTo(10, 20);
        poly.lineTo(0, 20);
        poly.closePathWithLine();

        OGCPolygon ogcPoly = new OGCPolygon(poly, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        System.out.println(ogcPoly.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom.getBytes()));

        // Max X should be 10.0
        assertEquals(10.0, udf.evaluate(geom, 1), 0.0001);
        // Max Y should be 20.0
        assertEquals(20.0, udf.evaluate(geom, 2), 0.0001);
    }

    @Test
    public void testEvaluate_NullInput() {
        assertNull(udf.evaluate(null, 1));
        assertNull(udf.evaluate(new BytesWritable(), 1));
        assertNull(udf.evaluate(new BytesWritable(new byte[0]), 1));
    }

    @Test
    public void testEvaluate_NullOrdinatePos() {
        Polygon poly = new Polygon();
        poly.startPath(0, 0);
        poly.lineTo(10, 10);
        OGCPolygon ogcPoly = new OGCPolygon(poly, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        assertNull(udf.evaluate(geom, null));
    }

    @Test
    public void testEvaluate_InvalidOrdinatePos() {
        Polygon poly = new Polygon();
        poly.startPath(0, 0);
        poly.lineTo(10, 10);
        OGCPolygon ogcPoly = new OGCPolygon(poly, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoly);

        assertNull(udf.evaluate(geom, 5));
    }

    @Test
    public void testEvaluate_Z_and_M() {
        com.esri.core.geometry.Point p = new com.esri.core.geometry.Point();
        p.setX(10.0);
        p.setY(20.0);
        p.setZ(30.0);
        p.setM(40.0);

        OGCGeometry ogcPoint = OGCGeometry.createFromEsriGeometry(p, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoint);

        assertEquals(30.0, udf.evaluate(geom, 3), 0.0001);
        assertEquals(40.0, udf.evaluate(geom, 4), 0.0001);
    }
}
