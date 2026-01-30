package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPoint;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SDO_ConvertToLrsGeomTest {

    private SDO_ConvertToLrsGeom udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_ConvertToLrsGeom();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_SimpleLine() {
        Polyline line = new Polyline();
        line.startPath(0, 0);
        line.lineTo(10, 0);
        OGCLineString ls = new OGCLineString(line, 0, sr);

        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ls);
        BytesWritable result = udf.evaluate(geom);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertTrue(ogcResult.isMeasured());

        // Check M values
        // ESRI OGC API doesn't expose M directly easily from OGCGeometry, 
        // we might need to go back to Esri Geometry or use ST_M if available.
        Polyline resultPolyline = (Polyline) ogcResult.getEsriGeometry();
        assertEquals(0.0, resultPolyline.getPoint(0).getM(), 0.0001);
        assertEquals(10.0, resultPolyline.getPoint(1).getM(), 0.0001);
    }

    @Test
    public void testEvaluate_WithCustomRange() {
        Polyline line = new Polyline();
        line.startPath(0, 0);
        line.lineTo(10, 0);
        OGCLineString ls = new OGCLineString(line, 0, sr);

        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ls);
        // startM = 100, endM = 200
        BytesWritable result = udf.evaluate(geom, 100.0, 200.0);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertTrue(ogcResult.isMeasured());

        Polyline resultPolyline = (Polyline) ogcResult.getEsriGeometry();
        assertEquals(100.0, resultPolyline.getPoint(0).getM(), 0.0001);
        assertEquals(200.0, resultPolyline.getPoint(1).getM(), 0.0001);
    }

    @Test
    public void testEvaluate_MultiLine() {
        Polyline line = new Polyline();
        // First path: (0,0) to (10,0) - Length 10
        line.startPath(0, 0);
        line.lineTo(10, 0);
        // Second path: (10,10) to (20,10) - Length 10
        line.startPath(10, 10);
        line.lineTo(20, 10);
        // Total Length = 20

        com.esri.core.geometry.ogc.OGCMultiLineString mls = new com.esri.core.geometry.ogc.OGCMultiLineString(line, sr);

        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(mls);
        BytesWritable result = udf.evaluate(geom);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertTrue(ogcResult.isMeasured());

        Polyline resultPolyline = (Polyline) ogcResult.getEsriGeometry();
        // Path 1
        assertEquals(0.0, resultPolyline.getPoint(0).getM(), 0.0001);
        assertEquals(10.0, resultPolyline.getPoint(1).getM(), 0.0001);
        // Path 2
        assertEquals(10.0, resultPolyline.getPoint(2).getM(), 0.0001);
        assertEquals(20.0, resultPolyline.getPoint(3).getM(), 0.0001);
    }

    @Test
    public void testEvaluate_InvalidInput() {
        // Point is not supported currently by my implementation (warns and returns null)
        OGCPoint p = new OGCPoint(new com.esri.core.geometry.Point(1, 1), sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(p);
        assertNull(udf.evaluate(geom));

        assertNull(udf.evaluate(null));
        assertNull(udf.evaluate(new BytesWritable(new byte[0])));
    }
}
