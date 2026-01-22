package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPoint;
import io.datadynamics.hive.udf.esri.GeometryUtils;
import io.datadynamics.hive.udf.utils.PrettyHexDump;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SDO_AppendTest {

    private SDO_Append udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_Append();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_CombinePolylines() {
        // Line 1: (0,0) to (1,1)
        Polyline line1 = new Polyline();
        line1.startPath(0, 0);
        line1.lineTo(1, 1);
        OGCLineString ogcLine1 = new OGCLineString(line1, 0, sr);
        BytesWritable geom1 = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine1);

        System.out.println(ogcLine1.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom1.getBytes()));

        // Line 2: (2,2) to (3,3)
        Polyline line2 = new Polyline();
        line2.startPath(2, 2);
        line2.lineTo(3, 3);
        OGCLineString ogcLine2 = new OGCLineString(line2, 0, sr);
        BytesWritable geom2 = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine2);

        System.out.println(ogcLine2.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom2.getBytes()));

        // Result
        BytesWritable result = udf.evaluate(geom1, geom2);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertEquals("MultiLineString", ogcResult.geometryType());

        // Print Result
        System.out.println(ogcResult.asText());
        byte[] array = ogcResult.asBinary().array();
        System.out.println(PrettyHexDump.prettyHexDump(array));

        // MultiLineString should have 2 paths (parts)
        Polyline resultPolyline = (Polyline) ogcResult.getEsriGeometry();
        assertEquals(2, resultPolyline.getPathCount());
        assertEquals(4326, ogcResult.SRID());
    }

    @Test
    public void testEvaluate_SRIDMismatch() {
        Polyline line1 = new Polyline();
        line1.startPath(0, 0);
        line1.lineTo(1, 1);
        OGCLineString ogcLine1 = new OGCLineString(line1, 0, sr);
        BytesWritable geom1 = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine1);

        Polyline line2 = new Polyline();
        line2.startPath(2, 2);
        line2.lineTo(3, 3);
        OGCLineString ogcLine2 = new OGCLineString(line2, 0, SpatialReference.create(3857));
        BytesWritable geom2 = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine2);

        BytesWritable result = udf.evaluate(geom1, geom2);

        assertNull(result);
    }

    @Test
    public void testEvaluate_NullArguments() {
        Polyline line1 = new Polyline();
        line1.startPath(0, 0);
        line1.lineTo(1, 1);
        OGCLineString ogcLine1 = new OGCLineString(line1, 0, sr);
        BytesWritable geom1 = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine1);

        assertNull(udf.evaluate(geom1, null));
        assertNull(udf.evaluate(null, geom1));
        assertNull(udf.evaluate(null, null));
    }

    @Test
    public void testEvaluate_EmptyArguments() {
        Polyline line1 = new Polyline();
        line1.startPath(0, 0);
        line1.lineTo(1, 1);
        OGCLineString ogcLine1 = new OGCLineString(line1, 0, sr);
        BytesWritable geom1 = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine1);
        BytesWritable emptyGeom = new BytesWritable(new byte[0]);

        assertNull(udf.evaluate(geom1, emptyGeom));
        assertNull(udf.evaluate(emptyGeom, geom1));
    }

    @Test
    public void testEvaluate_NonPolylineArguments() {
        // Point
        Point point1 = new Point(0, 0);
        OGCPoint ogcPoint1 = new OGCPoint(point1, sr);
        BytesWritable geom1 = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoint1);

        System.out.println(ogcPoint1.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom1.getBytes()));

        // Line
        Polyline line2 = new Polyline();
        line2.startPath(2, 2);
        line2.lineTo(3, 3);
        OGCLineString ogcLine2 = new OGCLineString(line2, 0, sr);
        BytesWritable geom2 = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine2);

        System.out.println(ogcLine2.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom2.getBytes()));

        // Result
        BytesWritable result = udf.evaluate(geom1, geom2);
        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertEquals("MultiLineString", ogcResult.geometryType());

        // Print Result
        System.out.println(ogcResult.asText());
        byte[] array = ogcResult.asBinary().array();
        System.out.println(PrettyHexDump.prettyHexDump(array));

        // Since Point is not a Polyline, the resulting MultiLineString should only contain the line from geom2
        Polyline resultPolyline = (Polyline) ogcResult.getEsriGeometry();
        assertEquals(1, resultPolyline.getPathCount());
    }
}
