package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPoint;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.utils.PrettyHexDump;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SDO_ClosestPointsTest {

    private SDO_ClosestPoints udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_ClosestPoints();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_PointToPoint() {
        OGCPoint p1 = new OGCPoint(new Point(0, 0), sr);
        OGCPoint p2 = new OGCPoint(new Point(10, 10), sr);

        BytesWritable geom1 = GeometryUtils.geometryToEsriShapeBytesWritable(p1);
        BytesWritable geom2 = GeometryUtils.geometryToEsriShapeBytesWritable(p2);

        System.out.println(p1.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom1.getBytes()));

        System.out.println(p2.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom2.getBytes()));

        BytesWritable result = udf.evaluate(geom1, geom2);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertEquals("LineString", ogcResult.geometryType());

        System.out.println(ogcResult.asText());
        System.out.println(PrettyHexDump.prettyHexDump(result.getBytes()));

        // (0,0) to (10,10)
        assertEquals("LINESTRING (0 0, 10 10)", ogcResult.asText());
    }

    @Test
    public void testEvaluate_PointToLine() {
        OGCPoint p = new OGCPoint(new Point(5, 5), sr);
        
        Polyline line = new Polyline();
        line.startPath(0, 0);
        line.lineTo(10, 0);
        OGCLineString ls = new OGCLineString(line, 0, sr);

        BytesWritable geom1 = GeometryUtils.geometryToEsriShapeBytesWritable(p);
        BytesWritable geom2 = GeometryUtils.geometryToEsriShapeBytesWritable(ls);

        System.out.println(p.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom1.getBytes()));

        System.out.println(ls.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom1.getBytes()));


        BytesWritable result = udf.evaluate(geom1, geom2);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertEquals("LineString", ogcResult.geometryType());

        System.out.println(ogcResult.asText());
        System.out.println(PrettyHexDump.prettyHexDump(result.getBytes()));

        // (5,5) to (5,0)
        assertEquals("LINESTRING (5 5, 5 0)", ogcResult.asText());
    }

    @Test
    public void testEvaluate_NullArguments() {
        OGCPoint p = new OGCPoint(new Point(0, 0), sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(p);

        assertNull(udf.evaluate(geom, null));
        assertNull(udf.evaluate(null, geom));
        assertNull(udf.evaluate(null, null));
    }

    @Test
    public void testEvaluate_EmptyArguments() {
        OGCPoint p = new OGCPoint(new Point(0, 0), sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(p);
        BytesWritable emptyGeom = new BytesWritable(new byte[0]);

        assertNull(udf.evaluate(geom, emptyGeom));
        assertNull(udf.evaluate(emptyGeom, geom));
    }
}
