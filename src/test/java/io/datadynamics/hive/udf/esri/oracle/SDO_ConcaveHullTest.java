package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.utils.PrettyHexDump;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SDO_ConcaveHullTest {

    private SDO_ConcaveHull udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_ConcaveHull();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_MultiPoint() {
        // Create a square of points
        MultiPoint mp = new MultiPoint();
        mp.add(0, 0);
        mp.add(10, 0);
        mp.add(10, 10);
        mp.add(0, 10);
        mp.add(5, 5); // Inside point

        OGCMultiPoint ogcMP = new OGCMultiPoint(mp, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcMP);

        System.out.println(ogcMP.asText());
        System.out.println(PrettyHexDump.prettyHexDump(geom.getBytes()));

        // Current implementation returns Convex Hull
        BytesWritable result = udf.evaluate(geom, null);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);

        System.out.println(ogcResult.asText());
        System.out.println(PrettyHexDump.prettyHexDump(result.getBytes()));

        // Convex hull of a square of points should be a Polygon
        assertEquals("Polygon", ogcResult.geometryType());
        
        // Area should be 100
        com.esri.core.geometry.Polygon resultPoly = (com.esri.core.geometry.Polygon) ogcResult.getEsriGeometry();
        assertEquals(100.0, resultPoly.calculateArea2D(), 0.001);
    }

    @Test
    public void testEvaluate_NullArguments() {
        assertNull(udf.evaluate(null, 10.0));
        assertNull(udf.evaluate(null));
    }

    @Test
    public void testEvaluate_EmptyArguments() {
        BytesWritable emptyGeom = new BytesWritable(new byte[0]);
        assertNull(udf.evaluate(emptyGeom));
    }
}
