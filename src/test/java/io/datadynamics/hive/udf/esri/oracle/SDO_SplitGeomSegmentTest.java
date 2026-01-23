package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SDO_SplitGeomSegmentTest {

    private SDO_SplitGeomSegment udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_SplitGeomSegment();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_Success() {
        // Create a line: (0,0) -> (100,0)
        Polyline polyline = new Polyline();
        polyline.startPath(0, 0);
        polyline.lineTo(100, 0);

        OGCLineString ogcLine = new OGCLineString(polyline, 0, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine);

        // Split at 50.0
        BytesWritable result = udf.evaluate(geom, 50.0);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        
        // Result should be a MultiLineString (due to ESRI library limitations with GeometryCollection)
        assertEquals("MultiLineString", ogcResult.geometryType());
        
        String text = ogcResult.asText();
        System.out.println("[DEBUG_LOG] Result WKT: " + text);
        // Check for coordinates or structure
        assertTrue(text.toUpperCase().contains("0.0 0.0") || text.toUpperCase().contains("0 0"));
        assertTrue(text.toUpperCase().contains("50.0 0.0") || text.toUpperCase().contains("50 0"));
        assertTrue(text.toUpperCase().contains("100.0 0.0") || text.toUpperCase().contains("100 0"));
    }

    @Test
    public void testEvaluate_OutOfBounds() {
        Polyline polyline = new Polyline();
        polyline.startPath(0, 0);
        polyline.lineTo(100, 0);

        OGCLineString ogcLine = new OGCLineString(polyline, 0, sr);
        BytesWritable geom = GeometryUtils.geometryToEsriShapeBytesWritable(ogcLine);

        // Split at 150.0 (out of bounds)
        BytesWritable result = udf.evaluate(geom, 150.0);
        assertNull(result);

        // Split at -10.0 (out of bounds)
        result = udf.evaluate(geom, -10.0);
        assertNull(result);
    }

    @Test
    public void testEvaluate_NullInput() {
        assertNull(udf.evaluate(null, 50.0));
        assertNull(udf.evaluate(new BytesWritable(new byte[0]), 50.0));
        assertNull(udf.evaluate(new BytesWritable(new byte[]{1, 2, 3}), null));
    }
}
