package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.utils.PrettyHexDump;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SDO_ConvertToStdGeomTest {

    private SDO_ConvertToStdGeom udf;
    private SpatialReference sr;

    @Before
    public void setUp() {
        udf = new SDO_ConvertToStdGeom();
        sr = SpatialReference.create(4326);
    }

    @Test
    public void testEvaluate_Success() {
        Point point = new Point(10.0, 20.0);
        OGCPoint ogcPoint = new OGCPoint(point, sr);
        BytesWritable input = GeometryUtils.geometryToEsriShapeBytesWritable(ogcPoint);

        System.out.println(ogcPoint.asText());
        System.out.println(PrettyHexDump.prettyHexDump(input.getBytes()));

        BytesWritable result = udf.evaluate(input);

        assertNotNull(result);
        OGCGeometry ogcResult = GeometryUtils.geometryFromEsriShape(result);
        assertEquals("Point", ogcResult.geometryType());

        System.out.println(ogcResult.asText());

        Point resultPoint = (Point) ogcResult.getEsriGeometry();

        System.out.println(resultPoint);

        assertEquals(10.0, resultPoint.getX(), 0.001);
        assertEquals(20.0, resultPoint.getY(), 0.001);
    }

    @Test
    public void testEvaluate_NullInput() {
        assertNull(udf.evaluate(null));
    }

    @Test
    public void testEvaluate_EmptyInput() {
        assertNull(udf.evaluate(new BytesWritable(new byte[0])));
    }
}
