package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import static org.junit.Assert.*;

public class SDO_AppendTest {

    private SDO_Append udf;
    private GeometryFactory factory;

    @Before
    public void setUp() {
        udf = new SDO_Append();
        factory = new GeometryFactory();
    }

    @Test
    public void testEvaluate_Success() {
        Point p1 = factory.createPoint(new Coordinate(1, 1));
        Point p2 = factory.createPoint(new Coordinate(2, 2));

        BytesWritable g1Bytes = GeometryUtils.geometryToBytes(p1);
        BytesWritable g2Bytes = GeometryUtils.geometryToBytes(p2);

        BytesWritable resultBytes = udf.evaluate(g1Bytes, g2Bytes);
        assertNotNull(resultBytes);

        Geometry result = GeometryUtils.bytesToGeometry(resultBytes);
        assertTrue(result instanceof GeometryCollection);
        assertEquals(2, result.getNumGeometries());
        
        Geometry g1 = result.getGeometryN(0);
        Geometry g2 = result.getGeometryN(1);
        
        assertEquals(1.0, g1.getCoordinate().x, 0.0001);
        assertEquals(1.0, g1.getCoordinate().y, 0.0001);
        assertEquals(2.0, g2.getCoordinate().x, 0.0001);
        assertEquals(2.0, g2.getCoordinate().y, 0.0001);
    }

    @Test
    public void testEvaluate_FirstNull() {
        Point p2 = factory.createPoint(new Coordinate(2, 2));
        BytesWritable g2Bytes = GeometryUtils.geometryToBytes(p2);

        BytesWritable resultBytes = udf.evaluate(null, g2Bytes);
        
        // SDO_Append.java:20: if (g1 == null) return g2Bytes;
        // g1은 bytesToGeometry(null) 결과가 null이면 return g2Bytes 함.
        assertSame(g2Bytes, resultBytes);
    }

    @Test
    public void testEvaluate_SecondNull() {
        Point p1 = factory.createPoint(new Coordinate(1, 1));
        BytesWritable g1Bytes = GeometryUtils.geometryToBytes(p1);

        BytesWritable resultBytes = udf.evaluate(g1Bytes, null);
        
        // SDO_Append.java:21: if (g2 == null) return g1Bytes;
        assertSame(g1Bytes, resultBytes);
    }

    @Test
    public void testEvaluate_BothNull() {
        // g1이 null이면 바로 g2Bytes를 리턴함. g2Bytes가 null이면 null 리턴.
        BytesWritable resultBytes = udf.evaluate(null, null);
        assertNull(resultBytes);
    }
}
