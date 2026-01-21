package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.*;

import static org.junit.Assert.*;

public class SDO_ClosestPointsTest {

    private SDO_ClosestPoints udf;
    private GeometryFactory factory;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_ClosestPoints();
        factory = new GeometryFactory();
        ObjectInspector[] arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_ValidPoints() throws HiveException {
        Point p1 = factory.createPoint(new Coordinate(0, 0));
        Point p2 = factory.createPoint(new Coordinate(10, 0));
        BytesWritable g1 = GeometryUtils.geometryToBytes(p1);
        BytesWritable g2 = GeometryUtils.geometryToBytes(p2);

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(g1),
                new GenericUDF.DeferredJavaObject(g2)
        };
        BytesWritable resultBytes = (BytesWritable) udf.evaluate(args);
        assertNotNull(resultBytes);

        Geometry result = GeometryUtils.bytesToGeometry(resultBytes);
        assertTrue(result instanceof LineString);
        assertEquals(2, result.getNumPoints());
        assertEquals(0.0, result.getCoordinates()[0].x, 0.0001);
        assertEquals(10.0, result.getCoordinates()[1].x, 0.0001);
    }

    @Test
    public void testEvaluate_LineAndPoint() throws HiveException {
        // (0,0) to (10,0) 사이의 선분과 (5,5) 점
        LineString line = factory.createLineString(new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(10, 0)
        });
        Point p = factory.createPoint(new Coordinate(5, 5));
        BytesWritable g1 = GeometryUtils.geometryToBytes(line);
        BytesWritable g2 = GeometryUtils.geometryToBytes(p);

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(g1),
                new GenericUDF.DeferredJavaObject(g2)
        };
        BytesWritable resultBytes = (BytesWritable) udf.evaluate(args);
        assertNotNull(resultBytes);

        Geometry result = GeometryUtils.bytesToGeometry(resultBytes);
        assertTrue(result instanceof LineString);
        // 최단 거리를 형성하는 두 점: (5,0) on line, (5,5) is the point
        assertEquals(5.0, result.getCoordinates()[0].x, 0.0001);
        assertEquals(0.0, result.getCoordinates()[0].y, 0.0001);
        assertEquals(5.0, result.getCoordinates()[1].x, 0.0001);
        assertEquals(5.0, result.getCoordinates()[1].y, 0.0001);
    }

    @Test
    public void testEvaluate_NullInputs() throws HiveException {
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(new BytesWritable(new byte[]{1, 2, 3}))
        };
        assertNull(udf.evaluate(args));
    }

    @Test
    public void testEvaluate_InvalidGeometry() throws HiveException {
        BytesWritable invalidWkb = new BytesWritable(new byte[]{0, 1, 2, 3});
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(invalidWkb),
                new GenericUDF.DeferredJavaObject(invalidWkb)
        };
        assertNull(udf.evaluate(args));
    }
}
