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

public class SDO_AppendTest {

    private SDO_Append udf;
    private GeometryFactory factory;
    private ObjectInspector[] arguments;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_Append();
        factory = new GeometryFactory();
        arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_Success() throws HiveException {
        Point p1 = factory.createPoint(new Coordinate(1, 1));
        Point p2 = factory.createPoint(new Coordinate(2, 2));

        BytesWritable g1Bytes = GeometryUtils.geometryToBytes(p1);
        BytesWritable g2Bytes = GeometryUtils.geometryToBytes(p2);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(g1Bytes),
                new GenericUDF.DeferredJavaObject(g2Bytes)
        };

        BytesWritable resultBytes = (BytesWritable) udf.evaluate(deferredObjects);
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
    public void testEvaluate_FirstNull() throws HiveException {
        Point p2 = factory.createPoint(new Coordinate(2, 2));
        BytesWritable g2Bytes = GeometryUtils.geometryToBytes(p2);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(g2Bytes)
        };

        BytesWritable resultBytes = (BytesWritable) udf.evaluate(deferredObjects);

        assertSame(g2Bytes, resultBytes);
    }

    @Test
    public void testEvaluate_SecondNull() throws HiveException {
        Point p1 = factory.createPoint(new Coordinate(1, 1));
        BytesWritable g1Bytes = GeometryUtils.geometryToBytes(p1);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(g1Bytes),
                new GenericUDF.DeferredJavaObject(null)
        };

        BytesWritable resultBytes = (BytesWritable) udf.evaluate(deferredObjects);

        assertSame(g1Bytes, resultBytes);
    }

    @Test
    public void testEvaluate_BothNull() throws HiveException {
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(null)
        };

        BytesWritable resultBytes = (BytesWritable) udf.evaluate(deferredObjects);
        assertNull(resultBytes);
    }
}
