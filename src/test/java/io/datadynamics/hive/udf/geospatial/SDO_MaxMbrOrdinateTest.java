package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SDO_MaxMbrOrdinateTest {

    private SDO_MaxMbrOrdinate udf;
    private GeometryFactory factory;
    private ObjectInspector[] arguments;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_MaxMbrOrdinate();
        factory = new GeometryFactory();
        arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
                PrimitiveObjectInspectorFactory.writableIntObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_MaxX() throws HiveException {
        // (10, 10), (20, 10), (20, 30), (10, 30), (10, 10) -> MBR MaxX = 20.0, MaxY = 30.0
        Polygon poly = factory.createPolygon(new Coordinate[]{
                new Coordinate(10, 10),
                new Coordinate(20, 10),
                new Coordinate(20, 30),
                new Coordinate(10, 30),
                new Coordinate(10, 10)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(poly);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(new IntWritable(1)) // X축
        };

        Double result = (Double) udf.evaluate(deferredObjects);
        assertEquals(20.0, result, 0.0001);
    }

    @Test
    public void testEvaluate_MaxY() throws HiveException {
        Polygon poly = factory.createPolygon(new Coordinate[]{
                new Coordinate(10, 10),
                new Coordinate(20, 10),
                new Coordinate(20, 30),
                new Coordinate(10, 30),
                new Coordinate(10, 10)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(poly);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(new IntWritable(2)) // Y축
        };

        Double result = (Double) udf.evaluate(deferredObjects);
        assertEquals(30.0, result, 0.0001);
    }

    @Test
    public void testEvaluate_NullGeom() throws HiveException {
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(new IntWritable(1))
        };

        Double result = (Double) udf.evaluate(deferredObjects);
        assertNull(result);
    }

    @Test
    public void testEvaluate_NullOrdinatePos() throws HiveException {
        Polygon poly = factory.createPolygon(new Coordinate[]{
                new Coordinate(10, 10),
                new Coordinate(20, 20),
                new Coordinate(10, 10)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(poly);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(null)
        };

        Double result = (Double) udf.evaluate(deferredObjects);
        assertNull(result);
    }

    @Test
    public void testEvaluate_InvalidOrdinatePos() throws HiveException {
        Polygon poly = factory.createPolygon(new Coordinate[]{
                new Coordinate(10, 10),
                new Coordinate(20, 20),
                new Coordinate(10, 10)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(poly);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(new IntWritable(3)) // 미지원 (Z축 등)
        };

        Double result = (Double) udf.evaluate(deferredObjects);
        assertNull(result);
    }
}
