package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import static org.junit.Assert.*;

public class SDO_SimplifyTest {

    private SDO_Simplify udf;
    private GeometryFactory factory;
    private ObjectInspector[] arguments;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_Simplify();
        factory = new GeometryFactory();
        arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_Success() throws HiveException {
        // (0,0), (5, 0.1), (10,0) -> (5, 0.1) 점은 0.2 threshold에서 제거되어야 함
        LineString line = factory.createLineString(new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(5, 0.1),
                new Coordinate(10, 0)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(line);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(new DoubleWritable(0.2))
        };

        BytesWritable resultBytes = (BytesWritable) udf.evaluate(deferredObjects);
        assertNotNull(resultBytes);

        Geometry result = GeometryUtils.bytesToGeometry(resultBytes);
        assertEquals(2, result.getNumPoints()); // 중간 점이 제거되어 2개의 점만 남아야 함
        assertEquals(0.0, result.getCoordinates()[0].x, 0.0001);
        assertEquals(10.0, result.getCoordinates()[1].x, 0.0001);
    }

    @Test
    public void testEvaluate_NullInputs() throws HiveException {
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(new DoubleWritable(0.1))
        };
        assertNull(udf.evaluate(deferredObjects));

        LineString line = factory.createLineString(new Coordinate[]{new Coordinate(0, 0), new Coordinate(1, 1)});
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(line);
        deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(null)
        };
        assertNull(udf.evaluate(deferredObjects));
    }
}
