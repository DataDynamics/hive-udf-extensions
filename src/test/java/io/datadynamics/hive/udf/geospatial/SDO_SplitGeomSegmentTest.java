package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.*;

import static org.junit.Assert.*;

public class SDO_SplitGeomSegmentTest {

    private SDO_SplitGeomSegment udf;
    private GeometryFactory factory;
    private ObjectInspector[] arguments;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_SplitGeomSegment();
        factory = new GeometryFactory();
        arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_Success() throws HiveException {
        // (0, 0) to (100, 0), length = 100
        LineString line = factory.createLineString(new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(100, 0)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(line);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(new DoubleWritable(40.0)) // 40 지점에서 분할
        };

        BytesWritable resultBytes = (BytesWritable) udf.evaluate(deferredObjects);
        assertNotNull(resultBytes);

        Geometry result = GeometryUtils.bytesToGeometry(resultBytes);
        assertTrue(result instanceof GeometryCollection);
        assertEquals(2, result.getNumGeometries());

        Geometry seg1 = result.getGeometryN(0);
        Geometry seg2 = result.getGeometryN(1);

        assertEquals(40.0, seg1.getLength(), 0.0001);
        assertEquals(60.0, seg2.getLength(), 0.0001);

        // 첫 번째 세그먼트의 끝점이 (40, 0)이어야 함
        assertEquals(40.0, seg1.getCoordinates()[seg1.getCoordinates().length - 1].x, 0.0001);
        // 두 번째 세그먼트의 시작점이 (40, 0)이어야 함
        assertEquals(40.0, seg2.getCoordinates()[0].x, 0.0001);
    }

    @Test
    public void testEvaluate_OutOfRange() throws HiveException {
        LineString line = factory.createLineString(new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(100, 0)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(line);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(new DoubleWritable(150.0)) // 범위를 벗어남
        };

        BytesWritable resultBytes = (BytesWritable) udf.evaluate(deferredObjects);
        assertNull(resultBytes);
    }

    @Test
    public void testEvaluate_NullInputs() throws HiveException {
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(new DoubleWritable(50.0))
        };
        assertNull(udf.evaluate(deferredObjects));

        LineString line = factory.createLineString(new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(100, 0)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(line);
        deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes),
                new GenericUDF.DeferredJavaObject(null)
        };
        assertNull(udf.evaluate(deferredObjects));
    }
}
