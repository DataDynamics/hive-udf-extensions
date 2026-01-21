package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import static org.junit.Assert.*;

public class SDO_ConvertToStdGeomTest {

    private SDO_ConvertToStdGeom udf;
    private GeometryFactory factory;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_ConvertToStdGeom();
        factory = new GeometryFactory();
        ObjectInspector[] arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_Success() throws HiveException {
        Point point = factory.createPoint(new Coordinate(10.0, 20.0));
        BytesWritable input = GeometryUtils.geometryToBytes(point);

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(input)
        };
        BytesWritable result = (BytesWritable) udf.evaluate(args);
        
        // 현재 구현은 입력을 그대로 반환하므로 동일한 객체(또는 동일한 내용)임을 확인
        assertNotNull(result);
        assertEquals(input, result);
    }

    @Test
    public void testEvaluate_NullInput() throws HiveException {
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null)
        };
        assertNull(udf.evaluate(args));
    }
}
