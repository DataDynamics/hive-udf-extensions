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
import org.locationtech.jts.geom.MultiPoint;

import static org.junit.Assert.*;

public class SDO_ConcaveHullTest {

    private SDO_ConcaveHull udf;
    private GeometryFactory factory;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_ConcaveHull();
        factory = new GeometryFactory();
        ObjectInspector[] arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_ValidInput() throws HiveException {
        // C 모양의 점 집합 생성 (Concave Hull이 Convex Hull과 다르게 생성되도록 함)
        Coordinate[] coords = new Coordinate[] {
            new Coordinate(0, 0),
            new Coordinate(10, 0),
            new Coordinate(10, 10),
            new Coordinate(0, 10),
            new Coordinate(2, 5) // 안쪽으로 들어간 점
        };
        MultiPoint multiPoint = factory.createMultiPointFromCoords(coords);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(multiPoint);

        // 큰 tolerance 값 (Convex Hull과 비슷해짐)
        GenericUDF.DeferredObject[] args1 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(100.0)
        };
        BytesWritable resultWkb1 = (BytesWritable) udf.evaluate(args1);
        assertNotNull(resultWkb1);
        Geometry resultGeom1 = GeometryUtils.bytesToGeometry(resultWkb1);
        assertNotNull(resultGeom1);

        // 작은 tolerance 값 (더 오목해짐)
        GenericUDF.DeferredObject[] args2 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(3.0)
        };
        BytesWritable resultWkb2 = (BytesWritable) udf.evaluate(args2);
        assertNotNull(resultWkb2);
        Geometry resultGeom2 = GeometryUtils.bytesToGeometry(resultWkb2);
        assertNotNull(resultGeom2);

        // 두 결과의 면적이 달라야 함 (오목한 정도가 다르므로)
        assertNotEquals(resultGeom1.getArea(), resultGeom2.getArea(), 0.001);
        assertTrue(resultGeom1.getArea() >= resultGeom2.getArea());
    }

    @Test
    public void testEvaluate_NullTolerance() throws HiveException {
        Coordinate[] coords = new Coordinate[] {
            new Coordinate(0, 0),
            new Coordinate(10, 0),
            new Coordinate(10, 10),
            new Coordinate(0, 10),
            new Coordinate(5, 5)
        };
        MultiPoint multiPoint = factory.createMultiPointFromCoords(coords);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(multiPoint);

        // tolerance가 null일 때 에러 없이 동작해야 함
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(null)
        };
        BytesWritable resultWkb = (BytesWritable) udf.evaluate(args);
        assertNotNull(resultWkb);
        Geometry resultGeom = GeometryUtils.bytesToGeometry(resultWkb);
        assertNotNull(resultGeom);
    }

    @Test
    public void testEvaluate_NullGeometry() throws HiveException {
        // 입력 기하학 객체가 null일 때 null 반환
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(1.0)
        };
        BytesWritable result = (BytesWritable) udf.evaluate(args);
        assertNull(result);
    }

    @Test
    public void testEvaluate_InvalidGeometry() throws HiveException {
        // 유효하지 않은 WKB 입력 시 null 반환 확인
        BytesWritable invalidWkb = new BytesWritable(new byte[]{0, 1, 2, 3});
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(invalidWkb),
                new GenericUDF.DeferredJavaObject(1.0)
        };
        BytesWritable result = (BytesWritable) udf.evaluate(args);
        assertNull(result);
    }
}
