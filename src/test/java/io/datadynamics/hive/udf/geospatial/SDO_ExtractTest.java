package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;

import static org.junit.Assert.*;

public class SDO_ExtractTest {

    private SDO_Extract udf;
    private GeometryFactory factory;
    private WKTReader reader;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_Extract();
        factory = new GeometryFactory();
        reader = new WKTReader(factory);
        // SDO_Extract supports 2 or 3 arguments. initialize with 3 to be safe
        ObjectInspector[] arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testExtractElementFromMultiPoint() throws Exception {
        // MULTIPOINT (0 0, 10 10, 20 20)
        String wkt = "MULTIPOINT ((0 0), (10 10), (20 20))";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);

        // 1번째 요소 추출 (Point(0,0))
        GenericUDF.DeferredObject[] args1 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(1)
        };
        BytesWritable result1 = (BytesWritable) udf.evaluate(args1);
        assertNotNull(result1);
        Geometry resGeom1 = GeometryUtils.bytesToGeometry(result1);
        assertTrue(resGeom1 instanceof Point);
        assertEquals(0.0, resGeom1.getCoordinate().x, 0.0001);

        // 2번째 요소 추출 (Point(10,10))
        GenericUDF.DeferredObject[] args2 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(2)
        };
        BytesWritable result2 = (BytesWritable) udf.evaluate(args2);
        Geometry resGeom2 = GeometryUtils.bytesToGeometry(result2);
        assertEquals(10.0, resGeom2.getCoordinate().x, 0.0001);

        // 3번째 요소 추출 (Point(20,20))
        GenericUDF.DeferredObject[] args3 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(3)
        };
        BytesWritable result3 = (BytesWritable) udf.evaluate(args3);
        Geometry resGeom3 = GeometryUtils.bytesToGeometry(result3);
        assertEquals(20.0, resGeom3.getCoordinate().x, 0.0001);

        // 존재하지 않는 인덱스 (4)
        GenericUDF.DeferredObject[] args4 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(4)
        };
        assertNull(udf.evaluate(args4));
        // 잘못된 인덱스 (0)
        GenericUDF.DeferredObject[] args0 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(0)
        };
        assertNull(udf.evaluate(args0));
    }

    @Test
    public void testExtractElementFromMultiPolygon() throws Exception {
        // MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))
        String wkt = "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);

        // 2번째 폴리곤 추출
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(2)
        };
        BytesWritable result = (BytesWritable) udf.evaluate(args);
        assertNotNull(result);
        Geometry resGeom = GeometryUtils.bytesToGeometry(result);
        assertEquals("Polygon", resGeom.getGeometryType());
        assertEquals(20.0, resGeom.getCoordinate().x, 0.0001);
    }

    @Test
    public void testExtractRingsFromPolygon() throws Exception {
        // 도넛 모양 폴리곤 (외곽선 + 구멍 1개)
        String wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);

        // 1번째 요소(Polygon 전체)의 1번째 링 (Exterior Ring)
        GenericUDF.DeferredObject[] args1 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(1),
                new GenericUDF.DeferredJavaObject(1)
        };
        BytesWritable exteriorWkb = (BytesWritable) udf.evaluate(args1);
        assertNotNull(exteriorWkb);
        Geometry exterior = GeometryUtils.bytesToGeometry(exteriorWkb);
        assertTrue(exterior instanceof LineString);
        assertEquals(5, exterior.getNumPoints());
        assertEquals(0.0, exterior.getCoordinates()[0].x, 0.0001);

        // 1번째 요소의 2번째 링 (Interior Ring / Hole)
        GenericUDF.DeferredObject[] args2 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(1),
                new GenericUDF.DeferredJavaObject(2)
        };
        BytesWritable interiorWkb = (BytesWritable) udf.evaluate(args2);
        assertNotNull(interiorWkb);
        Geometry interior = GeometryUtils.bytesToGeometry(interiorWkb);
        assertTrue(interior instanceof LineString);
        assertEquals(5, interior.getNumPoints());
        assertEquals(2.0, interior.getCoordinates()[0].x, 0.0001);

        // 존재하지 않는 링 인덱스 (3)
        GenericUDF.DeferredObject[] args3 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(1),
                new GenericUDF.DeferredJavaObject(3)
        };
        assertNull(udf.evaluate(args3));
    }

    @Test
    public void testExtractRingFromNonPolygon() throws Exception {
        String wkt = "POINT (1 1)";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb),
                new GenericUDF.DeferredJavaObject(1),
                new GenericUDF.DeferredJavaObject(1)
        };
        BytesWritable result = (BytesWritable) udf.evaluate(args);
        assertNotNull(result);
        Geometry resGeom = GeometryUtils.bytesToGeometry(result);
        assertTrue(resGeom instanceof Point);
    }

    @Test
    public void testNullInputs() throws HiveException {
        GenericUDF.DeferredObject[] args1 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null),
                new GenericUDF.DeferredJavaObject(1)
        };
        assertNull(udf.evaluate(args1));

        GenericUDF.DeferredObject[] args2 = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(new BytesWritable(new byte[]{1, 2, 3})),
                new GenericUDF.DeferredJavaObject(null)
        };
        assertNull(udf.evaluate(args2));
    }

    @Test
    public void testInvalidGeometry() throws HiveException {
        BytesWritable invalidWkb = new BytesWritable(new byte[]{0, 0, 0, 0});
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(invalidWkb),
                new GenericUDF.DeferredJavaObject(1)
        };
        assertNull(udf.evaluate(args));
    }
}
