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
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

import static org.junit.Assert.*;

public class SDO_RectifyGeometryTest {

    private SDO_RectifyGeometry udf;
    private GeometryFactory factory;
    private WKTReader reader;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_RectifyGeometry();
        factory = new GeometryFactory();
        reader = new WKTReader(factory);
        ObjectInspector[] arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_ValidGeometry() throws Exception {
        // 이미 유효한 폴리곤
        String wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb)
        };
        BytesWritable resultWkb = (BytesWritable) udf.evaluate(args);

        assertNotNull(resultWkb);
        // 이미 유효하면 원본 그대로 반환하는 성능 최적화 확인
        assertSame(inputWkb, resultWkb);
    }

    @Test
    public void testEvaluate_SelfIntersectingPolygon() throws Exception {
        // 자가 교차하는 유효하지 않은 폴리곤 (나비넥타이 모양)
        String wkt = "POLYGON ((0 0, 10 10, 10 0, 0 10, 0 0))";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(inputWkb)
        };
        BytesWritable resultWkb = (BytesWritable) udf.evaluate(args);

        assertNotNull(resultWkb);
        Geometry resultGeom = GeometryUtils.bytesToGeometry(resultWkb);
        assertTrue(resultGeom.isValid());
        // 나비넥타이 모양은 buffer(0) 이후 두 개의 삼각형(MultiPolygon)이 됨
        assertTrue(resultGeom.getGeometryType().equalsIgnoreCase("MultiPolygon") ||
                (resultGeom instanceof Polygon && resultGeom.isValid()));
    }

    @Test
    public void testEvaluate_NullInput() throws HiveException {
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null)
        };
        assertNull(udf.evaluate(args));
    }

    @Test
    public void testEvaluate_InvalidWkb() throws HiveException {
        BytesWritable invalidWkb = new BytesWritable(new byte[]{0, 1, 2, 3});
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(invalidWkb)
        };
        assertNull(udf.evaluate(args));
    }
}
