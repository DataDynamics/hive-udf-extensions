package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;

import static org.junit.Assert.*;

public class GeometryUtilsTest {

    private final GeometryFactory factory = new GeometryFactory();

    @Test
    public void testBytesToGeometry_ValidWKB() {
        Point point = factory.createPoint(new Coordinate(10.0, 20.0));
        WKBWriter writer = new WKBWriter();
        byte[] wkbBytes = writer.write(point);
        BytesWritable wkb = new BytesWritable(wkbBytes);

        Geometry result = GeometryUtils.bytesToGeometry(wkb);

        assertNotNull(result);
        assertTrue(result instanceof Point);
        assertEquals(10.0, result.getCoordinate().x, 0.0001);
        assertEquals(20.0, result.getCoordinate().y, 0.0001);
    }

    @Test
    public void testBytesToGeometry_NullOrEmpty() {
        assertNull(GeometryUtils.bytesToGeometry(null));
        assertNull(GeometryUtils.bytesToGeometry(new BytesWritable(new byte[0])));
    }

    @Test
    public void testBytesToGeometry_InvalidWKB() {
        byte[] invalidBytes = new byte[]{0, 1, 2, 3, 4, 5};
        BytesWritable wkb = new BytesWritable(invalidBytes);

        Geometry result = GeometryUtils.bytesToGeometry(wkb);

        assertNull(result);
    }

    @Test
    public void testGeometryToBytes_ValidGeometry() {
        Point point = factory.createPoint(new Coordinate(30.0, 40.0));
        
        BytesWritable result = GeometryUtils.geometryToBytes(point);

        assertNotNull(result);
        assertTrue(result.getLength() > 0);
        
        // 원복 테스트
        Geometry reversed = GeometryUtils.bytesToGeometry(result);
        assertNotNull(reversed);
        assertEquals(30.0, reversed.getCoordinate().x, 0.0001);
        assertEquals(40.0, reversed.getCoordinate().y, 0.0001);
    }

    @Test
    public void testGeometryToBytes_Null() {
        assertNull(GeometryUtils.geometryToBytes(null));
    }
    
    @Test
    public void testGeometryToBytes_3DGeometry() {
        // GeometryUtils에서 WKBWriter(3, true)를 사용하므로 3D 좌표 보존 여부 확인
        Point point = factory.createPoint(new Coordinate(10.0, 20.0, 30.0));
        
        BytesWritable result = GeometryUtils.geometryToBytes(point);
        assertNotNull(result);
        
        Geometry reversed = GeometryUtils.bytesToGeometry(result);
        assertNotNull(reversed);
        assertEquals(10.0, reversed.getCoordinate().x, 0.0001);
        assertEquals(20.0, reversed.getCoordinate().y, 0.0001);
        // JTS에서 Coordinate.getZ() 또는 Coordinate.z를 사용
        assertEquals(30.0, reversed.getCoordinate().getZ(), 0.0001);
    }

    @Test
    public void testStringToGeometry_ValidWKT() {
        String wkt = "POINT (10 20)";
        Geometry result = GeometryUtils.stringToGeometry(wkt);

        assertNotNull(result);
        assertTrue(result instanceof Point);
        assertEquals(10.0, result.getCoordinate().x, 0.0001);
        assertEquals(20.0, result.getCoordinate().y, 0.0001);
    }

    @Test
    public void testStringToGeometry_NullOrEmpty() {
        assertNull(GeometryUtils.stringToGeometry(null));
        assertNull(GeometryUtils.stringToGeometry(""));
    }

    @Test
    public void testStringToGeometry_InvalidWKT() {
        String invalidWkt = "INVALID GEOMETRY (10 20)";
        Geometry result = GeometryUtils.stringToGeometry(invalidWkt);

        assertNull(result);
    }

    @Test
    public void testGeometryToString_ValidGeometry() {
        Point point = factory.createPoint(new Coordinate(30.0, 40.0));
        String result = GeometryUtils.geometryToString(point);

        assertNotNull(result);
        assertTrue(result.contains("POINT"));
        assertTrue(result.contains("30"));
        assertTrue(result.contains("40"));

        // 원복 테스트
        Geometry reversed = GeometryUtils.stringToGeometry(result);
        assertNotNull(reversed);
        assertEquals(30.0, reversed.getCoordinate().x, 0.0001);
        assertEquals(40.0, reversed.getCoordinate().y, 0.0001);
    }

    @Test
    public void testGeometryToString_Null() {
        assertNull(GeometryUtils.geometryToString(null));
    }

    @Test
    public void testGeometryToString_3DGeometry() {
        Point point = factory.createPoint(new Coordinate(10.0, 20.0, 30.0));
        String result = GeometryUtils.geometryToString(point);

        assertNotNull(result);
        // WKTWriter(3)를 사용하므로 Z 좌표가 포함되어야 함
        assertTrue(result.contains("30"));

        Geometry reversed = GeometryUtils.stringToGeometry(result);
        assertNotNull(reversed);
        assertEquals(10.0, reversed.getCoordinate().x, 0.0001);
        assertEquals(20.0, reversed.getCoordinate().y, 0.0001);
        assertEquals(30.0, reversed.getCoordinate().getZ(), 0.0001);
    }
}
