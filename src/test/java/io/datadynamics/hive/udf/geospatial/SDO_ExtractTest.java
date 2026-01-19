package io.datadynamics.hive.udf.geospatial;

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
    public void setUp() {
        udf = new SDO_Extract();
        factory = new GeometryFactory();
        reader = new WKTReader(factory);
    }

    @Test
    public void testExtractElementFromMultiPoint() throws Exception {
        // MULTIPOINT (0 0, 10 10, 20 20)
        String wkt = "MULTIPOINT ((0 0), (10 10), (20 20))";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);

        // 1번째 요소 추출 (Point(0,0))
        BytesWritable result1 = udf.evaluate(inputWkb, 1);
        assertNotNull(result1);
        Geometry resGeom1 = GeometryUtils.bytesToGeometry(result1);
        assertTrue(resGeom1 instanceof Point);
        assertEquals(0.0, resGeom1.getCoordinate().x, 0.0001);

        // 2번째 요소 추출 (Point(10,10))
        BytesWritable result2 = udf.evaluate(inputWkb, 2);
        Geometry resGeom2 = GeometryUtils.bytesToGeometry(result2);
        assertEquals(10.0, resGeom2.getCoordinate().x, 0.0001);

        // 3번째 요소 추출 (Point(20,20))
        BytesWritable result3 = udf.evaluate(inputWkb, 3);
        Geometry resGeom3 = GeometryUtils.bytesToGeometry(result3);
        assertEquals(20.0, resGeom3.getCoordinate().x, 0.0001);

        // 존재하지 않는 인덱스 (4)
        assertNull(udf.evaluate(inputWkb, 4));
        // 잘못된 인덱스 (0)
        assertNull(udf.evaluate(inputWkb, 0));
    }

    @Test
    public void testExtractElementFromMultiPolygon() throws Exception {
        // MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))
        String wkt = "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);

        // 2번째 폴리곤 추출
        BytesWritable result = udf.evaluate(inputWkb, 2);
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
        BytesWritable exteriorWkb = udf.evaluate(inputWkb, 1, 1);
        assertNotNull(exteriorWkb);
        Geometry exterior = GeometryUtils.bytesToGeometry(exteriorWkb);
        assertTrue(exterior instanceof LineString);
        assertEquals(5, exterior.getNumPoints());
        assertEquals(0.0, exterior.getCoordinates()[0].x, 0.0001);

        // 1번째 요소의 2번째 링 (Interior Ring / Hole)
        BytesWritable interiorWkb = udf.evaluate(inputWkb, 1, 2);
        assertNotNull(interiorWkb);
        Geometry interior = GeometryUtils.bytesToGeometry(interiorWkb);
        assertTrue(interior instanceof LineString);
        assertEquals(5, interior.getNumPoints());
        assertEquals(2.0, interior.getCoordinates()[0].x, 0.0001);

        // 존재하지 않는 링 인덱스 (3)
        assertNull(udf.evaluate(inputWkb, 1, 3));
    }

    @Test
    public void testExtractRingFromNonPolygon() throws Exception {
        // Point에서 Ring을 추출하려고 시도 (Ring Index 무시하고 Element 반환하거나 null 반환 확인)
        // 현재 구현: rIdx < 0 || !(element instanceof Polygon) 이면 Element 반환
        // 만약 rIdx >= 0 이고 Polygon이 아니면? 
        // 코드: if (rIdx < 0 || !(element instanceof Polygon)) { return GeometryUtils.geometryToBytes(element); }
        // 즉 rIdx 가 0이어도(ringIndex=1) Polygon이 아니면 그냥 Element(Point) 반환
        
        String wkt = "POINT (1 1)";
        Geometry geom = reader.read(wkt);
        BytesWritable inputWkb = GeometryUtils.geometryToBytes(geom);
        
        BytesWritable result = udf.evaluate(inputWkb, 1, 1);
        assertNotNull(result);
        Geometry resGeom = GeometryUtils.bytesToGeometry(result);
        assertTrue(resGeom instanceof Point);
    }

    @Test
    public void testNullInputs() {
        assertNull(udf.evaluate(null, 1));
        assertNull(udf.evaluate(new BytesWritable(new byte[]{1, 2, 3}), null));
    }

    @Test
    public void testInvalidGeometry() {
        BytesWritable invalidWkb = new BytesWritable(new byte[]{0, 0, 0, 0});
        assertNull(udf.evaluate(invalidWkb, 1));
    }
}
