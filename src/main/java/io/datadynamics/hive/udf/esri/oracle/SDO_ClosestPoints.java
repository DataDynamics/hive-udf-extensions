package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Hive UDF로서 두 Geometry 객체 간의 최단 거리를 형성하는 두 점을 찾아 반환하는 함수입니다.
 *
 * <p>이 클래스는 Oracle Spatial의 SDO_GEOM.SDO_CLOSEST_POINTS 함수와 유사한 기능을 제공합니다.
 * Oracle에서는 SDO_CLOSEST_POINTS_TYPE 객체를 반환하지만, Hive/Impala 환경에서는
 * 두 점을 잇는 LineString을 반환하여 시각화 및 거리 측정이 용이하도록 설계되었습니다.</p>
 *
 * <h2>주요 기능</h2>
 * <ul>
 *   <li>두 Geometry 간의 최단 거리 점 쌍(nearest points pair) 계산</li>
 *   <li>결과를 LineString으로 반환하여 시각화 및 후속 처리 용이</li>
 *   <li>모든 Geometry 타입 조합에 대해 동작</li>
 * </ul>
 *
 * <h2>사용 목적</h2>
 * <ul>
 *   <li><b>거리 시각화</b>: 두 객체 간 최단 거리를 지도상에 선으로 표시</li>
 *   <li><b>근접 분석</b>: 건물과 도로, 시설과 시설 간의 최단 접근 경로 파악</li>
 *   <li><b>위치 기반 서비스</b>: 가장 가까운 지점 찾기 (예: 가장 가까운 출입구)</li>
 *   <li><b>공간 데이터 품질 검증</b>: 객체 간 간격 확인</li>
 * </ul>
 *
 * <h2>반환 형식</h2>
 * <p>Oracle의 SDO_CLOSEST_POINTS_TYPE과 달리, 이 UDF는 <b>LineString</b>을 반환합니다:</p>
 * <pre>
 * LINESTRING(x1 y1, x2 y2)
 *            ↑       ↑
 *      첫 번째    두 번째
 *     Geometry  Geometry
 *     위의 점   위의 점
 * </pre>
 * <p>이 LineString의 길이가 곧 두 Geometry 간의 최단 거리입니다.</p>
 *
 * <h2>알고리즘</h2>
 * <p>JTS 라이브러리의 {@link DistanceOp}를 사용하여 최단 거리 점 쌍을 계산합니다.
 * 이 알고리즘은 두 Geometry의 모든 정점과 선분을 고려하여 최단 거리를 형성하는
 * 두 점을 찾습니다. 계산 복잡도는 O(n*m)이며, n과 m은 각 Geometry의 정점 수입니다.</p>
 *
 * <h2>지원 Geometry 타입 조합</h2>
 * <table border="1">
 *   <tr><th>Geometry 1</th><th>Geometry 2</th><th>설명</th></tr>
 *   <tr><td>Point</td><td>Point</td><td>두 점 자체가 최단 점</td></tr>
 *   <tr><td>Point</td><td>LineString</td><td>점과 선 위의 가장 가까운 점</td></tr>
 *   <tr><td>Point</td><td>Polygon</td><td>점과 폴리곤 경계의 가장 가까운 점</td></tr>
 *   <tr><td>LineString</td><td>LineString</td><td>두 선 위의 가장 가까운 점 쌍</td></tr>
 *   <tr><td>LineString</td><td>Polygon</td><td>선과 폴리곤 경계의 가장 가까운 점 쌍</td></tr>
 *   <tr><td>Polygon</td><td>Polygon</td><td>두 폴리곤 경계의 가장 가까운 점 쌍</td></tr>
 *   <tr><td colspan="3">Multi* 타입 및 GeometryCollection도 지원</td></tr>
 * </table>
 *
 * <h2>Hive에서의 사용 예시</h2>
 * <pre>{@code
 * -- UDF 등록
 * CREATE TEMPORARY FUNCTION SDO_ClosestPoints AS 'io.datadynamics.hive.udf.geospatial.SDO_ClosestPoints';
 *
 * -- 두 Geometry 간 최단 거리 점 쌍 구하기
 * SELECT SDO_ClosestPoints(building_geom, road_geom) AS closest_line
 * FROM spatial_table;
 *
 * -- 최단 거리 계산과 함께 사용 (ST_Length와 조합)
 * SELECT
 *   building_id,
 *   road_id,
 *   SDO_ClosestPoints(building_geom, road_geom) AS closest_line,
 *   ST_Length(SDO_ClosestPoints(building_geom, road_geom)) AS min_distance
 * FROM buildings
 * CROSS JOIN roads;
 *
 * -- 가장 가까운 시설 찾기
 * SELECT
 *   customer_id,
 *   store_id,
 *   SDO_ClosestPoints(customer_location, store_location) AS connection_line
 * FROM customers
 * CROSS JOIN stores
 * WHERE ST_Length(SDO_ClosestPoints(customer_location, store_location)) < 1000;
 * }</pre>
 *
 * <h2>Oracle과의 차이점</h2>
 * <table border="1">
 *   <tr><th>항목</th><th>Oracle SDO_CLOSEST_POINTS</th><th>이 UDF</th></tr>
 *   <tr><td>반환 타입</td><td>SDO_CLOSEST_POINTS_TYPE (구조체)</td><td>LineString (Geometry)</td></tr>
 *   <tr><td>거리 정보</td><td>DISTANCE 필드에 포함</td><td>ST_Length()로 계산 필요</td></tr>
 *   <tr><td>점 정보</td><td>GEOMA, GEOMB 필드</td><td>LineString의 시작점/끝점</td></tr>
 *   <tr><td>tolerance</td><td>지원</td><td>미지원 (JTS 기본값 사용)</td></tr>
 * </table>
 *
 * <h2>주의 사항</h2>
 * <ul>
 *   <li>두 Geometry가 교차하거나 접촉하는 경우, 접점이 반환됩니다 (거리 = 0).</li>
 *   <li>하나의 Geometry가 다른 Geometry 내부에 있는 경우에도 최단 거리 점 쌍이 계산됩니다.</li>
 *   <li>좌표계에 따라 거리 단위가 달라집니다 (WGS84는 도, UTM은 미터).</li>
 *   <li>구면 거리가 필요한 경우 별도의 좌표 변환이 필요합니다.</li>
 * </ul>
 *
 * @author Data Dynamics
 * @version 1.0
 * @see DistanceOp JTS 거리 연산 클래스
 * @see DistanceOp#nearestPoints(Geometry, Geometry) 최단 거리 점 계산
 * @see GeometryFactory LineString 생성에 사용
 */
public class SDO_ClosestPoints extends ST_GeometryAccessor {

    static final Logger LOG = LoggerFactory.getLogger(SDO_ClosestPoints.class.getName());

    /**
     * 두 개의 ESRI Shape 형식의 기하학 데이터를 입력받아 최단 거리를 형성하는 두 점을 잇는 LineString을 반환합니다.
     *
     * @param geometryref1 첫 번째 기하학 데이터 (BytesWritable)
     * @param geometryref2 두 번째 기하학 데이터 (BytesWritable)
     * @return 최단 거리 점 쌍을 잇는 LineString을 포함하는 BytesWritable. 오류 발생 시 null 반환.
     */
    public BytesWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2) {
        // 입력 데이터 유효성 검사: null 이거나 길이가 0인 경우 처리
        if (geometryref1 == null || geometryref2 == null || geometryref1.getLength() == 0 || geometryref2.getLength() == 0) {
            return null;
        }

        // ESRI Shape 바이너리 데이터를 OGCGeometry 객체로 변환
        OGCGeometry ogcGeom1 = GeometryUtils.geometryFromEsriShape(geometryref1);
        OGCGeometry ogcGeom2 = GeometryUtils.geometryFromEsriShape(geometryref2);
        if (ogcGeom1 == null || ogcGeom2 == null) {
            return null;
        }

        try {
            // OGCGeometry (ESRI) -> WKB -> JTS Geometry 변환
            // JTS의 DistanceOp를 사용하기 위해 JTS 객체로 변환이 필요함
            WKBReader reader = new WKBReader();
            
            // 첫 번째 지오메트리 변환
            ByteBuffer wkb1 = ogcGeom1.asBinary();
            byte[] bytes1 = new byte[wkb1.remaining()];
            wkb1.get(bytes1);
            Geometry jtsGeom1 = reader.read(bytes1);

            // 두 번째 지오메트리 변환
            ByteBuffer wkb2 = ogcGeom2.asBinary();
            byte[] bytes2 = new byte[wkb2.remaining()];
            wkb2.get(bytes2);
            Geometry jtsGeom2 = reader.read(bytes2);

            // JTS DistanceOp를 사용하여 최단 거리 점 쌍(nearest points pair) 계산
            // nearestPoints[0]은 jtsGeom1 위의 점, nearestPoints[1]은 jtsGeom2 위의 점
            Coordinate[] nearestPoints = DistanceOp.nearestPoints(jtsGeom1, jtsGeom2);
            if (nearestPoints == null || nearestPoints.length < 2) {
                return null;
            }

            // 계산된 두 점을 잇는 LineString 생성
            GeometryFactory factory = new GeometryFactory();
            Geometry resultLine = factory.createLineString(nearestPoints);

            // JTS Geometry -> WKB -> OGCGeometry -> ESRI Shape 순으로 다시 변환하여 반환
            WKBWriter writer = new WKBWriter();
            byte[] resultWkb = writer.write(resultLine);
            OGCGeometry ogcResult = OGCGeometry.fromBinary(ByteBuffer.wrap(resultWkb));
            
            // 원본의 공간 참조(SRID)를 결과에 설정
            ogcResult.setSpatialReference(ogcGeom1.esriSR);

            // 최종 OGC 객체를 다시 Hive에서 사용할 수 있는 ESRI Shape 바이너리(BytesWritable)로 변환하여 반환
            return GeometryUtils.geometryToEsriShapeBytesWritable(ogcResult);
        } catch (Exception e) {
            // 변환 또는 계산 중 오류 발생 시 로그를 남기고 null 반환
            LOG.error("Error in SDO_ClosestPoints: " + e.getMessage(), e);
            return null;
        }
    }
}
