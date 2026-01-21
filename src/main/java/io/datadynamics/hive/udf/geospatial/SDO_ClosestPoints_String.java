package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.operation.distance.DistanceOp;

/**
 * 두 공간 객체 간의 최단 거리를 형성하는 두 점(Closest Points)을 계산하여
 * 이 두 점을 연결하는 LINESTRING으로 반환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.SDO_CLOSEST_POINTS 함수와 유사한 기능을 제공합니다.
 * 두 도형 사이의 최단 거리에 해당하는 점 쌍을 찾아, 이를 연결하는 선분(LINESTRING)으로 반환합니다.</p>
 *
 * <h3>최근접점(Closest Points)이란?</h3>
 * <p>두 공간 객체 간의 최단 거리를 형성하는 점 쌍입니다.
 * 각 도형에서 하나씩 선택된 두 점 사이의 거리가 가장 짧은 경우의 점들입니다.</p>
 *
 * <pre>
 *   도형 A (POLYGON)              도형 B (POLYGON)
 *
 *       ┌────────┐                    ┌────────┐
 *       │        │                    │        │
 *       │    A   │ P1 ●────────● P2   │    B   │
 *       │        │      최단거리       │        │
 *       └────────┘                    └────────┘
 *
 *   결과: LINESTRING(P1.x P1.y, P2.x P2.y)
 *
 *   P1: 도형 A에서 도형 B에 가장 가까운 점
 *   P2: 도형 B에서 도형 A에 가장 가까운 점
 *   P1-P2 사이의 거리 = 두 도형 간의 최단 거리
 * </pre>
 *
 * <h3>다양한 도형 조합 예시</h3>
 * <pre>
 *   POINT - POLYGON:              LINESTRING - LINESTRING:
 *
 *      ●P1                              ╲
 *       ╲                                ╲
 *        ╲ 최단거리                       ●P1
 *         ╲                               │ 최단거리
 *     ┌────●P2───┐                        │
 *     │          │                        ●P2
 *     │  POLYGON │                       ╱
 *     └──────────┘                      ╱
 * </pre>
 *
 * <h3>Oracle SDO_CLOSEST_POINTS와의 차이점</h3>
 * <table border="1">
 *   <tr><th>항목</th><th>Oracle SDO_CLOSEST_POINTS</th><th>이 UDF</th></tr>
 *   <tr><td>반환 타입</td><td>SDO_CLOSEST_POINTS_TYPE (복합 객체)</td><td>WKT LINESTRING</td></tr>
 *   <tr><td>반환 내용</td><td>점1, 점2, 거리</td><td>두 점을 잇는 선분</td></tr>
 *   <tr><td>거리 정보</td><td>포함됨</td><td>ST_Length()로 계산 가능</td></tr>
 *   <tr><td>입력 형식</td><td>SDO_GEOMETRY</td><td>WKT 문자열</td></tr>
 * </table>
 *
 * <h3>사용 시나리오</h3>
 * <ul>
 *   <li><b>시설물 연결</b>: 두 건물 사이의 최단 연결 경로 계산</li>
 *   <li><b>네트워크 분석</b>: 도로 네트워크에서 가장 가까운 연결점 찾기</li>
 *   <li><b>거리 측정</b>: 두 영역 사이의 정확한 최단 거리 지점 파악</li>
 *   <li><b>시각화</b>: 최단 거리를 시각적으로 표현하는 선분 생성</li>
 *   <li><b>연결선 생성</b>: 관련 객체들 사이의 연결선 자동 생성</li>
 * </ul>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_closest_points
 *   AS 'io.datadynamics.hive.udf.geospatial.SDO_ClosestPoints_String';
 *
 * -- 두 폴리곤 사이의 최근접점 선분 계산
 * SELECT sdo_closest_points(
 *   'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))',
 *   'POLYGON ((20 0, 30 0, 30 10, 20 10, 20 0))'
 * ) AS closest_line;
 * -- 결과: LINESTRING (10 5, 20 5)  (예시)
 *
 * -- 점과 라인스트링 사이의 최근접점
 * SELECT sdo_closest_points(
 *   'POINT (0 10)',
 *   'LINESTRING (5 0, 15 0, 25 0)'
 * ) AS closest_line;
 * -- 결과: LINESTRING (0 10, 5 0)  (예시)
 *
 * -- 최단 거리 계산 (결과 LINESTRING의 길이)
 * SELECT ST_Length(
 *   sdo_closest_points(geom1_wkt, geom2_wkt)
 * ) AS min_distance
 * FROM spatial_pairs;
 *
 * -- 가장 가까운 시설물 쌍과 연결선 조회
 * SELECT
 *   a.facility_id AS from_id,
 *   b.facility_id AS to_id,
 *   sdo_closest_points(a.geom_wkt, b.geom_wkt) AS connection_line,
 *   ST_Length(sdo_closest_points(a.geom_wkt, b.geom_wkt)) AS distance
 * FROM facilities a
 * CROSS JOIN facilities b
 * WHERE a.facility_id < b.facility_id
 * ORDER BY distance
 * LIMIT 10;
 *
 * -- 건물과 도로 사이의 최단 접근점 찾기
 * SELECT
 *   building_id,
 *   road_id,
 *   sdo_closest_points(building_wkt, road_wkt) AS access_line
 * FROM buildings
 * CROSS JOIN roads
 * WHERE ST_Distance(building_geom, road_geom) < 100;  -- 100m 이내
 * }</pre>
 *
 * <h3>반환값 활용</h3>
 * <p>반환되는 LINESTRING에서 각 점을 추출할 수 있습니다:</p>
 * <pre>{@code
 * -- 시작점(도형1의 최근접점) 추출
 * SELECT ST_StartPoint(sdo_closest_points(wkt1, wkt2)) AS point_on_geom1;
 *
 * -- 끝점(도형2의 최근접점) 추출
 * SELECT ST_EndPoint(sdo_closest_points(wkt1, wkt2)) AS point_on_geom2;
 *
 * -- 최단 거리 계산
 * SELECT ST_Length(sdo_closest_points(wkt1, wkt2)) AS shortest_distance;
 * }</pre>
 *
 * <h3>주의사항</h3>
 * <ul>
 *   <li>두 도형이 교차하거나 접하는 경우, 두 점이 동일하여 길이가 0인 LINESTRING이 반환됩니다</li>
 *   <li>입력 WKT가 유효하지 않거나 null인 경우 null을 반환합니다</li>
 *   <li>복잡한 도형의 경우 계산 비용이 높을 수 있습니다</li>
 *   <li>반환되는 좌표는 입력 도형의 좌표계를 따릅니다</li>
 * </ul>
 *
 * <h3>관련 함수</h3>
 * <ul>
 *   <li>{@link SDO_ClosestPoints} - WKB 바이트 배열 입력 버전</li>
 *   <li>SDO_GEOM.SDO_CLOSEST_POINTS (Oracle) - Oracle Spatial 최근접점</li>
 *   <li>ST_ClosestPoint (PostGIS) - 한 도형에서 다른 도형까지 가장 가까운 점</li>
 *   <li>ST_ShortestLine (PostGIS) - 유사 기능의 PostGIS 함수</li>
 *   <li>ST_Distance - 두 도형 간의 최단 거리 (숫자로 반환)</li>
 * </ul>
 *
 * @see UDF
 * @see DistanceOp
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_GEOM-reference.html">Oracle SDO_GEOM.SDO_CLOSEST_POINTS</a>
 * @see <a href="https://locationtech.github.io/jts/javadoc/org/locationtech/jts/operation/distance/DistanceOp.html">JTS DistanceOp</a>
 */
public class SDO_ClosestPoints_String extends UDF {

    /**
     * JTS GeometryFactory 인스턴스.
     * LINESTRING 생성에 사용되며, 스레드 안전하므로 재사용합니다.
     */
    private final GeometryFactory factory = new GeometryFactory();

    /**
     * 두 공간 객체 간의 최근접점을 계산하여 연결 선분을 반환합니다.
     *
     * <p>JTS의 {@link DistanceOp}를 사용하여 두 도형 사이의 최단 거리를 형성하는
     * 점 쌍을 계산하고, 이 두 점을 연결하는 LINESTRING을 WKT 문자열로 반환합니다.</p>
     *
     * <h4>처리 흐름</h4>
     * <ol>
     *   <li>WKT 문자열을 JTS Geometry 객체로 변환</li>
     *   <li>DistanceOp.nearestPoints()로 최근접점 쌍 계산</li>
     *   <li>두 좌표를 연결하는 LINESTRING 생성</li>
     *   <li>결과를 WKT 문자열로 반환</li>
     * </ol>
     *
     * <h4>알고리즘 개요</h4>
     * <p>JTS DistanceOp는 다음과 같이 동작합니다:</p>
     * <ol>
     *   <li>두 도형의 모든 정점과 변을 검사</li>
     *   <li>정점-정점, 정점-변, 변-변 거리를 계산</li>
     *   <li>최소 거리를 형성하는 점 쌍을 반환</li>
     * </ol>
     *
     * @param wkt1 첫 번째 공간 객체의 WKT(Well-Known Text) 문자열.
     *             POINT, LINESTRING, POLYGON 등 모든 도형 타입 가능.
     *             null인 경우 null 반환
     * @param wkt2 두 번째 공간 객체의 WKT(Well-Known Text) 문자열.
     *             POINT, LINESTRING, POLYGON 등 모든 도형 타입 가능.
     *             null인 경우 null 반환
     * @return 두 도형의 최근접점을 연결하는 LINESTRING의 WKT 문자열.
     *         <ul>
     *           <li>정상: "LINESTRING (x1 y1, x2 y2)" 형식</li>
     *           <li>입력이 null인 경우: null</li>
     *           <li>WKT 파싱 실패: null</li>
     *           <li>두 도형이 접촉: 길이 0인 LINESTRING (두 점이 동일)</li>
     *         </ul>
     */
    public String evaluate(String wkt1, String wkt2) {
        // WKT 문자열을 JTS Geometry 객체로 변환
        Geometry g1 = GeometryUtils.stringToGeometry(wkt1);
        Geometry g2 = GeometryUtils.stringToGeometry(wkt2);

        // null 입력 검증: 두 도형 모두 유효해야 함
        if (g1 == null || g2 == null) return null;

        // JTS DistanceOp을 사용하여 최근접점 쌍 계산
        // nearestPoints()는 두 도형 간의 최단 거리를 형성하는 점 쌍을 반환
        // 결과 배열: [도형1의 최근접점, 도형2의 최근접점]
        Coordinate[] nearestCoords = DistanceOp.nearestPoints(g1, g2);

        // 유효한 결과가 있는 경우 LINESTRING 생성
        if (nearestCoords != null && nearestCoords.length >= 2) {
            // 두 최근접점을 연결하는 LineString 생성
            // 이 선분의 길이 = 두 도형 간의 최단 거리
            Geometry result = factory.createLineString(nearestCoords);

            // WKT 문자열로 변환하여 반환
            return GeometryUtils.geometryToString(result);
        }

        // 최근접점 계산 실패 시 null 반환
        return null;
    }

}
