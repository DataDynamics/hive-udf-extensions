package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.algorithm.hull.ConcaveHull;
import org.locationtech.jts.geom.Geometry;

/**
 * 점 집합이나 공간 객체를 감싸는 오목한(Concave) 경계 다각형을 생성하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.SDO_CONCAVEHULL 또는 SDO_GEOM.SDO_CONCAVEHULL_BOUNDARY
 * 함수와 유사한 기능을 제공합니다. JTS의 ConcaveHull 알고리즘을 사용하여
 * 입력 도형을 감싸는 오목한 다각형을 계산합니다.</p>
 *
 * <h3>Convex Hull vs Concave Hull</h3>
 * <pre>
 *   입력 점 집합:                 Convex Hull:              Concave Hull:
 *
 *        *     *                    ┌─────────┐              ┌─────────┐
 *      *         *                 ╱           ╲            ╱     ╲   ╱
 *    *             *              ╱             ╲          ╱       ╲ ╱
 *      *       *                 │               │        │    ╲   ╱
 *        * *                     │               │        │     ╲ ╱
 *      *     *                    ╲             ╱          ╲     ╱
 *    *         *                   ╲           ╱            ╲   ╱
 *        *                          └─────────┘              └─┘
 *
 *   → 점들의 분포 형태          → 모든 점을 감싸는         → 점들의 형태를 따라가는
 *                                 최소 볼록 다각형           오목한 다각형
 * </pre>
 *
 * <h3>Concave Hull의 특징</h3>
 * <ul>
 *   <li><b>볼록성(Convexity)</b>: Convex Hull과 달리 오목한 부분을 가질 수 있음</li>
 *   <li><b>형태 근사</b>: 입력 점들의 실제 분포 형태를 더 잘 반영</li>
 *   <li><b>tolerance 제어</b>: 오목함의 정도를 파라미터로 조절 가능</li>
 *   <li><b>Alpha Shape</b>: 수학적으로 Alpha Shape 알고리즘과 유사한 개념</li>
 * </ul>
 *
 * <h3>tolerance (Maximum Edge Length) 파라미터</h3>
 * <p>tolerance 값은 결과 다각형 경계의 최대 변(edge) 길이를 제한합니다:</p>
 * <table border="1">
 *   <tr><th>tolerance 값</th><th>결과</th><th>설명</th></tr>
 *   <tr><td>큰 값 (또는 NULL)</td><td>Convex Hull에 가까움</td><td>긴 변을 허용하여 볼록한 형태</td></tr>
 *   <tr><td>중간 값</td><td>적당히 오목함</td><td>일반적인 Concave Hull</td></tr>
 *   <tr><td>작은 값</td><td>매우 오목함</td><td>점들의 형태를 세밀하게 따라감</td></tr>
 *   <tr><td>너무 작은 값</td><td>분리된 다각형</td><td>연결이 끊어질 수 있음</td></tr>
 * </table>
 *
 * <pre>
 *   tolerance 효과 시각화:
 *
 *   tolerance = ∞ (Convex)      tolerance = 50           tolerance = 10
 *   ┌───────────────┐           ┌────────┐              ┌──┐
 *   │               │           │   ╲    │              │ ╲│
 *   │               │           │    ╲   │              │  │
 *   │               │           │     ╲  │              │ ╱│
 *   │               │           │      ╲ │              │╱ │
 *   └───────────────┘           └────────┘              └──┘
 *   (볼록한 형태)               (약간 오목)             (매우 오목)
 * </pre>
 *
 * <h3>사용 시나리오</h3>
 * <ul>
 *   <li><b>지역 경계 추출</b>: 시설물 분포 점에서 서비스 영역 경계 추정</li>
 *   <li><b>클러스터 시각화</b>: 군집화된 점들의 실제 분포 형태 표현</li>
 *   <li><b>지리적 범위</b>: GPS 궤적 점에서 실제 이동 영역 추정</li>
 *   <li><b>자연 경계</b>: 산림, 호수 등 자연 지형의 경계 근사</li>
 *   <li><b>데이터 분석</b>: 점 데이터의 공간적 분포 패턴 시각화</li>
 * </ul>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_concave_hull AS 'io.datadynamics.hive.udf.geospatial.SDO_ConcaveHull';
 *
 * -- 기본 Concave Hull 생성 (tolerance 자동 계산)
 * SELECT sdo_concave_hull(point_collection, NULL) AS boundary
 * FROM store_locations;
 *
 * -- tolerance 지정하여 오목함 정도 조절
 * SELECT sdo_concave_hull(point_collection, 100.0) AS boundary
 * FROM store_locations;
 *
 * -- GPS 궤적 점에서 이동 영역 추정
 * SELECT
 *   user_id,
 *   sdo_concave_hull(ST_Collect(gps_point), 50.0) AS movement_area
 * FROM gps_tracks
 * GROUP BY user_id;
 *
 * -- 배달 가능 지역 경계 생성
 * SELECT
 *   store_id,
 *   ST_AsText(sdo_concave_hull(delivery_points, 500.0)) AS delivery_zone_wkt
 * FROM delivery_history
 * GROUP BY store_id;
 *
 * -- Convex Hull과 Concave Hull 면적 비교
 * SELECT
 *   region_id,
 *   ST_Area(ST_ConvexHull(points)) AS convex_area,
 *   ST_Area(sdo_concave_hull(points, 100.0)) AS concave_area,
 *   ST_Area(sdo_concave_hull(points, 100.0)) / ST_Area(ST_ConvexHull(points)) AS ratio
 * FROM point_clusters;
 * -- Concave Hull 면적은 항상 Convex Hull보다 작거나 같음
 * }</pre>
 *
 * <h3>주의사항</h3>
 * <ul>
 *   <li>tolerance 값이 너무 작으면 결과가 여러 개의 분리된 다각형이 될 수 있습니다</li>
 *   <li>점이 3개 미만이면 유효한 다각형을 생성할 수 없습니다</li>
 *   <li>입력이 이미 폴리곤인 경우 정점들에 대한 Concave Hull이 계산됩니다</li>
 *   <li>tolerance가 NULL이면 JTS가 자동으로 적절한 값을 계산합니다</li>
 *   <li>대량의 점에 대해 계산 비용이 높을 수 있습니다</li>
 * </ul>
 *
 * <h3>관련 함수</h3>
 * <ul>
 *   <li>SDO_GEOM.SDO_CONCAVEHULL (Oracle) - Oracle Spatial Concave Hull</li>
 *   <li>ST_ConcaveHull (PostGIS) - PostGIS Concave Hull</li>
 *   <li>ST_ConvexHull - 볼록 껍질 계산</li>
 *   <li>ST_AlphaShape (PostGIS) - Alpha Shape 알고리즘</li>
 * </ul>
 *
 * @see UDF
 * @see ConcaveHull
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_GEOM-reference.html">Oracle SDO_GEOM.SDO_CONCAVEHULL</a>
 * @see <a href="https://locationtech.github.io/jts/javadoc/org/locationtech/jts/algorithm/hull/ConcaveHull.html">JTS ConcaveHull</a>
 */
public class SDO_ConcaveHull extends UDF {

    /**
     * 입력 도형에 대한 Concave Hull(오목 껍질)을 계산합니다.
     *
     * <p>JTS의 ConcaveHull 알고리즘을 사용하여 입력 도형(점 집합, 라인, 폴리곤)을
     * 감싸는 오목한 경계 다각형을 생성합니다. tolerance 파라미터로 오목함의 정도를
     * 제어할 수 있습니다.</p>
     *
     * <h4>처리 흐름</h4>
     * <ol>
     *   <li>BytesWritable을 JTS Geometry 객체로 변환</li>
     *   <li>ConcaveHull 객체 생성</li>
     *   <li>tolerance 값이 유효한 경우 최대 변 길이 설정</li>
     *   <li>Concave Hull 계산 수행</li>
     *   <li>결과를 WKB 바이트 배열로 반환</li>
     * </ol>
     *
     * <h4>알고리즘 개요</h4>
     * <p>JTS ConcaveHull 알고리즘은 다음과 같이 동작합니다:</p>
     * <ol>
     *   <li>입력 점들의 Delaunay Triangulation(들로네 삼각분할) 생성</li>
     *   <li>외곽 삼각형의 변 중 최대 길이를 초과하는 변 제거</li>
     *   <li>남은 삼각형들의 외곽 경계를 연결하여 Concave Hull 생성</li>
     * </ol>
     *
     * @param geomBytes WKB(Well-Known Binary) 형식의 공간 객체 바이트 배열.
     *                  POINT, MULTIPOINT, LINESTRING, POLYGON 등 모든 도형 타입 가능.
     *                  null인 경우 null 반환
     * @param tolerance 최대 변(edge) 길이 제한값.
     *                  <ul>
     *                    <li>단위: 좌표계의 단위와 동일 (예: 미터, 도)</li>
     *                    <li>값이 클수록: Convex Hull에 가까운 결과</li>
     *                    <li>값이 작을수록: 더 오목한 결과 (점 분포를 세밀하게 따라감)</li>
     *                    <li>NULL: JTS가 자동으로 적절한 값을 계산</li>
     *                    <li>0 이하: NULL과 동일하게 자동 계산</li>
     *                  </ul>
     * @return Concave Hull 폴리곤의 WKB 바이트 배열.
     *         <ul>
     *           <li>입력이 null인 경우: null</li>
     *           <li>점이 3개 미만인 경우: 입력 도형 또는 빈 폴리곤</li>
     *           <li>정상적인 경우: 오목한 경계 폴리곤</li>
     *         </ul>
     */
    public BytesWritable evaluate(BytesWritable geomBytes, Double tolerance) {
        // 바이트 배열을 JTS Geometry 객체로 변환
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // null 입력 처리
        if (geom == null) return null;

        // JTS ConcaveHull 객체 생성
        // ConcaveHull은 입력 도형의 정점들에 대해 오목 껍질을 계산
        ConcaveHull hull = new ConcaveHull(geom);

        // tolerance(최대 변 길이) 설정
        // Oracle의 tolerance 파라미터를 JTS의 MaximumEdgeLength로 매핑
        // - tolerance가 null이거나 0 이하면 JTS 자동 계산 로직 사용
        // - tolerance가 양수면 해당 값을 최대 변 길이로 설정
        if (tolerance != null && tolerance > 0) {
            hull.setMaximumEdgeLength(tolerance);
        }

        // Concave Hull 계산 수행
        // 내부적으로 Delaunay Triangulation을 기반으로 오목 경계 계산
        Geometry result = hull.getHull();

        // 결과를 WKB 바이트 배열로 변환하여 반환
        return GeometryUtils.geometryToBytes(result);
    }

}