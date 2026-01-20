package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

/**
 * Hive UDF로서 공간 객체(Geometry)를 단순화(Simplify)하는 함수입니다.
 *
 * <p>이 클래스는 Oracle Spatial의 SDO_UTIL.SIMPLIFY 함수와 유사한 기능을 제공합니다.
 * Douglas-Peucker 알고리즘을 기반으로 하는 {@link TopologyPreservingSimplifier}를 사용하여
 * 기하 도형의 정점(vertex) 수를 줄이면서도 위상(topology) 관계를 보존합니다.</p>
 *
 * <h2>사용 목적</h2>
 * <ul>
 *   <li>대용량 공간 데이터의 저장 공간 절약</li>
 *   <li>지도 렌더링 성능 향상 (줌 레벨에 따른 세밀도 조절)</li>
 *   <li>공간 연산 속도 개선 (정점 수 감소로 인한 계산량 감소)</li>
 *   <li>데이터 전송량 최소화</li>
 * </ul>
 *
 * <h2>Douglas-Peucker 알고리즘</h2>
 * <p>Douglas-Peucker 알고리즘은 곡선을 근사화하는 데 널리 사용되는 알고리즘입니다.
 * 주어진 허용 오차(tolerance/threshold) 내에서 원본 형상과 가장 유사한 단순화된 형상을 생성합니다.
 * 알고리즘은 재귀적으로 동작하며, 시작점과 끝점을 연결한 직선에서 가장 먼 점을 찾아
 * 그 거리가 허용 오차보다 크면 해당 점을 유지하고, 그렇지 않으면 제거합니다.</p>
 *
 * <h2>TopologyPreservingSimplifier의 특징</h2>
 * <p>JTS 라이브러리의 {@link TopologyPreservingSimplifier}는 일반 Douglas-Peucker 알고리즘과 달리
 * 다음과 같은 위상 관계를 보존합니다:</p>
 * <ul>
 *   <li>폴리곤의 유효성 유지 (자기 교차 방지)</li>
 *   <li>인접 폴리곤 간의 경계 일관성 유지</li>
 *   <li>링(ring)의 최소 정점 수 보장</li>
 * </ul>
 *
 * <h2>Hive에서의 사용 예시</h2>
 * <pre>{@code
 * -- UDF 등록
 * CREATE TEMPORARY FUNCTION SDO_Simplify AS 'io.datadynamics.hive.udf.geospatial.SDO_Simplify';
 *
 * -- 사용 예시: 허용 오차 0.001로 geometry 단순화
 * SELECT SDO_Simplify(geometry_column, 0.001) AS simplified_geom
 * FROM spatial_table;
 *
 * -- 줌 레벨에 따른 동적 단순화
 * SELECT
 *   CASE
 *     WHEN zoom_level < 5 THEN SDO_Simplify(geom, 0.01)
 *     WHEN zoom_level < 10 THEN SDO_Simplify(geom, 0.001)
 *     ELSE geom
 *   END AS display_geom
 * FROM map_features;
 * }</pre>
 *
 * <h2>threshold(허용 오차) 값 선택 가이드</h2>
 * <ul>
 *   <li><b>0.0001</b>: 매우 정밀한 단순화, 원본과 거의 동일</li>
 *   <li><b>0.001</b>: 일반적인 용도에 적합한 단순화</li>
 *   <li><b>0.01</b>: 중간 수준의 단순화, 대략적인 형태 유지</li>
 *   <li><b>0.1</b>: 강한 단순화, 개략적인 윤곽만 유지</li>
 * </ul>
 * <p>참고: threshold 값은 좌표계의 단위에 따라 다르게 적용됩니다.
 * WGS84(EPSG:4326)의 경우 도(degree) 단위이며, UTM 좌표계의 경우 미터(meter) 단위입니다.</p>
 *
 * @author Data Dynamics
 * @version 1.0
 * @see org.locationtech.jts.simplify.TopologyPreservingSimplifier
 * @see org.locationtech.jts.simplify.DouglasPeuckerSimplifier
 * @see <a href="https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm">Douglas-Peucker Algorithm (Wikipedia)</a>
 */
public class SDO_Simplify extends UDF {

    /**
     * 주어진 geometry를 지정된 허용 오차(threshold)로 단순화합니다.
     *
     * <p>이 메서드는 {@link TopologyPreservingSimplifier}를 사용하여 입력 geometry의
     * 정점 수를 줄이면서 위상 관계를 보존합니다. 단순화 결과는 직렬화된 바이트 배열로 반환됩니다.</p>
     *
     * <h3>처리 흐름</h3>
     * <ol>
     *   <li>입력된 바이트 배열을 JTS Geometry 객체로 역직렬화</li>
     *   <li>입력값 유효성 검사 (null 체크)</li>
     *   <li>TopologyPreservingSimplifier를 사용하여 geometry 단순화</li>
     *   <li>결과 geometry를 바이트 배열로 직렬화하여 반환</li>
     * </ol>
     *
     * <h3>지원 Geometry 타입</h3>
     * <ul>
     *   <li>Point - 단순화 효과 없음 (정점이 1개이므로)</li>
     *   <li>LineString - 정점 수 감소</li>
     *   <li>Polygon - 외부/내부 링의 정점 수 감소</li>
     *   <li>MultiPoint - 단순화 효과 없음</li>
     *   <li>MultiLineString - 각 LineString에 대해 단순화 적용</li>
     *   <li>MultiPolygon - 각 Polygon에 대해 단순화 적용</li>
     *   <li>GeometryCollection - 각 구성요소에 대해 단순화 적용</li>
     * </ul>
     *
     * <h3>주의 사항</h3>
     * <ul>
     *   <li>threshold 값이 0이면 원본 geometry가 그대로 반환됩니다.</li>
     *   <li>threshold 값이 너무 크면 geometry가 과도하게 단순화되어 형태가 왜곡될 수 있습니다.</li>
     *   <li>매우 작은 geometry에 큰 threshold를 적용하면 빈 geometry가 반환될 수 있습니다.</li>
     * </ul>
     *
     * @param geomBytes 단순화할 geometry의 직렬화된 바이트 배열 (WKB 형식)
     *                  null인 경우 null을 반환합니다.
     * @param threshold 단순화에 사용할 허용 오차(tolerance) 값
     *                  이 값은 좌표계의 단위(도 또는 미터)에 따라 해석됩니다.
     *                  양수 값이어야 하며, null인 경우 null을 반환합니다.
     * @return 단순화된 geometry의 직렬화된 바이트 배열
     *         입력이 null이거나 유효하지 않은 경우 null을 반환합니다.
     *
     * @see GeometryUtils#bytesToGeometry(BytesWritable) 바이트 배열을 Geometry로 변환
     * @see GeometryUtils#geometryToBytes(Geometry) Geometry를 바이트 배열로 변환
     * @see TopologyPreservingSimplifier#simplify(Geometry, double) 위상 보존 단순화 수행
     */
    public BytesWritable evaluate(BytesWritable geomBytes, Double threshold) {
        // 1. 입력 바이트 배열을 JTS Geometry 객체로 역직렬화
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // 2. null 체크: geometry 또는 threshold가 null이면 null 반환
        if (geom == null || threshold == null) return null;

        // 3. TopologyPreservingSimplifier를 사용하여 geometry 단순화
        //    - Douglas-Peucker 알고리즘 기반
        //    - 위상 관계(topology) 보존
        //    - 폴리곤의 자기 교차 방지
        Geometry simplified = TopologyPreservingSimplifier.simplify(geom, threshold);

        // 4. 단순화된 geometry를 바이트 배열로 직렬화하여 반환
        return GeometryUtils.geometryToBytes(simplified);
    }

}
