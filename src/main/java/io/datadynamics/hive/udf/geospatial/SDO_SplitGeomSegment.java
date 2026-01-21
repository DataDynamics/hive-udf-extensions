package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.linearref.LengthIndexedLine;

/**
 * LRS(Linear Referencing System) 선형 객체를 지정된 Measure(거리) 값을 기준으로
 * 두 개의 세그먼트로 분할하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_LRS.SPLIT_GEOM_SEGMENT 함수와 유사한 기능을 제공합니다.
 * 도로, 파이프라인, 철도 등 선형 네트워크에서 특정 지점을 기준으로 구간을 분리할 때 사용합니다.</p>
 *
 * <h3>LRS(Linear Referencing System)란?</h3>
 * <p>선형 참조 시스템은 선형 객체(도로, 철도 등)를 따라 위치를 지정하는 방법입니다.
 * X, Y 좌표 대신 선의 시작점으로부터의 거리(Measure, M값)를 사용하여 위치를 표현합니다.</p>
 *
 * <pre>
 *   선형 객체 (LINESTRING)
 *
 *   Start                    Split Point                      End
 *     ●━━━━━━━━━━━━━━━━━━━━━━━━━●━━━━━━━━━━━━━━━━━━━━━━━━━━━━━●
 *     |                         |                              |
 *   M=0                      M=50                           M=100
 *     |←─────── Segment 1 ─────→|←─────── Segment 2 ──────────→|
 *
 *   결과: GeometryCollection[Segment1, Segment2]
 * </pre>
 *
 * <h3>사용 시나리오</h3>
 * <ul>
 *   <li><b>도로 관리</b>: 도로를 특정 km 지점에서 분할하여 구간별 관리</li>
 *   <li><b>유지보수</b>: 파이프라인을 점검 지점에서 분리하여 구역별 점검</li>
 *   <li><b>사고 처리</b>: 사고 지점을 기준으로 도로 구간 분리</li>
 *   <li><b>행정 구역</b>: 시/군 경계에서 도로를 분할하여 관할권 구분</li>
 * </ul>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_split_geom_segment AS 'io.datadynamics.hive.udf.geospatial.SDO_SplitGeomSegment';
 *
 * -- 도로를 50km 지점에서 분할
 * SELECT sdo_split_geom_segment(road_geom, 50.0) AS split_segments
 * FROM roads
 * WHERE road_id = 'R001';
 *
 * -- 결과: GeometryCollection(
 * --   LINESTRING(0 0, 25 25, 50 50),     -- 시작점 ~ 50km
 * --   LINESTRING(50 50, 75 75, 100 100)  -- 50km ~ 끝점
 * -- )
 *
 * -- 파이프라인을 중간 지점(전체 길이의 50%)에서 분할
 * SELECT sdo_split_geom_segment(
 *          pipe_geom,
 *          ST_Length(pipe_geom) * 0.5  -- 전체 길이의 절반 지점
 *        ) AS split_pipes
 * FROM pipelines;
 *
 * -- 분할 결과에서 첫 번째 세그먼트만 추출
 * SELECT ST_GeometryN(
 *          sdo_split_geom_segment(road_geom, 30.0),
 *          1  -- 첫 번째 세그먼트
 *        ) AS first_segment
 * FROM roads;
 * }</pre>
 *
 * <h3>반환 형식</h3>
 * <p>분할 결과는 {@code GeometryCollection}으로 반환되며, 두 개의 요소를 포함합니다:</p>
 * <ul>
 *   <li><b>요소 1</b>: 시작점(Start)부터 분할 지점(Split Point)까지의 세그먼트</li>
 *   <li><b>요소 2</b>: 분할 지점(Split Point)부터 끝점(End)까지의 세그먼트</li>
 * </ul>
 *
 * <h3>주의사항</h3>
 * <ul>
 *   <li>분할 지점(splitMeasure)은 선형 객체의 시작(0)과 끝(총 길이) 사이의 값이어야 합니다</li>
 *   <li>범위를 벗어난 분할 지점을 지정하면 NULL이 반환됩니다</li>
 *   <li>입력 도형이 LINESTRING 또는 선형 타입이어야 합니다</li>
 *   <li>분할 지점이 정확히 시작점이나 끝점인 경우, 한쪽 세그먼트가 빈 도형이 될 수 있습니다</li>
 * </ul>
 *
 * <h3>관련 함수</h3>
 * <ul>
 *   <li>SDO_LRS.CLIP_GEOM_SEGMENT - 특정 구간만 추출</li>
 *   <li>SDO_LRS.CONCATENATE_GEOM_SEGMENTS - 세그먼트 연결</li>
 *   <li>SDO_LRS.LOCATE_PT - 특정 M값의 좌표 반환</li>
 * </ul>
 *
 * @see UDF
 * @see LengthIndexedLine
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_LRS-reference.html">Oracle SDO_LRS.SPLIT_GEOM_SEGMENT</a>
 * @see <a href="https://locationtech.github.io/jts/javadoc/org/locationtech/jts/linearref/LengthIndexedLine.html">JTS LengthIndexedLine</a>
 */
public class SDO_SplitGeomSegment extends UDF {

    /**
     * JTS GeometryFactory 인스턴스.
     * GeometryCollection 생성에 사용되며, 스레드 안전하므로 재사용합니다.
     */
    private final GeometryFactory factory = new GeometryFactory();

    /**
     * 선형 객체를 지정된 거리(Measure) 값을 기준으로 두 개의 세그먼트로 분할합니다.
     *
     * <p>JTS의 {@link LengthIndexedLine}을 사용하여 선형 참조 기반 분할을 수행합니다.
     * 분할 결과는 두 개의 세그먼트를 포함하는 GeometryCollection으로 반환됩니다.</p>
     *
     * <h4>처리 흐름</h4>
     * <ol>
     *   <li>BytesWritable을 JTS Geometry 객체로 변환</li>
     *   <li>LengthIndexedLine으로 선형 참조 시스템 초기화</li>
     *   <li>분할 지점의 유효성 검증 (시작~끝 범위 내)</li>
     *   <li>시작점~분할점, 분할점~끝점 두 세그먼트 추출</li>
     *   <li>GeometryCollection으로 묶어서 반환</li>
     * </ol>
     *
     * @param geomBytes    WKB(Well-Known Binary) 형식의 선형 공간 객체 바이트 배열.
     *                     LINESTRING, MULTILINESTRING 등 선형 타입이어야 합니다.
     *                     null인 경우 null 반환
     * @param splitMeasure 분할 기준 거리(Measure) 값.
     *                     선형 객체의 시작점으로부터의 거리를 나타냅니다.
     *                     <ul>
     *                       <li>단위: 좌표계의 단위와 동일 (예: 미터, 도)</li>
     *                       <li>유효 범위: 0 ~ 선형 객체의 총 길이</li>
     *                     </ul>
     *                     null이거나 범위를 벗어난 경우 null 반환
     * @return 분할된 두 세그먼트를 포함하는 GeometryCollection의 WKB 바이트 배열.
     * <ul>
     *   <li>요소[0]: 시작점 ~ 분할점 세그먼트</li>
     *   <li>요소[1]: 분할점 ~ 끝점 세그먼트</li>
     * </ul>
     * 입력이 null이거나 분할 지점이 유효하지 않은 경우 null 반환
     */
    public BytesWritable evaluate(BytesWritable geomBytes, Double splitMeasure) {
        // 바이트 배열을 JTS Geometry 객체로 변환
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // null 입력 검증
        if (geom == null || splitMeasure == null) return null;

        // JTS LengthIndexedLine 초기화
        // LengthIndexedLine은 선형 객체를 거리(length) 기반으로 참조할 수 있게 해주는 클래스
        // 내부적으로 선형 객체의 각 정점까지의 누적 거리를 계산하여 인덱싱
        LengthIndexedLine lil = new LengthIndexedLine(geom);

        // 선형 객체의 시작 인덱스(항상 0)와 끝 인덱스(총 길이) 획득
        double startIndex = lil.getStartIndex();  // 시작점: 0
        double endIndex = lil.getEndIndex();      // 끝점: 선형 객체의 총 길이

        // 분할 지점 유효성 검증
        // 분할 지점은 반드시 [시작, 끝] 범위 내에 있어야 함
        if (splitMeasure < startIndex || splitMeasure > endIndex) {
            // 범위를 벗어난 분할 지점은 처리 불가
            return null;
        }

        // 선형 객체를 두 세그먼트로 분할
        // extractLine(start, end): 지정된 구간의 부분 선형 객체 추출
        Geometry seg1 = lil.extractLine(startIndex, splitMeasure);  // 시작점 ~ 분할점
        Geometry seg2 = lil.extractLine(splitMeasure, endIndex);    // 분할점 ~ 끝점

        // 두 세그먼트를 GeometryCollection으로 묶어서 반환
        // GeometryCollection은 여러 도형을 하나의 객체로 그룹화
        Geometry[] geometries = new Geometry[]{seg1, seg2};
        Geometry result = factory.createGeometryCollection(geometries);

        // 결과를 WKB 바이트 배열로 변환하여 반환
        return GeometryUtils.geometryToBytes(result);
    }
}