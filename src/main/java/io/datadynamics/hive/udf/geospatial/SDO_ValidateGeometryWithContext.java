package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.locationtech.jts.operation.valid.TopologyValidationError;

/**
 * 공간 객체의 기하학적 유효성을 검사하고, 오류 발생 시 원인과 위치 정보를 포함한
 * 상세한 컨텍스트 문자열을 반환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT 함수와 유사한 기능을 제공합니다.
 * 단순히 유효/무효만 판단하는 것이 아니라, 오류의 구체적인 원인과 발생 좌표를 함께 제공하여
 * 데이터 품질 분석 및 디버깅에 활용할 수 있습니다.</p>
 *
 * <h3>OGC Simple Feature 유효성 규칙</h3>
 * <p>이 함수는 OGC(Open Geospatial Consortium) Simple Feature 표준에 정의된
 * 기하학적 유효성 규칙을 기준으로 검사합니다:</p>
 * <ul>
 *   <li>폴리곤은 닫혀 있어야 함 (시작점 = 끝점)</li>
 *   <li>폴리곤 링은 자가 교차하면 안 됨</li>
 *   <li>내부 링(구멍)은 외부 링 안에 있어야 함</li>
 *   <li>링들은 서로 교차하면 안 됨</li>
 *   <li>멀티폴리곤의 요소들은 겹치면 안 됨</li>
 * </ul>
 *
 * <h3>검출 가능한 오류 유형 (TopologyValidationError)</h3>
 * <table border="1">
 *   <tr><th>오류 유형</th><th>설명</th><th>예시 메시지</th></tr>
 *   <tr><td>Self-intersection</td><td>경계가 자기 자신과 교차</td><td>"Self-intersection at (10, 20)"</td></tr>
 *   <tr><td>Ring Self-intersection</td><td>링이 자가 교차</td><td>"Ring Self-intersection at (5, 5)"</td></tr>
 *   <tr><td>Nested holes</td><td>구멍 안에 구멍이 있음</td><td>"Nested holes at (15, 25)"</td></tr>
 *   <tr><td>Disconnected Interior</td><td>내부가 연결되지 않음</td><td>"Disconnected Interior at (0, 0)"</td></tr>
 *   <tr><td>Hole Outside Shell</td><td>구멍이 외부 링 밖에 위치</td><td>"Hole Outside Shell at (100, 100)"</td></tr>
 *   <tr><td>Invalid Coordinate</td><td>좌표값이 NaN 또는 무한대</td><td>"Invalid Coordinate at (NaN, NaN)"</td></tr>
 *   <tr><td>Too Few Points</td><td>정점 수가 부족</td><td>"Too Few Points at (0, 0)"</td></tr>
 * </table>
 *
 * <h3>반환값 형식</h3>
 * <pre>
 *   유효한 경우:
 *   ┌─────────────────────────────────────────┐
 *   │  "TRUE"                                 │
 *   └─────────────────────────────────────────┘
 *
 *   유효하지 않은 경우:
 *   ┌─────────────────────────────────────────┐
 *   │  "FALSE: [오류 유형] at ([X], [Y])"     │
 *   └─────────────────────────────────────────┘
 *   예: "FALSE: Self-intersection at (10.5, 20.3)"
 *
 *   NULL 입력인 경우:
 *   ┌─────────────────────────────────────────┐
 *   │  "NULL GEOMETRY"                        │
 *   └─────────────────────────────────────────┘
 * </pre>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_validate_geometry_with_context
 *   AS 'io.datadynamics.hive.udf.geospatial.SDO_ValidateGeometryWithContext';
 *
 * -- 전체 데이터 유효성 검사
 * SELECT id, sdo_validate_geometry_with_context(geom_bytes) AS validation_result
 * FROM spatial_table;
 *
 * -- 결과 예시:
 * -- id | validation_result
 * -- 1  | TRUE
 * -- 2  | FALSE: Self-intersection at (126.97, 37.56)
 * -- 3  | FALSE: Ring Self-intersection at (127.01, 37.49)
 * -- 4  | NULL GEOMETRY
 *
 * -- 유효하지 않은 데이터만 필터링
 * SELECT id, sdo_validate_geometry_with_context(geom_bytes) AS error_detail
 * FROM spatial_table
 * WHERE sdo_validate_geometry_with_context(geom_bytes) != 'TRUE';
 *
 * -- 오류 유형별 집계
 * SELECT
 *   CASE
 *     WHEN validation_result = 'TRUE' THEN 'VALID'
 *     WHEN validation_result LIKE '%Self-intersection%' THEN 'Self-intersection'
 *     WHEN validation_result LIKE '%Ring Self-intersection%' THEN 'Ring Self-intersection'
 *     WHEN validation_result LIKE '%Too Few Points%' THEN 'Too Few Points'
 *     ELSE 'Other Error'
 *   END AS error_type,
 *   COUNT(*) AS count
 * FROM (
 *   SELECT sdo_validate_geometry_with_context(geom_bytes) AS validation_result
 *   FROM spatial_table
 * ) t
 * GROUP BY
 *   CASE
 *     WHEN validation_result = 'TRUE' THEN 'VALID'
 *     WHEN validation_result LIKE '%Self-intersection%' THEN 'Self-intersection'
 *     WHEN validation_result LIKE '%Ring Self-intersection%' THEN 'Ring Self-intersection'
 *     WHEN validation_result LIKE '%Too Few Points%' THEN 'Too Few Points'
 *     ELSE 'Other Error'
 *   END;
 *
 * -- 오류 좌표 추출 (정규표현식 활용)
 * SELECT
 *   id,
 *   regexp_extract(validation_result, 'at \\(([^)]+)\\)', 1) AS error_location
 * FROM (
 *   SELECT id, sdo_validate_geometry_with_context(geom_bytes) AS validation_result
 *   FROM spatial_table
 * ) t
 * WHERE validation_result != 'TRUE';
 * }</pre>
 *
 * <h3>ST_IsValid와의 차이점</h3>
 * <table border="1">
 *   <tr><th>함수</th><th>반환 타입</th><th>오류 정보</th><th>용도</th></tr>
 *   <tr><td>ST_IsValid</td><td>BOOLEAN</td><td>없음</td><td>단순 필터링</td></tr>
 *   <tr><td>SDO_ValidateGeometryWithContext</td><td>STRING</td><td>오류 유형 + 좌표</td><td>상세 분석, 디버깅</td></tr>
 * </table>
 *
 * <h3>관련 함수</h3>
 * <ul>
 *   <li>{@link SDO_RectifyGeometry} - 유효하지 않은 도형 자동 수정</li>
 *   <li>ST_IsValid - 단순 유효성 검사 (BOOLEAN 반환)</li>
 *   <li>ST_IsValidReason (PostGIS) - 유사 기능의 PostGIS 함수</li>
 * </ul>
 *
 * @see UDF
 * @see IsValidOp
 * @see TopologyValidationError
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_GEOM-reference.html">Oracle SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT</a>
 * @see <a href="https://locationtech.github.io/jts/javadoc/org/locationtech/jts/operation/valid/TopologyValidationError.html">JTS TopologyValidationError</a>
 */
public class SDO_ValidateGeometryWithContext extends UDF {

    /**
     * 공간 객체의 기하학적 유효성을 검사하고 상세한 결과 문자열을 반환합니다.
     *
     * <p>OGC Simple Feature 표준에 따른 유효성 규칙을 검사하며,
     * 오류 발생 시 오류 유형과 발생 좌표를 포함한 상세 정보를 제공합니다.</p>
     *
     * <h4>처리 흐름</h4>
     * <ol>
     *   <li>BytesWritable을 JTS Geometry 객체로 변환</li>
     *   <li>NULL 입력 체크 → "NULL GEOMETRY" 반환</li>
     *   <li>IsValidOp으로 유효성 검사 수행</li>
     *   <li>유효한 경우 → "TRUE" 반환</li>
     *   <li>유효하지 않은 경우 → TopologyValidationError에서 오류 정보 추출</li>
     *   <li>"FALSE: [오류 메시지] at [좌표]" 형식으로 반환</li>
     * </ol>
     *
     * @param geomBytes WKB(Well-Known Binary) 형식의 공간 객체 바이트 배열.
     *                  null인 경우 "NULL GEOMETRY" 반환
     * @return 유효성 검사 결과 문자열:
     * <ul>
     *   <li><b>"TRUE"</b>: 도형이 유효함</li>
     *   <li><b>"NULL GEOMETRY"</b>: 입력이 null이거나 파싱 실패</li>
     *   <li><b>"FALSE: [오류] at [좌표]"</b>: 도형이 유효하지 않음
     *       (예: "FALSE: Self-intersection at (10.5, 20.3)")</li>
     * </ul>
     */
    public String evaluate(BytesWritable geomBytes) {
        // 바이트 배열을 JTS Geometry 객체로 변환
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // NULL 입력 처리: 파싱 실패 또는 null 바이트 배열
        if (geom == null) return "NULL GEOMETRY";

        // OGC Simple Feature 표준에 따른 유효성 검사 수행
        // IsValidOp은 다양한 토폴로지 규칙 위반을 검사
        IsValidOp isValidOp = new IsValidOp(geom);

        if (isValidOp.isValid()) {
            // 모든 유효성 규칙을 통과한 경우
            return "TRUE";
        } else {
            // 유효성 검사 실패 시 상세 오류 정보 추출
            // TopologyValidationError는 오류 유형과 발생 위치(좌표)를 포함
            TopologyValidationError err = isValidOp.getValidationError();

            // 형식: "FALSE: [오류 메시지] at [좌표]"
            // 예: "FALSE: Self-intersection at (10.5, 20.3)"
            // err.getMessage(): 오류 유형 설명 (예: "Self-intersection")
            // err.getCoordinate(): 오류 발생 좌표 (X, Y)
            return String.format("FALSE: %s at %s", err.getMessage(), err.getCoordinate());
        }
    }
}