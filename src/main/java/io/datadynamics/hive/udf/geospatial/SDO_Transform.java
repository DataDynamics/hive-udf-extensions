package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;

/**
 * 공간 객체의 좌표 참조 시스템(CRS, Coordinate Reference System)을 다른 좌표계로 변환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_CS.TRANSFORM 함수와 유사한 기능을 제공합니다.
 * GeoTools 라이브러리를 사용하여 EPSG 코드 기반의 좌표 변환을 수행합니다.</p>
 *
 * <h3>좌표 참조 시스템(CRS)이란?</h3>
 * <p>CRS는 지구상의 위치를 숫자 좌표로 표현하는 방법을 정의합니다.
 * 동일한 지점이라도 사용하는 좌표계에 따라 다른 숫자 값으로 표현됩니다.</p>
 *
 * <h3>자주 사용되는 EPSG 코드</h3>
 * <table border="1">
 *   <tr><th>EPSG 코드</th><th>좌표계 이름</th><th>용도</th><th>단위</th></tr>
 *   <tr><td>EPSG:4326</td><td>WGS 84</td><td>GPS, 전 세계 표준</td><td>도(degree)</td></tr>
 *   <tr><td>EPSG:3857</td><td>Web Mercator</td><td>Google Maps, OpenStreetMap</td><td>미터(m)</td></tr>
 *   <tr><td>EPSG:5186</td><td>Korea 2000 / Central Belt 2010</td><td>한국 중부 지역</td><td>미터(m)</td></tr>
 *   <tr><td>EPSG:5179</td><td>Korea 2000 / Unified CS</td><td>한국 통합 좌표계</td><td>미터(m)</td></tr>
 *   <tr><td>EPSG:32652</td><td>WGS 84 / UTM zone 52N</td><td>한국 포함 UTM 구역</td><td>미터(m)</td></tr>
 * </table>
 *
 * <h3>좌표 변환 예시</h3>
 * <pre>
 *   WGS 84 (EPSG:4326)              Web Mercator (EPSG:3857)
 *   경위도 좌표                      평면 직교 좌표
 *
 *   서울시청 좌표 변환 예:
 *   (126.9780, 37.5665)      →      (14135490.8, 4518332.5)
 *        경도      위도                   X (m)      Y (m)
 *
 *   ┌─────────────────┐           ┌─────────────────┐
 *   │  지구 타원체    │    CRS    │   평면 투영     │
 *   │  (곡면 좌표)    │ ────────→ │  (직교 좌표)    │
 *   │  단위: 도(°)    │  Transform│   단위: 미터    │
 *   └─────────────────┘           └─────────────────┘
 * </pre>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_transform AS 'io.datadynamics.hive.udf.geospatial.SDO_Transform';
 *
 * -- WGS84 → Web Mercator 변환 (GPS 좌표를 웹 지도용 좌표로)
 * SELECT sdo_transform(geom_bytes, 'EPSG:4326', 'EPSG:3857') AS web_mercator_geom
 * FROM gps_tracks;
 *
 * -- WGS84 → 한국 중부 좌표계 변환
 * SELECT sdo_transform(geom_bytes, 'EPSG:4326', 'EPSG:5186') AS korea_geom
 * FROM spatial_table;
 *
 * -- Web Mercator → WGS84 역변환 (웹 지도 좌표를 GPS 좌표로)
 * SELECT sdo_transform(geom_bytes, 'EPSG:3857', 'EPSG:4326') AS wgs84_geom
 * FROM web_map_data;
 *
 * -- 거리 계산을 위해 UTM 좌표계로 변환
 * SELECT ST_Length(sdo_transform(road_geom, 'EPSG:4326', 'EPSG:32652')) AS length_meters
 * FROM roads;
 * -- 결과: 미터 단위의 정확한 거리
 *
 * -- 면적 계산을 위해 평면 좌표계로 변환
 * SELECT ST_Area(sdo_transform(parcel_geom, 'EPSG:4326', 'EPSG:5186')) AS area_sqm
 * FROM land_parcels;
 * -- 결과: 제곱미터 단위의 정확한 면적
 * }</pre>
 *
 * <h3>변환이 필요한 경우</h3>
 * <ul>
 *   <li><b>거리/면적 계산</b>: 경위도(도 단위)에서는 정확한 거리/면적 계산 불가</li>
 *   <li><b>웹 지도 표시</b>: GPS 데이터를 Google Maps/Kakao Map에 표시할 때</li>
 *   <li><b>데이터 통합</b>: 서로 다른 좌표계의 데이터를 통합할 때</li>
 *   <li><b>공간 분석</b>: 버퍼, 교차 분석 등 정밀 연산 수행 전</li>
 * </ul>
 *
 * <h3>주의사항</h3>
 * <ul>
 *   <li>잘못된 EPSG 코드를 지정하면 NULL이 반환됩니다</li>
 *   <li>lenient 모드(오차 허용)로 변환하여 약간의 오차가 발생할 수 있습니다</li>
 *   <li>대규모 영역의 변환 시 투영 왜곡이 발생할 수 있습니다</li>
 *   <li>원본 데이터의 좌표계를 정확히 알아야 올바른 변환이 가능합니다</li>
 * </ul>
 *
 * <h3>관련 함수</h3>
 * <ul>
 *   <li>SDO_CS.TRANSFORM (Oracle) - Oracle Spatial 좌표 변환</li>
 *   <li>ST_Transform (PostGIS) - PostGIS 좌표 변환</li>
 *   <li>ST_SetSRID - SRID 설정 (변환 없이 메타데이터만 변경)</li>
 * </ul>
 *
 * @see UDF
 * @see CRS
 * @see MathTransform
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_CS-reference.html">Oracle SDO_CS.TRANSFORM</a>
 * @see <a href="https://epsg.io/">EPSG.io - 좌표계 코드 검색</a>
 * @see <a href="https://docs.geotools.org/latest/userguide/library/referencing/crs.html">GeoTools CRS Tutorial</a>
 */
public class SDO_Transform extends UDF {

    /**
     * 공간 객체의 좌표 참조 시스템을 변환합니다.
     *
     * <p>GeoTools의 CRS 라이브러리를 사용하여 EPSG 코드 기반 좌표 변환을 수행합니다.
     * 변환 후 결과 도형에는 대상 좌표계의 SRID가 설정됩니다.</p>
     *
     * <h4>처리 흐름</h4>
     * <ol>
     *   <li>BytesWritable을 JTS Geometry 객체로 변환</li>
     *   <li>EPSG 코드로부터 소스/대상 CRS 객체 생성</li>
     *   <li>두 CRS 간의 MathTransform 획득</li>
     *   <li>JTS.transform()으로 좌표 변환 수행</li>
     *   <li>결과 도형에 대상 SRID 설정</li>
     *   <li>WKB 바이트 배열로 반환</li>
     * </ol>
     *
     * @param geomBytes     WKB(Well-Known Binary) 형식의 공간 객체 바이트 배열.
     *                      null인 경우 null 반환
     * @param sourceCrsCode 원본 좌표계의 EPSG 코드 문자열.
     *                      형식: "EPSG:코드번호" (예: "EPSG:4326", "EPSG:5186")
     *                      null인 경우 null 반환
     * @param targetCrsCode 변환 대상 좌표계의 EPSG 코드 문자열.
     *                      형식: "EPSG:코드번호" (예: "EPSG:3857", "EPSG:32652")
     *                      null인 경우 null 반환
     * @return 좌표 변환된 공간 객체의 WKB 바이트 배열.
     * <ul>
     *   <li>입력이 null인 경우: null</li>
     *   <li>EPSG 코드가 유효하지 않은 경우: null</li>
     *   <li>변환 실패 시: null</li>
     *   <li>성공 시: 변환된 도형 (SRID가 대상 좌표계로 설정됨)</li>
     * </ul>
     */
    public BytesWritable evaluate(BytesWritable geomBytes, String sourceCrsCode, String targetCrsCode) {
        // 바이트 배열을 JTS Geometry 객체로 변환
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // null 입력 검증: 모든 파라미터가 필수
        if (geom == null || sourceCrsCode == null || targetCrsCode == null) return null;

        try {
            // GeoTools를 이용한 좌표계 디코딩
            // EPSG 코드(예: "EPSG:4326")를 CoordinateReferenceSystem 객체로 변환
            // 내부적으로 EPSG 데이터베이스에서 좌표계 정의를 조회
            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceCrsCode);
            CoordinateReferenceSystem targetCRS = CRS.decode(targetCrsCode);

            // lenient=true: 버사 변환(Bursa-Wolf) 파라미터가 없어도 변환 허용
            // 약간의 정밀도 손실이 있을 수 있으나, 대부분의 경우 실용적으로 충분
            boolean lenient = true;

            // 두 좌표계 간의 수학적 변환 객체 획득
            // 내부적으로 적절한 변환 경로를 자동으로 탐색 (직접 변환 또는 중간 좌표계 경유)
            MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);

            // JTS Geometry 객체의 모든 좌표를 변환
            // 내부적으로 각 좌표점에 MathTransform을 적용
            Geometry transformedGeom = JTS.transform(geom, transform);

            // 변환된 도형에 대상 좌표계의 SRID 설정
            // SRID는 공간 데이터베이스에서 좌표계를 식별하는 정수 코드
            // 예: "EPSG:4326" → SRID 4326
            try {
                String[] split = targetCrsCode.split(":");
                int srid = Integer.parseInt(split[1]);
                transformedGeom.setSRID(srid);
            } catch (Exception ignore) {
                // EPSG 코드 파싱 실패 시 SRID 설정 생략
                // 도형 자체는 정상적으로 변환되었으므로 계속 진행
            }

            // 변환된 도형을 WKB 바이트 배열로 반환
            return GeometryUtils.geometryToBytes(transformedGeom);
        } catch (Exception e) {
            // 변환 실패 원인:
            // 1. 잘못된 EPSG 코드 (존재하지 않는 좌표계)
            // 2. 지원되지 않는 변환 경로 (두 좌표계 간 변환 방법 없음)
            // 3. 좌표값이 대상 좌표계의 유효 범위를 벗어남
            return null;
        }
    }
}