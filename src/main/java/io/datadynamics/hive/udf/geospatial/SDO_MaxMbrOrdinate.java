package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * 공간 객체의 최소 경계 사각형(MBR, Minimum Bounding Rectangle)에서
 * 지정된 축(X 또는 Y)의 최댓값을 반환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.SDO_MAX_MBR_ORDINATE 함수와 유사한 기능을 제공합니다.
 * 공간 객체를 감싸는 최소 경계 사각형(Envelope)을 계산하고, 해당 사각형의 X 또는 Y 축 최댓값을 반환합니다.</p>
 *
 * <h3>MBR(Minimum Bounding Rectangle)이란?</h3>
 * <p>MBR은 공간 객체를 완전히 포함하는 가장 작은 직사각형입니다.
 * 축에 평행한(axis-aligned) 사각형으로, 공간 인덱싱과 빠른 공간 필터링에 사용됩니다.</p>
 *
 * <pre>
 *     MaxY ┌─────────────┐
 *          │    ****     │
 *          │   *    *    │  ← 공간 객체 (예: POLYGON)
 *          │  *      *   │
 *          │   *    *    │
 *          │    ****     │
 *     MinY └─────────────┘
 *         MinX          MaxX
 * </pre>
 *
 * <h3>좌표축 인덱스 (Oracle 호환)</h3>
 * <table border="1">
 *   <tr><th>ordinatePos</th><th>축</th><th>반환값</th></tr>
 *   <tr><td>1</td><td>X축</td><td>MBR의 최대 X 좌표 (MaxX, 동쪽 경계)</td></tr>
 *   <tr><td>2</td><td>Y축</td><td>MBR의 최대 Y 좌표 (MaxY, 북쪽 경계)</td></tr>
 * </table>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_max_mbr_ordinate AS 'io.datadynamics.hive.udf.geospatial.SDO_MaxMbrOrdinate';
 *
 * -- POLYGON의 MBR에서 최대 X 좌표 조회
 * SELECT sdo_max_mbr_ordinate(geom_bytes, 1) AS max_x
 * FROM spatial_table;
 * -- 결과: 10.0 (MBR의 오른쪽 경계)
 *
 * -- POLYGON의 MBR에서 최대 Y 좌표 조회
 * SELECT sdo_max_mbr_ordinate(geom_bytes, 2) AS max_y
 * FROM spatial_table;
 * -- 결과: 20.0 (MBR의 상단 경계)
 *
 * -- 실제 활용: 특정 영역 내 객체 필터링
 * SELECT *
 * FROM spatial_table
 * WHERE sdo_max_mbr_ordinate(geom_bytes, 1) <= 180.0  -- 경도 180도 이내
 *   AND sdo_max_mbr_ordinate(geom_bytes, 2) <= 90.0;  -- 위도 90도 이내
 * }</pre>
 *
 * <h3>관련 함수</h3>
 * <ul>
 *   <li>{@link SDO_MinMbrOrdinate} - MBR의 최솟값 반환</li>
 * </ul>
 *
 * @see UDF
 * @see Envelope
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_GEOM-reference.html">Oracle SDO_GEOM.SDO_MAX_MBR_ORDINATE</a>
 */
public class SDO_MaxMbrOrdinate extends UDF {

    /**
     * 공간 객체의 MBR에서 지정된 축의 최댓값을 반환합니다.
     *
     * <p>입력된 바이트 배열을 JTS Geometry 객체로 변환한 후,
     * 해당 객체의 Envelope(MBR)을 계산하여 지정된 축의 최댓값을 반환합니다.</p>
     *
     * <h4>처리 흐름</h4>
     * <ol>
     *   <li>BytesWritable을 JTS Geometry 객체로 변환</li>
     *   <li>Geometry에서 Envelope(MBR) 추출</li>
     *   <li>ordinatePos에 따라 MaxX 또는 MaxY 반환</li>
     * </ol>
     *
     * @param geomBytes  WKB(Well-Known Binary) 형식의 공간 객체 바이트 배열.
     *                   null인 경우 null 반환
     * @param ordinatePos 좌표축 인덱스. Oracle Spatial 호환 인덱스 사용
     *                    <ul>
     *                      <li>1: X축 (경도 방향) - MaxX 반환</li>
     *                      <li>2: Y축 (위도 방향) - MaxY 반환</li>
     *                    </ul>
     *                    null이거나 1, 2가 아닌 경우 null 반환
     * @return 지정된 축의 최댓값 (Double).
     *         입력이 null이거나 유효하지 않은 ordinatePos인 경우 null 반환
     */
    public Double evaluate(BytesWritable geomBytes, Integer ordinatePos) {
        // 바이트 배열을 JTS Geometry 객체로 변환
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // null 체크: 공간 객체 또는 좌표축 인덱스가 null이면 처리 불가
        if (geom == null || ordinatePos == null) return null;

        // Geometry의 Envelope(최소 경계 사각형) 추출
        // Envelope는 MinX, MaxX, MinY, MaxY 값을 포함
        Envelope env = geom.getEnvelopeInternal();

        // Oracle Spatial 호환 좌표축 인덱스 처리
        // Oracle에서는 1=X축, 2=Y축, 3=Z축 (0-based가 아님에 주의)
        if (ordinatePos == 1) return env.getMaxX();  // X축 최댓값 (동쪽 경계)
        if (ordinatePos == 2) return env.getMaxY();  // Y축 최댓값 (북쪽 경계)

        // Z축(3) 지원은 현재 미구현
        // JTS Envelope는 2D만 지원하므로, Z축 처리를 위해서는
        // CoordinateSequence를 순회하며 Z값을 직접 비교해야 함
        return null;
    }
}