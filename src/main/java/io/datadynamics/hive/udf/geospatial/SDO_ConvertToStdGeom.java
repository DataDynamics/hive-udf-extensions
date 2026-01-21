package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;

/**
 * 3차원(XYZ) 또는 4차원(XYZM) 좌표를 가진 공간 객체에서 M(Measure) 값을 제거하여
 * 표준 2D(XY) 또는 3D(XYZ) 형식으로 변환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_UTIL.CONVERT_GEOMETRY 함수와 유사한 기능을 제공합니다.
 * LRS(Linear Referencing System)에서 사용되는 M(Measure) 차원을 제거하여
 * 표준 공간 함수와의 호환성을 확보합니다.</p>
 *
 * <h3>좌표 차원 유형</h3>
 * <table border="1">
 *   <tr><th>차원</th><th>좌표 구성</th><th>용도</th><th>예시</th></tr>
 *   <tr><td>2D</td><td>X, Y</td><td>평면 좌표 (일반적)</td><td>(126.97, 37.56)</td></tr>
 *   <tr><td>3D (XYZ)</td><td>X, Y, Z</td><td>고도/높이 포함</td><td>(126.97, 37.56, 50.0)</td></tr>
 *   <tr><td>3D (XYM)</td><td>X, Y, M</td><td>선형 참조 (거리)</td><td>(126.97, 37.56, 1500.0)</td></tr>
 *   <tr><td>4D (XYZM)</td><td>X, Y, Z, M</td><td>고도 + 선형 참조</td><td>(126.97, 37.56, 50.0, 1500.0)</td></tr>
 * </table>
 *
 * <h3>M(Measure) 값이란?</h3>
 * <p>M 값은 선형 참조 시스템(LRS)에서 사용되는 측정값으로,
 * 주로 선형 객체(도로, 철도 등)의 시작점으로부터의 거리를 나타냅니다.</p>
 *
 * <pre>
 *   도로 선형 객체 (LINESTRING ZM)
 *
 *   ●────────────●────────────●────────────●
 *   │            │            │            │
 *   (X1,Y1,Z1,M1) (X2,Y2,Z2,M2) (X3,Y3,Z3,M3) (X4,Y4,Z4,M4)
 *   │            │            │            │
 *   M=0km      M=5km       M=10km       M=15km
 *
 *   변환 후 (LINESTRING Z):
 *   ●────────────●────────────●────────────●
 *   │            │            │            │
 *   (X1,Y1,Z1)   (X2,Y2,Z2)   (X3,Y3,Z3)   (X4,Y4,Z4)
 *   M 값이 제거됨
 * </pre>
 *
 * <h3>변환 규칙</h3>
 * <table border="1">
 *   <tr><th>입력 차원</th><th>출력 차원</th><th>제거되는 값</th></tr>
 *   <tr><td>XYZM (4D)</td><td>XYZ (3D)</td><td>M (Measure)</td></tr>
 *   <tr><td>XYM (3D)</td><td>XY (2D)</td><td>M (Measure)</td></tr>
 *   <tr><td>XYZ (3D)</td><td>XYZ (3D)</td><td>없음 (그대로 유지)</td></tr>
 *   <tr><td>XY (2D)</td><td>XY (2D)</td><td>없음 (그대로 유지)</td></tr>
 * </table>
 *
 * <h3>사용 시나리오</h3>
 * <ul>
 *   <li><b>공간 함수 호환성</b>: M 값을 지원하지 않는 함수 사용 전 전처리</li>
 *   <li><b>데이터 교환</b>: M 차원을 지원하지 않는 시스템으로 내보내기</li>
 *   <li><b>시각화</b>: 지도 렌더링 시 표준 좌표만 필요한 경우</li>
 *   <li><b>저장 공간 최적화</b>: 불필요한 M 값 제거로 데이터 크기 절감</li>
 * </ul>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_convert_to_std_geom
 *   AS 'io.datadynamics.hive.udf.geospatial.SDO_ConvertToStdGeom';
 *
 * -- XYZM 좌표를 XYZ로 변환 (M 값 제거)
 * SELECT sdo_convert_to_std_geom(lrs_road_geom) AS std_road_geom
 * FROM lrs_roads;
 *
 * -- 변환 후 표준 공간 함수 사용
 * SELECT
 *   road_id,
 *   ST_Length(sdo_convert_to_std_geom(lrs_road_geom)) AS length_2d
 * FROM lrs_roads;
 *
 * -- 시각화용 데이터 추출 (M 값 불필요)
 * SELECT
 *   name,
 *   ST_AsText(sdo_convert_to_std_geom(route_geom)) AS wkt_for_display
 * FROM hiking_trails;
 *
 * -- 데이터 마이그레이션: LRS 데이터를 표준 형식으로 변환
 * INSERT INTO standard_roads
 * SELECT
 *   road_id,
 *   road_name,
 *   sdo_convert_to_std_geom(lrs_geom) AS geom
 * FROM lrs_roads_source;
 * }</pre>
 *
 * <h3>현재 구현 상태</h3>
 * <p><b>참고:</b> 현재 이 함수는 플레이스홀더 구현으로, 입력을 그대로 반환합니다.
 * 완전한 구현을 위해서는 JTS의 {@code CoordinateFilter} 또는
 * {@code CoordinateSequenceFilter}를 사용하여 M 차원을 제거해야 합니다.</p>
 *
 * <h4>완전한 구현을 위한 접근 방식</h4>
 * <ol>
 *   <li>{@code CoordinateSequenceFilter} 구현하여 각 좌표의 M 값을 NaN으로 설정</li>
 *   <li>새로운 {@code CoordinateSequence}를 생성하여 XY 또는 XYZ만 복사</li>
 *   <li>변환된 좌표로 새로운 Geometry 객체 생성</li>
 * </ol>
 *
 * <h3>관련 함수</h3>
 * <ul>
 *   <li>SDO_UTIL.CONVERT_GEOMETRY (Oracle) - 도형 형식 변환</li>
 *   <li>ST_Force2D (PostGIS) - 2D로 강제 변환</li>
 *   <li>ST_Force3D (PostGIS) - 3D로 강제 변환</li>
 *   <li>ST_RemoveRepeatedPoints - 중복 좌표 제거</li>
 * </ul>
 *
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_UTIL-reference.html">Oracle SDO_UTIL.CONVERT_GEOMETRY</a>
 * @see <a href="https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/CoordinateSequenceFilter.html">JTS CoordinateSequenceFilter</a>
 */
public class SDO_ConvertToStdGeom extends GenericUDF {

    private transient BinaryObjectInspector geomOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("SDO_ConvertToStdGeom requires 1 argument");
        }
        if (!(arguments[0] instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("SDO_ConvertToStdGeom requires a binary argument");
        }
        this.geomOI = (BinaryObjectInspector) arguments[0];
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object geomObj = arguments[0].get();
        BytesWritable geomBytes = geomOI.getPrimitiveWritableObject(geomObj);

        // 바이트 배열을 JTS Geometry 객체로 변환
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // null 입력 처리
        if (geom == null) return null;

        // 플레이스홀더: 입력을 그대로 반환
        return geomBytes;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("SDO_ConvertToStdGeom", children);
    }
}