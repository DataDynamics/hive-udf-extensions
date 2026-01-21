package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.operation.distance.DistanceOp;

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
 * @see org.locationtech.jts.operation.distance.DistanceOp JTS 거리 연산 클래스
 * @see org.locationtech.jts.operation.distance.DistanceOp#nearestPoints(Geometry, Geometry) 최단 거리 점 계산
 * @see GeometryFactory LineString 생성에 사용
 */
public class SDO_ClosestPoints extends GenericUDF {

    private final GeometryFactory factory = new GeometryFactory();
    private transient BinaryObjectInspector geom1OI;
    private transient BinaryObjectInspector geom2OI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("SDO_ClosestPoints requires 2 arguments");
        }
        if (!(arguments[0] instanceof BinaryObjectInspector) || !(arguments[1] instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("SDO_ClosestPoints requires 2 binary arguments");
        }
        this.geom1OI = (BinaryObjectInspector) arguments[0];
        this.geom2OI = (BinaryObjectInspector) arguments[1];
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object g1Obj = arguments[0].get();
        Object g2Obj = arguments[1].get();

        BytesWritable geom1Bytes = geom1OI.getPrimitiveWritableObject(g1Obj);
        BytesWritable geom2Bytes = geom2OI.getPrimitiveWritableObject(g2Obj);

        // 1. 입력 바이트 배열을 JTS Geometry 객체로 역직렬화
        Geometry g1 = GeometryUtils.bytesToGeometry(geom1Bytes);
        Geometry g2 = GeometryUtils.bytesToGeometry(geom2Bytes);

        // 2. null 체크: 두 Geometry 중 하나라도 null이면 null 반환
        if (g1 == null || g2 == null) return null;

        // 3. 최단 거리 점 쌍 계산
        //    - DistanceOp.nearestPoints()는 두 Geometry 간 최단 거리를 형성하는 점 쌍을 반환
        //    - nearestCoords[0]: g1 위의 가장 가까운 점
        //    - nearestCoords[1]: g2 위의 가장 가까운 점
        Coordinate[] nearestCoords = DistanceOp.nearestPoints(g1, g2);

        // 4. 결과 유효성 확인 및 LineString 생성
        //    - 정상적인 경우 항상 2개의 좌표가 반환됨
        //    - 두 점을 잇는 LineString을 생성하여 시각화 및 거리 측정이 용이하도록 함
        if (nearestCoords != null && nearestCoords.length >= 2) {
            // 5. 두 점을 연결하는 LineString 생성
            //    - 이 LineString의 길이가 곧 두 Geometry 간의 최단 거리
            Geometry result = factory.createLineString(nearestCoords);

            // 6. 결과 Geometry를 바이트 배열로 직렬화하여 반환
            return GeometryUtils.geometryToBytes(result);
        }

        // 7. 계산 실패 시 null 반환
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("SDO_ClosestPoints", children);
    }

}
