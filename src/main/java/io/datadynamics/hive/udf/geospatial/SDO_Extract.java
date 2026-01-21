package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

/**
 * Hive UDF로서 복합 기하학 객체(GeometryCollection, MultiPolygon 등)에서
 * 특정 요소(Element) 또는 폴리곤의 특정 링(Ring)을 추출하는 함수입니다.
 *
 * <p>이 클래스는 Oracle Spatial의 SDO_UTIL.EXTRACT 함수와 유사한 기능을 제공합니다.
 * Oracle의 인덱스 규칙(1부터 시작)을 따르므로, Oracle Spatial에서 마이그레이션하는
 * 사용자들이 익숙하게 사용할 수 있습니다.</p>
 *
 * <h2>주요 기능</h2>
 * <ul>
 *   <li><b>Element 추출</b>: 복합 Geometry에서 n번째 구성 요소 추출</li>
 *   <li><b>Ring 추출</b>: Polygon에서 외곽선(Exterior Ring) 또는 구멍(Interior Ring/Hole) 추출</li>
 * </ul>
 *
 * <h2>인덱스 규칙 (Oracle 호환)</h2>
 * <table border="1">
 *   <tr><th>인덱스</th><th>의미</th></tr>
 *   <tr><td>elemIndex = 1</td><td>첫 번째 Element (Geometry)</td></tr>
 *   <tr><td>elemIndex = 2</td><td>두 번째 Element</td></tr>
 *   <tr><td>ringIndex = 0 또는 생략</td><td>Element 전체 반환 (Ring 추출 안 함)</td></tr>
 *   <tr><td>ringIndex = 1</td><td>Exterior Ring (외곽선)</td></tr>
 *   <tr><td>ringIndex = 2</td><td>첫 번째 Interior Ring (구멍)</td></tr>
 *   <tr><td>ringIndex = 3</td><td>두 번째 Interior Ring</td></tr>
 * </table>
 *
 * <h2>지원 Geometry 타입</h2>
 * <ul>
 *   <li><b>GeometryCollection</b>: 여러 Geometry의 집합에서 n번째 요소 추출</li>
 *   <li><b>MultiPolygon</b>: 여러 Polygon 중 n번째 Polygon 추출</li>
 *   <li><b>MultiLineString</b>: 여러 LineString 중 n번째 LineString 추출</li>
 *   <li><b>MultiPoint</b>: 여러 Point 중 n번째 Point 추출</li>
 *   <li><b>Polygon</b>: elemIndex=1로 자신을 반환, ringIndex로 링 추출 가능</li>
 *   <li><b>단일 Geometry</b>: elemIndex=1로 자신을 반환</li>
 * </ul>
 *
 * <h2>Hive에서의 사용 예시</h2>
 * <pre>{@code
 * -- UDF 등록
 * CREATE TEMPORARY FUNCTION SDO_Extract AS 'io.datadynamics.hive.udf.geospatial.SDO_Extract';
 *
 * -- MultiPolygon에서 첫 번째 Polygon 추출
 * SELECT SDO_Extract(multi_polygon_column, 1) AS first_polygon
 * FROM spatial_table;
 *
 * -- MultiPolygon에서 두 번째 Polygon 추출
 * SELECT SDO_Extract(multi_polygon_column, 2) AS second_polygon
 * FROM spatial_table;
 *
 * -- Polygon에서 외곽선(Exterior Ring) 추출
 * SELECT SDO_Extract(polygon_column, 1, 1) AS exterior_ring
 * FROM spatial_table;
 *
 * -- Polygon에서 첫 번째 구멍(Interior Ring) 추출
 * SELECT SDO_Extract(polygon_column, 1, 2) AS first_hole
 * FROM spatial_table;
 *
 * -- GeometryCollection에서 세 번째 Element의 외곽선 추출
 * SELECT SDO_Extract(geom_collection, 3, 1) AS third_elem_exterior
 * FROM spatial_table;
 * }</pre>
 *
 * <h2>Ring(링)의 개념</h2>
 * <p>Polygon은 하나의 외곽선(Exterior Ring)과 0개 이상의 구멍(Interior Ring/Hole)으로 구성됩니다.</p>
 * <pre>
 * ┌─────────────────────────────┐
 * │  Exterior Ring (ringIndex=1)│
 * │    ┌─────────┐              │
 * │    │ Hole 1  │              │
 * │    │(rIdx=2) │   ┌─────┐    │
 * │    └─────────┘   │Hole2│    │
 * │                  │(r=3)│    │
 * │                  └─────┘    │
 * └─────────────────────────────┘
 * </pre>
 *
 * <h2>반환값</h2>
 * <ul>
 *   <li>Element 추출 시: 해당 인덱스의 Geometry (Polygon, LineString, Point 등)</li>
 *   <li>Ring 추출 시: LinearRing (선형 링, 닫힌 LineString)</li>
 *   <li>유효하지 않은 인덱스: null</li>
 * </ul>
 *
 * @author Data Dynamics
 * @version 1.0
 * @see org.locationtech.jts.geom.Geometry#getGeometryN(int) Element 추출에 사용
 * @see org.locationtech.jts.geom.Polygon#getExteriorRing() 외곽선 추출에 사용
 * @see org.locationtech.jts.geom.Polygon#getInteriorRingN(int) 구멍 추출에 사용
 */
public class SDO_Extract extends GenericUDF {

    private transient BinaryObjectInspector geomOI;
    private transient IntObjectInspector elemIdxOI;
    private transient IntObjectInspector ringIdxOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentException("SDO_Extract requires 2 or 3 arguments");
        }
        if (!(arguments[0] instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("SDO_Extract requires a binary argument as the first parameter");
        }
        if (!(arguments[1] instanceof IntObjectInspector)) {
            throw new UDFArgumentException("SDO_Extract requires an int argument as the second parameter");
        }
        this.geomOI = (BinaryObjectInspector) arguments[0];
        this.elemIdxOI = (IntObjectInspector) arguments[1];
        if (arguments.length == 3) {
            if (!(arguments[2] instanceof IntObjectInspector)) {
                throw new UDFArgumentException("SDO_Extract requires an int argument as the third parameter");
            }
            this.ringIdxOI = (IntObjectInspector) arguments[2];
        }
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object geomObj = arguments[0].get();
        Object elemIdxObj = arguments[1].get();
        Object ringIdxObj = (arguments.length == 3) ? arguments[2].get() : null;

        BytesWritable geomBytes = geomOI.getPrimitiveWritableObject(geomObj);
        Integer elemIndex = (elemIdxObj == null) ? null : elemIdxOI.get(elemIdxObj);
        Integer ringIndex = (ringIdxObj == null) ? null : ringIdxOI.get(ringIdxObj);

        // 1. 입력 바이트 배열을 JTS Geometry 객체로 역직렬화
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // 2. 필수 파라미터 null 체크
        if (geom == null || elemIndex == null) return null;

        // 3. Oracle 1-based index → Java 0-based index 변환
        //    - Oracle/SQL에서는 인덱스가 1부터 시작
        //    - Java에서는 인덱스가 0부터 시작
        int eIdx = elemIndex - 1;

        // ringIndex가 null이면 -1로 설정하여 Ring 추출을 하지 않음을 표시
        int rIdx = (ringIndex == null) ? -1 : ringIndex - 1;

        // 4. Element 인덱스 범위 검증
        //    - 인덱스가 0보다 작거나 Geometry 개수 이상이면 유효하지 않음
        if (eIdx < 0 || eIdx >= geom.getNumGeometries()) return null;

        // 5. Element 추출
        //    - getGeometryN()으로 복합 Geometry에서 n번째 하위 Geometry 추출
        //    - 단일 Geometry의 경우 getGeometryN(0)은 자기 자신을 반환
        Geometry element = geom.getGeometryN(eIdx);

        // 6. Ring 추출 필요 여부 확인
        //    - rIdx < 0: ringIndex가 0 또는 null → Element 전체 반환
        //    - element가 Polygon이 아닌 경우: Ring이 없으므로 Element 반환
        if (rIdx < 0 || !(element instanceof Polygon)) {
            return GeometryUtils.geometryToBytes(element);
        }

        // 7. Polygon에서 Ring 추출
        Polygon poly = (Polygon) element;

        // Ring Index 매핑:
        //   - ringIndex = 1 (rIdx = 0): Exterior Ring (외곽선)
        //   - ringIndex = 2 (rIdx = 1): 첫 번째 Interior Ring (구멍)
        //   - ringIndex = 3 (rIdx = 2): 두 번째 Interior Ring
        //   - ...
        if (rIdx == 0) {
            // 8a. Exterior Ring (외곽선) 추출
            //     - 모든 Polygon은 정확히 하나의 Exterior Ring을 가짐
            return GeometryUtils.geometryToBytes(poly.getExteriorRing());
        } else {
            // 8b. Interior Ring (구멍) 추출
            //     - rIdx = 1 → innerIdx = 0 (첫 번째 구멍)
            //     - rIdx = 2 → innerIdx = 1 (두 번째 구멍)
            int innerIdx = rIdx - 1;

            // Interior Ring 인덱스 범위 검증
            if (innerIdx >= 0 && innerIdx < poly.getNumInteriorRing()) {
                return GeometryUtils.geometryToBytes(poly.getInteriorRingN(innerIdx));
            }
        }

        // 9. 유효하지 않은 Ring 인덱스인 경우 null 반환
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("SDO_Extract", children);
    }
}
