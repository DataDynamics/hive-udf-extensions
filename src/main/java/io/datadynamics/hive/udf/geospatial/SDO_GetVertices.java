package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * WKT(Well-Known Text) 형식의 공간 객체에서 모든 정점(Vertex)을 추출하여
 * 테이블 형태의 행(Row)으로 반환하는 Hive UDTF(User-Defined Table-Generating Function)입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_UTIL.GETVERTICES 함수와 유사한 기능을 제공합니다.
 * 입력으로 받은 WKT 문자열을 파싱하여 각 좌표점을 개별 행으로 출력합니다.</p>
 *
 * <h3>지원하는 WKT 형식</h3>
 * <ul>
 *   <li>POINT (x y)</li>
 *   <li>LINESTRING (x1 y1, x2 y2, ...)</li>
 *   <li>POLYGON ((x1 y1, x2 y2, x3 y3, x1 y1))</li>
 *   <li>기타 WKT 기반 공간 객체</li>
 * </ul>
 *
 * <h3>출력 스키마</h3>
 * <table border="1">
 *   <tr><th>컬럼명</th><th>타입</th><th>설명</th></tr>
 *   <tr><td>id</td><td>INT</td><td>정점의 순번 (1부터 시작)</td></tr>
 *   <tr><td>x</td><td>STRING</td><td>정점의 X 좌표 (경도)</td></tr>
 *   <tr><td>y</td><td>STRING</td><td>정점의 Y 좌표 (위도)</td></tr>
 * </table>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_getvertices AS 'io.datadynamics.hive.udf.geospatial.SDO_GetVertices';
 *
 * -- LINESTRING에서 정점 추출
 * SELECT v.id, v.x, v.y
 * FROM (SELECT 'LINESTRING (0 0, 10 10, 20 25)' AS geom) t
 * LATERAL VIEW sdo_getvertices(t.geom) v AS id, x, y;
 *
 * -- 결과:
 * -- id | x  | y
 * -- 1  | 0  | 0
 * -- 2  | 10 | 10
 * -- 3  | 20 | 25
 *
 * -- POLYGON에서 정점 추출
 * SELECT v.id, v.x, v.y
 * FROM (SELECT 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))' AS geom) t
 * LATERAL VIEW sdo_getvertices(t.geom) v AS id, x, y;
 *
 * -- 결과:
 * -- id | x  | y
 * -- 1  | 0  | 0
 * -- 2  | 10 | 0
 * -- 3  | 10 | 10
 * -- 4  | 0  | 10
 * -- 5  | 0  | 0
 * }</pre>
 *
 * @see GenericUDTF
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_UTIL-reference.html">Oracle SDO_UTIL.GETVERTICES</a>
 */
public class SDO_GetVertices extends GenericUDTF {

    /**
     * UDTF의 출력 스키마를 초기화하고 입력 인자의 유효성을 검증합니다.
     *
     * <p>이 메서드는 Hive 쿼리 컴파일 시점에 한 번 호출되며,
     * 출력될 컬럼의 이름과 타입을 정의합니다.</p>
     *
     * @param args 입력 인자의 ObjectInspector 배열. 정확히 1개의 인자(WKT 문자열)를 기대합니다.
     * @return 출력 스키마를 정의하는 StructObjectInspector (id: INT, x: STRING, y: STRING)
     * @throws UDFArgumentException 인자의 개수가 1개가 아닌 경우 발생
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        // 입력 인자 개수 검증: 정확히 1개의 WKT 문자열만 허용
        if (args.length != 1) {
            throw new UDFArgumentException("SDO_GetVertices() takes exactly one argument.");
        }

        // 출력 컬럼 이름 정의
        List<String> fieldNames = new ArrayList<>();
        // 출력 컬럼 타입 정의를 위한 ObjectInspector 목록
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        // 첫 번째 출력 컬럼: id (정점 순번, 1부터 시작하는 정수)
        fieldNames.add("id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        // 두 번째 출력 컬럼: x (X 좌표, 문자열로 반환하여 정밀도 손실 방지)
        fieldNames.add("x");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // 세 번째 출력 컬럼: y (Y 좌표, 문자열로 반환하여 정밀도 손실 방지)
        fieldNames.add("y");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // 정의된 필드명과 타입으로 구조체 ObjectInspector 생성 및 반환
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 입력된 WKT 문자열을 파싱하여 각 정점을 개별 행으로 출력합니다.
     *
     * <p>이 메서드는 입력 테이블의 각 행에 대해 호출됩니다.
     * WKT 문자열에서 알파벳과 괄호를 제거하고, 콤마로 구분된 좌표쌍을 추출하여
     * 각각의 정점을 forward() 메서드를 통해 출력합니다.</p>
     *
     * <h4>파싱 로직</h4>
     * <ol>
     *   <li>WKT에서 알파벳(POINT, LINESTRING 등)과 괄호 제거</li>
     *   <li>콤마(,)를 기준으로 좌표쌍 분리</li>
     *   <li>각 좌표쌍에서 공백을 기준으로 X, Y 좌표 분리</li>
     *   <li>순번(id), X 좌표, Y 좌표를 행으로 출력</li>
     * </ol>
     *
     * @param args 입력 인자 배열. args[0]은 WKT 형식의 공간 객체 문자열
     * @throws HiveException 처리 중 오류 발생 시
     */
    @Override
    public void process(Object[] args) throws HiveException {
        // null 입력은 무시 (결과 행을 생성하지 않음)
        if (args[0] == null) return;

        // 입력값을 문자열로 변환
        String wkt = args[0].toString();

        // WKT 파싱: 알파벳(a-zA-Z)과 괄호((), ))를 제거하여 좌표 데이터만 추출
        // 예: "LINESTRING (0 0, 10 10)" -> "0 0, 10 10"
        String cleanWkt = wkt.replaceAll("[a-zA-Z\\(\\)]", "").trim();

        // 콤마를 기준으로 각 좌표쌍 분리
        // 예: "0 0, 10 10" -> ["0 0", "10 10"]
        String[] points = cleanWkt.split(",");

        // 각 좌표쌍을 순회하며 행 생성
        for (int i = 0; i < points.length; i++) {
            // 공백을 기준으로 X, Y 좌표 분리 (연속된 공백도 처리)
            // 예: "0 0" -> ["0", "0"]
            String[] coords = points[i].trim().split("\\s+");

            // 최소 X, Y 두 개의 좌표가 있는 경우에만 처리
            if (coords.length >= 2) {
                // 출력 행 생성: [id, x, y]
                Object[] row = new Object[3];
                row[0] = i + 1;         // ID: 1부터 시작하는 순번
                row[1] = coords[0];     // X: 첫 번째 좌표값 (경도)
                row[2] = coords[1];     // Y: 두 번째 좌표값 (위도)

                // 생성된 행을 출력 (Hive에게 전달)
                forward(row);
            }
        }
    }

    /**
     * UDTF 처리가 완료된 후 호출되는 정리 메서드입니다.
     *
     * <p>이 UDTF는 상태를 유지하지 않으므로 별도의 정리 작업이 필요하지 않습니다.
     * 만약 버퍼링된 데이터가 있거나 리소스 해제가 필요한 경우 이 메서드에서 처리합니다.</p>
     *
     * @throws HiveException 정리 중 오류 발생 시
     */
    @Override
    public void close() throws HiveException {
        // 이 UDTF는 상태를 유지하지 않으므로 종료 시 별도 처리 불필요
    }
}