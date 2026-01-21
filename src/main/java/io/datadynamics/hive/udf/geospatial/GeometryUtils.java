package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/**
 * 공간 데이터 형식 간의 변환을 담당하는 핵심 유틸리티 클래스입니다.
 *
 * <p>이 클래스는 Hive/Impala의 BINARY 타입(WKB)과 JTS Geometry 객체,
 * 그리고 WKT 문자열 간의 상호 변환 기능을 제공합니다.
 * 모든 공간 UDF 클래스들이 이 유틸리티를 통해 데이터 형식 변환을 수행합니다.</p>
 *
 * <h3>공간 데이터 형식</h3>
 * <table border="1">
 *   <tr><th>형식</th><th>약어</th><th>설명</th><th>용도</th></tr>
 *   <tr><td>Well-Known Binary</td><td>WKB</td><td>이진 바이트 배열</td><td>DB 저장, 네트워크 전송</td></tr>
 *   <tr><td>Well-Known Text</td><td>WKT</td><td>텍스트 문자열</td><td>가독성, 디버깅, 쿼리</td></tr>
 *   <tr><td>JTS Geometry</td><td>-</td><td>Java 객체</td><td>공간 연산 수행</td></tr>
 * </table>
 *
 * <h3>데이터 변환 흐름</h3>
 * <pre>
 *   Hive/Impala                      Java UDF                       결과
 *
 *   ┌─────────────┐                ┌─────────────┐               ┌─────────────┐
 *   │ BINARY(WKB) │ ─────────────→ │ JTS Geometry│ ────────────→ │ BINARY(WKB) │
 *   │ BytesWritable│  bytesToGeometry()  │  (공간 연산)  │  geometryToBytes()  │ BytesWritable│
 *   └─────────────┘                └─────────────┘               └─────────────┘
 *
 *   ┌─────────────┐                ┌─────────────┐               ┌─────────────┐
 *   │ STRING(WKT) │ ─────────────→ │ JTS Geometry│ ────────────→ │ STRING(WKT) │
 *   │   String    │  stringToGeometry()  │  (공간 연산)  │  geometryToString()  │   String    │
 *   └─────────────┘                └─────────────┘               └─────────────┘
 * </pre>
 *
 * <h3>WKB (Well-Known Binary) 형식</h3>
 * <p>WKB는 OGC Simple Feature 표준에 정의된 이진 형식입니다:</p>
 * <pre>
 *   WKB 구조:
 *   ┌──────────────┬─────────────┬─────────────────────────────┐
 *   │ Byte Order   │ Geometry    │ Coordinates                 │
 *   │ (1 byte)     │ Type        │ (variable)                  │
 *   │ 00=Big       │ (4 bytes)   │ (8 bytes per ordinate)      │
 *   │ 01=Little    │             │                             │
 *   └──────────────┴─────────────┴─────────────────────────────┘
 *
 *   Geometry Type 값:
 *   1 = Point, 2 = LineString, 3 = Polygon,
 *   4 = MultiPoint, 5 = MultiLineString, 6 = MultiPolygon,
 *   7 = GeometryCollection
 * </pre>
 *
 * <h3>WKT (Well-Known Text) 형식</h3>
 * <p>WKT는 사람이 읽을 수 있는 텍스트 형식입니다:</p>
 * <pre>
 *   예시:
 *   - POINT (30 10)
 *   - LINESTRING (30 10, 10 30, 40 40)
 *   - POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
 *   - POLYGON Z ((30 10 5, 40 40 10, 20 40 15, 10 20 20, 30 10 5))
 *   - MULTIPOINT ((10 40), (40 30), (20 20), (30 10))
 * </pre>
 *
 * <h3>스레드 안전성</h3>
 * <p>WKBReader, WKBWriter, WKTReader, WKTWriter는 스레드 안전하므로
 * static final로 선언하여 재사용합니다. 이를 통해 객체 생성 오버헤드를 줄이고
 * 대량 데이터 처리 시 성능을 향상시킵니다.</p>
 *
 * <h3>3D 좌표 지원</h3>
 * <p>WKBWriter와 WKTWriter는 3차원(XYZ) 좌표를 지원하도록 설정되어 있습니다.
 * 이를 통해 고도(Z) 정보가 포함된 공간 데이터를 손실 없이 처리할 수 있습니다.
 * Oracle LRS(Linear Referencing System) 데이터의 M(Measure) 값 처리도 고려되었습니다.</p>
 *
 * <h3>오류 처리 전략</h3>
 * <p>잘못된 형식의 입력 데이터에 대해 예외를 발생시키지 않고 null을 반환합니다.
 * 이는 Hive 쿼리 중 일부 레코드의 오류로 인해 전체 쿼리가 실패하는 것을 방지합니다.</p>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * // WKB -> Geometry 변환 (UDF 내부에서)
 * public BytesWritable evaluate(BytesWritable geomBytes) {
 *     Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);
 *     if (geom == null) return null;
 *
 *     // 공간 연산 수행
 *     Geometry result = geom.buffer(10);
 *
 *     return GeometryUtils.geometryToBytes(result);
 * }
 *
 * // WKT -> Geometry 변환 (String 기반 UDF에서)
 * public String evaluate(String wkt) {
 *     Geometry geom = GeometryUtils.stringToGeometry(wkt);
 *     if (geom == null) return null;
 *
 *     // 공간 연산 수행
 *     Geometry result = geom.convexHull();
 *
 *     return GeometryUtils.geometryToString(result);
 * }
 * }</pre>
 *
 * <h3>관련 클래스</h3>
 * <ul>
 *   <li>{@link WKBReader} - WKB 바이트 배열을 Geometry로 파싱</li>
 *   <li>{@link WKBWriter} - Geometry를 WKB 바이트 배열로 직렬화</li>
 *   <li>{@link WKTReader} - WKT 문자열을 Geometry로 파싱</li>
 *   <li>{@link WKTWriter} - Geometry를 WKT 문자열로 직렬화</li>
 *   <li>{@link BytesWritable} - Hadoop/Hive의 바이너리 데이터 래퍼</li>
 * </ul>
 *
 * @see Geometry
 * @see BytesWritable
 * @see <a href="https://www.ogc.org/standards/sfa">OGC Simple Feature Access</a>
 * @see <a href="https://locationtech.github.io/jts/javadoc/org/locationtech/jts/io/package-summary.html">JTS I/O Package</a>
 */
public class GeometryUtils {

    /**
     * WKB(Well-Known Binary) 바이트 배열을 JTS Geometry 객체로 변환하는 Reader.
     *
     * <p>2D(XY) 및 3D(XYZ) 좌표를 모두 지원하며, 바이트 순서(Big-Endian/Little-Endian)를
     * 자동으로 감지합니다. 스레드 안전하므로 static으로 재사용합니다.</p>
     */
    private static final WKBReader wkbReader = new WKBReader();

    /**
     * JTS Geometry 객체를 WKB(Well-Known Binary) 바이트 배열로 변환하는 Writer.
     *
     * <p>3차원(XYZ) 좌표 출력을 지원하도록 설정되어 있습니다:</p>
     * <ul>
     *   <li>첫 번째 파라미터(3): 출력 차원 (2=XY, 3=XYZ)</li>
     *   <li>두 번째 파라미터(true): SRID 포함 여부</li>
     * </ul>
     *
     * <p>Oracle LRS 데이터의 Measure(M) 값 처리를 위해 3차원 설정이 필요합니다.
     * 스레드 안전하므로 static으로 재사용합니다.</p>
     */
    private static final WKBWriter wkbWriter = new WKBWriter(3, true);

    /**
     * WKT(Well-Known Text) 문자열을 JTS Geometry 객체로 변환하는 Reader.
     *
     * <p>표준 WKT 형식(POINT, LINESTRING, POLYGON 등)과
     * 확장 WKT 형식(POINT Z, POLYGON ZM 등)을 모두 지원합니다.
     * 스레드 안전하므로 static으로 재사용합니다.</p>
     */
    private static final WKTReader wktReader = new WKTReader();

    /**
     * JTS Geometry 객체를 WKT(Well-Known Text) 문자열로 변환하는 Writer.
     *
     * <p>3차원(XYZ) 좌표 출력을 지원하도록 설정되어 있습니다.
     * Z 좌표가 있는 도형은 "POINT Z (10 20 30)" 형식으로 출력됩니다.
     * 스레드 안전하므로 static으로 재사용합니다.</p>
     */
    private static final WKTWriter wktWriter = new WKTWriter(3);

    /**
     * Hive/Impala의 BINARY 타입(BytesWritable)을 JTS Geometry 객체로 변환합니다.
     *
     * <p>입력된 WKB(Well-Known Binary) 바이트 배열을 파싱하여
     * JTS Geometry 객체를 생성합니다. 이 메서드는 모든 WKB 기반 UDF의
     * 입력 변환에 사용됩니다.</p>
     *
     * <h4>처리 흐름</h4>
     * <ol>
     *   <li>null 및 빈 입력 체크</li>
     *   <li>BytesWritable에서 바이트 배열 안전하게 복사</li>
     *   <li>WKBReader로 바이트 배열 파싱</li>
     *   <li>JTS Geometry 객체 반환</li>
     * </ol>
     *
     * <h4>copyBytes() 사용 이유</h4>
     * <p>BytesWritable.getBytes()는 내부 버퍼의 참조를 반환하며,
     * 실제 데이터 길이보다 큰 배열일 수 있습니다.
     * copyBytes()는 정확한 길이의 새 배열을 반환하므로 안전합니다.</p>
     *
     * @param wkb Hive/Impala BINARY 타입의 WKB 데이터.
     *            null이거나 길이가 0인 경우 null 반환
     * @return 파싱된 JTS Geometry 객체.
     *         <ul>
     *           <li>입력이 null이거나 비어있는 경우: null</li>
     *           <li>WKB 파싱 실패(잘못된 형식): null</li>
     *           <li>성공: POINT, LINESTRING, POLYGON 등의 Geometry 객체</li>
     *         </ul>
     */
    public static Geometry bytesToGeometry(BytesWritable wkb) {
        // null 또는 빈 입력 처리
        if (wkb == null || wkb.getLength() == 0) return null;

        try {
            // copyBytes()를 사용하여 안전하게 바이트 배열 추출
            // getBytes()는 내부 버퍼 참조를 반환하므로 길이 불일치 가능
            // copyBytes()는 정확한 길이의 새 배열을 생성
            return wkbReader.read(wkb.copyBytes());
        } catch (Exception e) {
            // WKB 파싱 실패 시 null 반환
            // - 잘못된 WKB 형식
            // - 손상된 바이트 데이터
            // - 지원하지 않는 Geometry 타입
            // 예외를 전파하지 않아 전체 쿼리 실패를 방지
            return null;
        }
    }

    /**
     * JTS Geometry 객체를 Hive/Impala의 BINARY 타입(BytesWritable)으로 변환합니다.
     *
     * <p>JTS Geometry를 WKB(Well-Known Binary) 형식으로 직렬화하여
     * BytesWritable로 래핑합니다. 이 메서드는 모든 WKB 기반 UDF의
     * 출력 변환에 사용됩니다.</p>
     *
     * <h4>WKB 출력 설정</h4>
     * <ul>
     *   <li>3차원(XYZ) 좌표 지원</li>
     *   <li>SRID(Spatial Reference ID) 포함</li>
     *   <li>네이티브 바이트 순서 사용</li>
     * </ul>
     *
     * @param geom 변환할 JTS Geometry 객체.
     *             null인 경우 null 반환
     * @return WKB 바이트를 포함하는 BytesWritable.
     *         <ul>
     *           <li>입력이 null인 경우: null</li>
     *           <li>성공: WKB 형식의 바이트를 포함하는 BytesWritable</li>
     *         </ul>
     */
    public static BytesWritable geometryToBytes(Geometry geom) {
        // null 입력 처리
        if (geom == null) return null;

        // Geometry를 WKB 바이트 배열로 직렬화
        // 3차원 좌표와 SRID가 포함됨
        byte[] bytes = wkbWriter.write(geom);

        // BytesWritable로 래핑하여 반환
        // Hive/Impala의 BINARY 타입과 호환
        return new BytesWritable(bytes);
    }

    /**
     * WKT(Well-Known Text) 문자열을 JTS Geometry 객체로 변환합니다.
     *
     * <p>입력된 WKT 문자열을 파싱하여 JTS Geometry 객체를 생성합니다.
     * 이 메서드는 모든 WKT/String 기반 UDF의 입력 변환에 사용됩니다.</p>
     *
     * <h4>지원하는 WKT 형식</h4>
     * <ul>
     *   <li>POINT (x y) / POINT Z (x y z)</li>
     *   <li>LINESTRING (x1 y1, x2 y2, ...)</li>
     *   <li>POLYGON ((x1 y1, x2 y2, ..., x1 y1))</li>
     *   <li>MULTIPOINT, MULTILINESTRING, MULTIPOLYGON</li>
     *   <li>GEOMETRYCOLLECTION</li>
     * </ul>
     *
     * @param wkt WKT(Well-Known Text) 형식의 공간 객체 문자열.
     *            null이거나 비어있는 경우 null 반환
     * @return 파싱된 JTS Geometry 객체.
     *         <ul>
     *           <li>입력이 null이거나 비어있는 경우: null</li>
     *           <li>WKT 파싱 실패(잘못된 형식): null</li>
     *           <li>성공: POINT, LINESTRING, POLYGON 등의 Geometry 객체</li>
     *         </ul>
     */
    public static Geometry stringToGeometry(String wkt) {
        // null 또는 빈 문자열 처리
        if (wkt == null || wkt.isEmpty()) return null;

        try {
            // WKT 문자열을 파싱하여 Geometry 객체 생성
            return wktReader.read(wkt);
        } catch (Exception e) {
            // WKT 파싱 실패 시 null 반환
            // - 잘못된 WKT 문법
            // - 지원하지 않는 Geometry 타입
            // - 좌표 형식 오류
            // 예외를 전파하지 않아 전체 쿼리 실패를 방지
            return null;
        }
    }

    /**
     * JTS Geometry 객체를 WKT(Well-Known Text) 문자열로 변환합니다.
     *
     * <p>JTS Geometry를 사람이 읽을 수 있는 WKT 형식으로 직렬화합니다.
     * 이 메서드는 모든 WKT/String 기반 UDF의 출력 변환에 사용됩니다.</p>
     *
     * <h4>출력 형식 예시</h4>
     * <pre>
     *   2D: POINT (30 10)
     *   3D: POINT Z (30 10 5)
     *   폴리곤: POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))
     * </pre>
     *
     * @param geom 변환할 JTS Geometry 객체.
     *             null인 경우 null 반환
     * @return WKT 형식의 문자열.
     *         <ul>
     *           <li>입력이 null인 경우: null</li>
     *           <li>성공: "POINT (30 10)" 등의 WKT 문자열</li>
     *         </ul>
     */
    public static String geometryToString(Geometry geom) {
        // null 입력 처리
        if (geom == null) return null;

        // Geometry를 WKT 문자열로 직렬화
        // 3차원 좌표가 있는 경우 "POINT Z (x y z)" 형식으로 출력
        return wktWriter.write(geom);
    }
}