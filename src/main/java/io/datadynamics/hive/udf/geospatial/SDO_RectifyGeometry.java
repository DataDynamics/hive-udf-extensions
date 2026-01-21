package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.valid.IsValidOp;

/**
 * 자가 교차(Self-intersection), 꼬인 폴리곤, 중복 정점 등 기하학적으로 유효하지 않은
 * 공간 객체를 수정하여 유효한(Valid) 형태로 변환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_UTIL.RECTIFY_GEOMETRY 함수와 유사한 기능을 제공합니다.
 * OGC(Open Geospatial Consortium) Simple Feature 표준에 따른 유효성 규칙을 위반하는
 * 공간 객체를 자동으로 복구합니다.</p>
 *
 * <h3>수정 가능한 기하학적 오류 유형</h3>
 * <ul>
 *   <li><b>자가 교차(Self-intersection)</b>: 폴리곤 경계가 자기 자신과 교차하는 경우</li>
 *   <li><b>꼬인 폴리곤(Twisted Polygon)</b>: 정점 순서가 잘못되어 뒤틀린 형태</li>
 *   <li><b>나비넥타이 형태(Bowtie/Figure-8)</b>: 한 점에서 교차하여 8자 모양이 된 폴리곤</li>
 *   <li><b>중복 정점</b>: 동일한 좌표가 연속으로 나타나는 경우</li>
 *   <li><b>스파이크(Spike)</b>: 매우 좁은 돌출부가 있는 폴리곤</li>
 * </ul>
 *
 * <h3>유효하지 않은 폴리곤 예시</h3>
 * <pre>
 *   자가 교차 (Self-intersection)          나비넥타이 (Bowtie)
 *
 *       1───2                                  1
 *       │ ╲ │                                 /│\
 *       │  ╲│                                / │ \
 *       │  /│                               /  │  \
 *       │ / │                              2───┼───3
 *       4───3                                  │
 *                                              4
 *   → 경계선이 내부에서 교차            → 중심점에서 두 삼각형이 교차
 * </pre>
 *
 * <h3>수정 알고리즘: Buffer(0) 트릭</h3>
 * <p>이 함수는 JTS의 {@code buffer(0)} 연산을 사용하여 기하학적 오류를 수정합니다.
 * Buffer(0)는 폭이 0인 버퍼를 생성하는 연산으로, 다음과 같이 동작합니다:</p>
 * <ol>
 *   <li>입력 도형의 경계를 따라 노드(node)를 생성</li>
 *   <li>교차점에서 도형을 분할</li>
 *   <li>토폴로지 규칙에 따라 영역을 재구성</li>
 *   <li>유효한 단일 또는 복합 도형으로 결과 반환</li>
 * </ol>
 *
 * <h3>사용 예시</h3>
 * <pre>{@code
 * -- 함수 등록
 * CREATE TEMPORARY FUNCTION sdo_rectify_geometry AS 'io.datadynamics.hive.udf.geospatial.SDO_RectifyGeometry';
 *
 * -- 유효하지 않은 공간 객체 수정
 * SELECT sdo_rectify_geometry(geom_bytes) AS rectified_geom
 * FROM spatial_table
 * WHERE NOT ST_IsValid(geom_bytes);
 *
 * -- 테이블 전체 데이터 정제
 * INSERT OVERWRITE TABLE spatial_table_clean
 * SELECT id, sdo_rectify_geometry(geom_bytes) AS geom_bytes
 * FROM spatial_table;
 *
 * -- 수정 전후 비교
 * SELECT
 *   id,
 *   ST_IsValid(geom_bytes) AS before_valid,
 *   ST_IsValid(sdo_rectify_geometry(geom_bytes)) AS after_valid
 * FROM spatial_table;
 * }</pre>
 *
 * <h3>주의사항</h3>
 * <ul>
 *   <li>수정 과정에서 도형의 면적이나 형태가 약간 변경될 수 있습니다</li>
 *   <li>자가 교차 폴리곤은 여러 개의 폴리곤(MultiPolygon)으로 분리될 수 있습니다</li>
 *   <li>수정 불가능한 심각한 오류의 경우 NULL을 반환합니다</li>
 *   <li>이미 유효한 도형은 수정 없이 원본 그대로 반환됩니다</li>
 * </ul>
 *
 * <h3>관련 함수</h3>
 * <ul>
 *   <li>SDO_SELF_UNION - 유사한 목적으로 자가 합집합을 통해 오류 수정</li>
 *   <li>ST_IsValid - 도형의 유효성 검사</li>
 *   <li>ST_MakeValid (PostGIS) - 동일 기능의 PostGIS 함수</li>
 * </ul>
 *
 * @see GenericUDF
 * @see IsValidOp
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_UTIL-reference.html">Oracle SDO_UTIL.RECTIFY_GEOMETRY</a>
 * @see <a href="https://locationtech.github.io/jts/javadoc/org/locationtech/jts/operation/valid/IsValidOp.html">JTS IsValidOp</a>
 */
public class SDO_RectifyGeometry extends GenericUDF {

    private transient BinaryObjectInspector geomOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("SDO_RectifyGeometry requires 1 argument");
        }
        if (!(arguments[0] instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("SDO_RectifyGeometry requires a binary argument");
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

        // OGC Simple Feature 표준에 따른 유효성 검사 수행
        // IsValidOp은 자가 교차, 링 방향, 중복 정점 등을 검사
        if (new IsValidOp(geom).isValid()) {
            // 이미 유효한 도형은 변환 없이 원본 반환 (불필요한 직렬화 방지)
            return geomBytes;
        }

        // 유효하지 않은 도형에 대해 buffer(0) 트릭 적용
        // buffer(0)는 폭이 0인 버퍼를 생성하며, 이 과정에서:
        // 1. 모든 교차점에서 도형이 분할됨
        // 2. 토폴로지 규칙에 따라 영역이 재구성됨
        // 3. 결과적으로 유효한 도형이 생성됨
        try {
            Geometry rectified = geom.buffer(0);
            return GeometryUtils.geometryToBytes(rectified);
        } catch (Exception e) {
            // buffer(0)로도 수정할 수 없는 심각한 기하학적 오류의 경우
            // (예: 좌표가 NaN이거나, 극단적으로 복잡한 자가 교차)
            // NULL을 반환하여 데이터 품질 문제를 명시적으로 표시
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("SDO_RectifyGeometry", children);
    }
}