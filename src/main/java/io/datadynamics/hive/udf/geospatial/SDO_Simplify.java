package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

/**
 * Hive UDF로서 공간 객체(Geometry)를 단순화(Simplify)하는 함수입니다.
 *
 * <p>이 클래스는 Oracle Spatial의 SDO_UTIL.SIMPLIFY 함수와 유사한 기능을 제공합니다.
 * Douglas-Peucker 알고리즘을 기반으로 하는 {@link TopologyPreservingSimplifier}를 사용하여
 * 기하 도형의 정점(vertex) 수를 줄이면서도 위상(topology) 관계를 보존합니다.</p>
 *
 * <h2>사용 목적</h2>
 * <ul>
 *   <li>대용량 공간 데이터의 저장 공간 절약</li>
 *   <li>지도 렌더링 성능 향상 (줌 레벨에 따른 세밀도 조절)</li>
 *   <li>공간 연산 속도 개선 (정점 수 감소로 인한 계산량 감소)</li>
 *   <li>데이터 전송량 최소화</li>
 * </ul>
 *
 * <h2>Douglas-Peucker 알고리즘</h2>
 * <p>Douglas-Peucker 알고리즘은 곡선을 근사화하는 데 널리 사용되는 알고리즘입니다.
 * 주어진 허용 오차(tolerance/threshold) 내에서 원본 형상과 가장 유사한 단순화된 형상을 생성합니다.
 * 알고리즘은 재귀적으로 동작하며, 시작점과 끝점을 연결한 직선에서 가장 먼 점을 찾아
 * 그 거리가 허용 오차보다 크면 해당 점을 유지하고, 그렇지 않으면 제거합니다.</p>
 *
 * <h2>TopologyPreservingSimplifier의 특징</h2>
 * <p>JTS 라이브러리의 {@link TopologyPreservingSimplifier}는 일반 Douglas-Peucker 알고리즘과 달리
 * 다음과 같은 위상 관계를 보존합니다:</p>
 * <ul>
 *   <li>폴리곤의 유효성 유지 (자기 교차 방지)</li>
 *   <li>인접 폴리곤 간의 경계 일관성 유지</li>
 *   <li>링(ring)의 최소 정점 수 보장</li>
 * </ul>
 *
 * <h2>Hive에서의 사용 예시</h2>
 * <pre>{@code
 * -- UDF 등록
 * CREATE TEMPORARY FUNCTION SDO_Simplify AS 'io.datadynamics.hive.udf.geospatial.SDO_Simplify';
 *
 * -- 사용 예시: 허용 오차 0.001로 geometry 단순화
 * SELECT SDO_Simplify(geometry_column, 0.001) AS simplified_geom
 * FROM spatial_table;
 *
 * -- 줌 레벨에 따른 동적 단순화
 * SELECT
 *   CASE
 *     WHEN zoom_level < 5 THEN SDO_Simplify(geom, 0.01)
 *     WHEN zoom_level < 10 THEN SDO_Simplify(geom, 0.001)
 *     ELSE geom
 *   END AS display_geom
 * FROM map_features;
 * }</pre>
 *
 * <h2>threshold(허용 오차) 값 선택 가이드</h2>
 * <ul>
 *   <li><b>0.0001</b>: 매우 정밀한 단순화, 원본과 거의 동일</li>
 *   <li><b>0.001</b>: 일반적인 용도에 적합한 단순화</li>
 *   <li><b>0.01</b>: 중간 수준의 단순화, 대략적인 형태 유지</li>
 *   <li><b>0.1</b>: 강한 단순화, 개략적인 윤곽만 유지</li>
 * </ul>
 * <p>참고: threshold 값은 좌표계의 단위에 따라 다르게 적용됩니다.
 * WGS84(EPSG:4326)의 경우 도(degree) 단위이며, UTM 좌표계의 경우 미터(meter) 단위입니다.</p>
 *
 * @author Data Dynamics
 * @version 1.0
 * @see org.locationtech.jts.simplify.TopologyPreservingSimplifier
 * @see org.locationtech.jts.simplify.DouglasPeuckerSimplifier
 * @see <a href="https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm">Douglas-Peucker Algorithm (Wikipedia)</a>
 */
public class SDO_Simplify extends GenericUDF {

    private transient BinaryObjectInspector geomOI;
    private transient DoubleObjectInspector thresholdOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("SDO_Simplify requires 2 arguments");
        }
        if (!(arguments[0] instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("SDO_Simplify requires a binary argument as the first parameter");
        }
        if (!(arguments[1] instanceof DoubleObjectInspector)) {
            throw new UDFArgumentException("SDO_Simplify requires a double argument as the second parameter");
        }
        this.geomOI = (BinaryObjectInspector) arguments[0];
        this.thresholdOI = (DoubleObjectInspector) arguments[1];
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object geomObj = arguments[0].get();
        Object thresholdObj = arguments[1].get();

        BytesWritable geomBytes = geomOI.getPrimitiveWritableObject(geomObj);
        Double threshold = (thresholdObj == null) ? null : thresholdOI.get(thresholdObj);

        // 1. 입력 바이트 배열을 JTS Geometry 객체로 역직렬화
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        // 2. null 체크: geometry 또는 threshold가 null이면 null 반환
        if (geom == null || threshold == null) return null;

        // 3. TopologyPreservingSimplifier를 사용하여 geometry 단순화
        //    - Douglas-Peucker 알고리즘 기반
        //    - 위상 관계(topology) 보존
        //    - 폴리곤의 자기 교차 방지
        Geometry simplified = TopologyPreservingSimplifier.simplify(geom, threshold);

        // 4. 단순화된 geometry를 바이트 배열로 직렬화하여 반환
        return GeometryUtils.geometryToBytes(simplified);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("SDO_Simplify", children);
    }

}
