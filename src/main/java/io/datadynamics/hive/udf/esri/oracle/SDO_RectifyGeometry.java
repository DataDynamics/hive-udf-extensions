package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorSimplify;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 유효하지 않은 기하 구조를 수정하여 유효한 상태로 반환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.SDO_RECTIFY_GEOMETRY 함수와 유사한 기능을 제공합니다.
 * ESRI Geometry API의 OperatorSimplify를 사용하여 자기 교차(self-intersection),
 * 잘못된 링 방향(ring orientation) 등의 문제를 해결합니다.</p>
 *
 * <h3>Oracle SDO_RECTIFY_GEOMETRY 파라미터 정의:</h3>
 * <ul>
 *   <li><b>geometry</b>: 수정할 기하 객체.</li>
 *   <li><b>tolerance</b>: 기하학적 일치 여부를 결정하는 허용 오차.</li>
 * </ul>
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
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_GEOM-reference.html#GUID-E13D6042-3B10-48E0-9E07-074D41CCF554">Oracle SDO_GEOM.SDO_RECTIFY_GEOMETRY</a>
 */
public class SDO_RectifyGeometry extends ST_GeometryAccessor {

    static final Logger LOG = LoggerFactory.getLogger(SDO_RectifyGeometry.class.getName());

    /**
     * 입력 기하 구조를 수정하여 유효한 기하 구조로 반환합니다.
     *
     * @param geom      ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @param tolerance 허용 오차 (현재 ESRI OperatorSimplify에서는 직접 사용되지 않음)
     * @return 수정된 기하 구조를 포함하는 BytesWritable. 오류 시 null.
     */
    public BytesWritable evaluate(BytesWritable geom, Double tolerance) {
        if (geom == null || geom.getLength() == 0) {
            return null;
        }

        try {
            OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
            if (ogcGeom == null) {
                return null;
            }

            Geometry esriGeom = ogcGeom.getEsriGeometry();
            SpatialReference sr = ogcGeom.esriSR;

            // ESRI OperatorSimplify를 사용하여 기하 구조 수정
            // 이 연산자는 OGC 사양에 맞게 기하 구조를 정규화합니다.
            Geometry fixedGeom = OperatorSimplify.local().execute(esriGeom, sr, true, null);

            if (fixedGeom == null) {
                return null;
            }

            OGCGeometry ogcResult = OGCGeometry.createFromEsriGeometry(fixedGeom, sr);
            return GeometryUtils.geometryToEsriShapeBytesWritable(ogcResult);

        } catch (Exception e) {
            LOG.error("Error in SDO_RectifyGeometry: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * tolerance 없이 호출되는 경우의 evaluate 메서드
     */
    public BytesWritable evaluate(BytesWritable geom) {
        return evaluate(geom, null);
    }
}
