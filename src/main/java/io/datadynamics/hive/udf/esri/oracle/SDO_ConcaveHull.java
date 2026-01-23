package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 점 집합이나 공간 객체를 감싸는 오목한(Concave) 경계 다각형을 생성하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.SDO_CONCAVEHULL 또는 SDO_GEOM.SDO_CONCAVEHULL_BOUNDARY
 * 함수와 유사한 기능을 제공합니다.</p>
 *
 * <p><b>주의:</b> ESRI Geometry API(v2.2.4)는 공식적으로 Concave Hull 알고리즘을 직접 지원하지 않습니다.
 * 따라서 본 구현에서는 차선책으로 <b>Convex Hull</b>을 반환합니다.
 * 정밀한 Concave Hull 계산이 필요한 경우 JTS 기반의 구현체를 사용하십시오.</p>
 *
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_GEOM-reference.html">Oracle SDO_GEOM.SDO_CONCAVEHULL</a>
 */
public class SDO_ConcaveHull extends ST_GeometryAccessor {

    static final Logger LOG = LoggerFactory.getLogger(SDO_ConcaveHull.class.getName());

    /**
     * 입력 기하 구조의 Concave Hull을 계산합니다.
     * ESRI API 제약으로 인해 현재는 Convex Hull을 반환합니다.
     *
     * @param geom      ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @param tolerance 오목함 정도를 조절하는 파라미터 (현재 구현에서는 무시됨)
     * @return 계산된 Hull (Polygon)을 포함하는 BytesWritable. 오류 시 null.
     */
    public BytesWritable evaluate(BytesWritable geom, Double tolerance) {
        if (geom == null || geom.getLength() == 0) {
            return null;
        }

        OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
        if (ogcGeom == null) {
            return null;
        }

        try {
            // ESRI Geometry API에서 제공하는 Convex Hull 기능을 사용
            Geometry esriGeom = ogcGeom.getEsriGeometry();
            Geometry hullGeom = GeometryEngine.convexHull(esriGeom);

            OGCGeometry ogcHull = OGCGeometry.createFromEsriGeometry(hullGeom, ogcGeom.esriSR);

            return GeometryUtils.geometryToEsriShapeBytesWritable(ogcHull);
        } catch (Exception e) {
            LOG.error("Error in SDO_ConcaveHull: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * 파라미터가 1개인 경우의 evaluate 메서드 (tolerance가 생략된 경우)
     */
    public BytesWritable evaluate(BytesWritable geom) {
        return evaluate(geom, null);
    }
}