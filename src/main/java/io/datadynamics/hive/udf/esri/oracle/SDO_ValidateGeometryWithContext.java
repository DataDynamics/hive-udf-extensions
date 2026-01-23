package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.OperatorSimplify;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 공간 객체의 기하학적 유효성을 검사하고, 오류 발생 시 원인과 위치 정보를 포함한
 * 상세한 컨텍스트 문자열을 반환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT 함수와 유사한 기능을 제공합니다.
 * ESRI Shape 형식을 입력받아 ESRI Geometry API를 사용하여 검사를 수행합니다.</p>
 *
 * <h3>반환값 형식</h3>
 * <pre>
 *   유효한 경우: "TRUE"
 *   유효하지 않은 경우: "FALSE: [오류 유형]"
 *   NULL 입력인 경우: "NULL GEOMETRY"
 * </pre>
 */
public class SDO_ValidateGeometryWithContext extends ST_GeometryAccessor {

    private static final Logger LOG = LoggerFactory.getLogger(SDO_ValidateGeometryWithContext.class);

    /**
     * 공간 객체의 유효성을 검사합니다.
     *
     * @param geom ESRI Shape 형식의 기하 데이터
     * @return 유효성 검사 결과 문자열
     */
    public String evaluate(BytesWritable geom) {
        if (geom == null || geom.getLength() == 0) {
            return "NULL GEOMETRY";
        }

        try {
            // 1. ESRI Shape -> OGCGeometry
            OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
            if (ogcGeom == null) {
                return "NULL GEOMETRY";
            }

            com.esri.core.geometry.Geometry esriGeom = ogcGeom.getEsriGeometry();
            SpatialReference sr = ogcGeom.esriSR;

            // 2. 유효성 검사 수행
            // ESRI API는 JTS처럼 상세한 오류 메시지를 직접 제공하지 않으므로, 
            // isSimple() 결과를 사용하여 판단합니다.
            // Polygon의 경우 생성 시점에 정규화(Simplify)되지 않으면 isSimple이 false일 수 있으므로
            // 명시적으로 Simplify를 수행하여 유효성을 검증합니다.
            
            try {
                // OperatorSimplify.execute()를 force=true로 수행하면 
                // 유효하지 않은 기하 구조를 유효하게 만듭니다.
                // 만약 이 과정에서 예외가 발생하거나 결과가 null이면 유효하지 않은 것입니다.
                com.esri.core.geometry.Geometry simplifiedGeom = OperatorSimplify.local().execute(esriGeom, sr, true, null);
                
                if (simplifiedGeom == null) {
                    return "FALSE: Could not simplify geometry";
                }

                // ESRI에서는 명시적으로 isSimpleAsFeature를 사용하여 OGC Simple Feature 규격을 체크합니다.
                if (OperatorSimplify.local().isSimpleAsFeature(esriGeom, sr, null)) {
                    return "TRUE";
                } else {
                    return "FALSE: Not simple (e.g. self-intersection)";
                }
            } catch (Exception e) {
                return "FALSE: Exception during validation - " + e.getMessage();
            }

        } catch (Exception e) {
            LOG.error("Error in SDO_ValidateGeometryWithContext: " + e.getMessage(), e);
            return "ERROR: " + e.getMessage();
        }
    }
}
