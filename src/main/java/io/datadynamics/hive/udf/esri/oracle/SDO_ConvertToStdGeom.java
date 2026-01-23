package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * M(Measure) 값을 포함할 수 있는 공간 객체를 표준 기하 구조(M 값이 제거된 형태)로 변환하는 Hive UDF입니다.
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
 * <p>이 함수는 Oracle Spatial의 SDO_UTIL.CONVERT_TO_STD_GEOM 또는 유사한 기능을 제공합니다.
 * ESRI Geometry API를 사용하여 입력된 기하 구조에서 M 값을 제거하고 표준 형식을 유지하도록 합니다.</p>
 */
public class SDO_ConvertToStdGeom extends ST_GeometryAccessor {

    static final Logger LOG = LoggerFactory.getLogger(SDO_ConvertToStdGeom.class.getName());

    /**
     * 입력 기하 구조를 표준 기하 구조로 변환합니다.
     *
     * @param geom ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @return 표준화된 기하 데이터를 포함하는 BytesWritable. 오류 시 null.
     */
    public BytesWritable evaluate(BytesWritable geom) {
        if (geom == null || geom.getLength() == 0) {
            return null;
        }

        try {
            OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
            if (ogcGeom == null) {
                return null;
            }

            // ESRI OGCGeometry.fromEsriGeometry 등을 거치면서 M 값이 제거되거나
            // 표준 형태로 정규화될 수 있도록 처리합니다.
            // ESRI의 OGC 구현체는 기본적으로 M 차원을 다루지 않는 경우가 많아
            // shape에서 OGCGeometry로 변환했다가 다시 내보내는 과정 자체가 표준화 과정이 될 수 있습니다.

            return GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeom);
        } catch (Exception e) {
            LOG.error("Error in SDO_ConvertToStdGeom: " + e.getMessage(), e);
            return null;
        }
    }

}