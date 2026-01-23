package io.datadynamics.hive.udf.esri.oracle;
 
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * 공간 객체의 최소 경계 사각형(MBR, Minimum Bounding Rectangle)에서
 * 지정된 축(X 또는 Y)의 최대값을 반환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.SDO_MAX_MBR_ORDINATE 함수와 유사한 기능을 제공합니다.
 * 공간 객체를 감싸는 최소 경계 사각형(Envelope)을 계산하고, 해당 사각형의 X 또는 Y 축 최댓값을 반환합니다.</p>
 *
 * <h3>좌표축 인덱스 (Oracle 호환)</h3>
 * <table border="1">
 *   <tr><th>ordinatePos</th><th>축</th><th>반환값</th></tr>
 *   <tr><td>1</td><td>X축</td><td>MBR의 최대 X 좌표 (MaxX)</td></tr>
 *   <tr><td>2</td><td>Y축</td><td>MBR의 최대 Y 좌표 (MaxY)</td></tr>
 * </table>
 *
 * <p>MBR은 공간 객체를 완전히 포함하는 가장 작은 직사각형입니다.
 * 축에 평행한(axis-aligned) 사각형으로, 공간 인덱싱과 빠른 공간 필터링에 사용됩니다.</p>
 *
 * <pre>
 *     MaxY ┌─────────────┐
 *          │    ****     │
 *          │   *    *    │  ← 공간 객체 (예: POLYGON)
 *          │  *      *   │
 *          │   *    *    │
 *          │    ****     │
 *     MinY └─────────────┘
 *         MinX          MaxX
 * </pre>
 *
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_GEOM-reference.html#GUID-A99A2D0D-342C-4BA4-927A-9B3E93C52382">Oracle SDO_GEOM.SDO_MAX_MBR_ORDINATE</a>
 */
public class SDO_MaxMbrOrdinate extends ST_GeometryAccessor {
 
    static final Logger LOG = LoggerFactory.getLogger(SDO_MaxMbrOrdinate.class.getName());
 
    /**
     * 입력 기하 구조의 MBR에서 지정된 축의 최댓값을 반환합니다.
     *
     * @param geom        ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @param ordinatePos 축 인덱스 (1: X, 2: Y)
     * @return 지정된 축의 최댓값 (Double). 오류 시 null.
     */
    public Double evaluate(BytesWritable geom, Integer ordinatePos) {
        if (geom == null || geom.getLength() == 0 || ordinatePos == null) {
            return null;
        }
 
        try {
            OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
            if (ogcGeom == null) {
                return null;
            }
 
            Geometry esriGeom = ogcGeom.getEsriGeometry();
            if (esriGeom == null || esriGeom.isEmpty()) {
                return null;
            }
 
            Envelope2D env = new Envelope2D();
            esriGeom.queryEnvelope2D(env);
 
            if (ordinatePos == 1) {
                return env.xmax;
            } else if (ordinatePos == 2) {
                return env.ymax;
            } else {
                // Oracle SDO_GEOM.SDO_MAX_MBR_ORDINATE supports 3 (Z) and 4 (M) if available
                // Current ESRI API Envelope2D only provides X and Y.
                // For Z and M, we need to iterate over vertices or use other methods.
                if (ordinatePos == 3 && esriGeom.hasZ()) {
                    // Simple implementation for Z: iterate all points
                    // In a more complex scenario, we could optimize this.
                    return getMaxZ(esriGeom);
                } else if (ordinatePos == 4 && esriGeom.hasM()) {
                    return getMaxM(esriGeom);
                }
                return null;
            }
        } catch (Exception e) {
            LOG.error("Error in SDO_MaxMbrOrdinate: " + e.getMessage(), e);
            return null;
        }
    }
 
    private Double getMaxZ(Geometry esriGeom) {
        // ESRI Geometry doesn't provide maxZ in Envelope2D.
        // For simplicity, we can get it from the Envelope (3D) if we had one, 
        // but let's just use the vertices for correctness across all geometry types.
        if (esriGeom instanceof com.esri.core.geometry.MultiVertexGeometry) {
            com.esri.core.geometry.MultiVertexGeometry mvGeom = (com.esri.core.geometry.MultiVertexGeometry) esriGeom;
            double maxZ = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < mvGeom.getPointCount(); i++) {
                maxZ = Math.max(maxZ, mvGeom.getPoint(i).getZ());
            }
            return maxZ == Double.NEGATIVE_INFINITY ? null : maxZ;
        } else if (esriGeom instanceof com.esri.core.geometry.Point) {
            return ((com.esri.core.geometry.Point) esriGeom).getZ();
        }
        return null;
    }
 
    private Double getMaxM(Geometry esriGeom) {
        if (esriGeom instanceof com.esri.core.geometry.MultiVertexGeometry) {
            com.esri.core.geometry.MultiVertexGeometry mvGeom = (com.esri.core.geometry.MultiVertexGeometry) esriGeom;
            double maxM = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < mvGeom.getPointCount(); i++) {
                maxM = Math.max(maxM, mvGeom.getPoint(i).getM());
            }
            return maxM == Double.NEGATIVE_INFINITY ? null : maxM;
        } else if (esriGeom instanceof com.esri.core.geometry.Point) {
            return ((com.esri.core.geometry.Point) esriGeom).getM();
        }
        return null;
    }
}
