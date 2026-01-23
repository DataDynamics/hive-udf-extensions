package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.*;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 기하 구조에서 특정 요소(element)나 링(ring)을 추출하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.SDO_EXTRACT 함수와 유사한 기능을 제공합니다.</p>
 *
 * <h3>Oracle SDO_EXTRACT 파라미터 정의:</h3>
 * <ul>
 *   <li><b>element_index</b>: 추출할 요소의 인덱스 (1부터 시작).
 *       다중 기하(Multi-part)인 경우 특정 파트를 지정합니다.</li>
 *   <li><b>ring_index</b>: 추출할 링의 인덱스 (1부터 시작).
 *       주로 폴리곤의 외곽선(Outer Ring)이나 내곽선(Inner Ring)을 추출할 때 사용합니다.</li>
 * </ul>
 *
 * 함수 사용시 Oracle의 index 시작과 UDF의 index 시작이 1부터 시작하는 점에 유의하세요.
 *
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_GEOM-reference.html#GUID-E1E7D012-70B3-4E82-9333-33C4092E4264">Oracle SDO_GEOM.SDO_EXTRACT</a>
 */
public class SDO_Extract extends ST_GeometryAccessor {

    static final Logger LOG = LoggerFactory.getLogger(SDO_Extract.class.getName());

    /**
     * 입력 기하 구조에서 특정 요소와 링을 추출합니다.
     *
     * @param geom         ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @param elementIndex 추출할 요소의 인덱스 (1-based)
     * @param ringIndex    추출할 링의 인덱스 (1-based)
     * @return 추출된 기하 구조를 포함하는 BytesWritable. 오류 시 null.
     */
    public BytesWritable evaluate(BytesWritable geom, Integer elementIndex, Integer ringIndex) {
        if (geom == null || geom.getLength() == 0 || elementIndex == null) {
            return null;
        }

        try {
            OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
            if (ogcGeom == null) {
                return null;
            }

            Geometry esriGeom = ogcGeom.getEsriGeometry();
            SpatialReference sr = ogcGeom.esriSR;
            Geometry resultGeom = null;

            // Oracle은 1-based index를 사용하므로 0-based로 변환
            int eIdx = elementIndex - 1;

            if (esriGeom instanceof MultiPath) {
                MultiPath multiPath = (MultiPath) esriGeom;
                int pathCount = multiPath.getPathCount();

                if (eIdx < 0 || eIdx >= pathCount) {
                    return null;
                }

                if (ringIndex == null) {
                    // elementIndex만 지정된 경우 해당 파트 전체를 추출
                    // ESRI MultiPath에서 특정 Path만 추출하려면 새로운 Geometry 생성 필요
                    if (esriGeom instanceof Polygon) {
                        Polygon poly = new Polygon();
                        poly.addPath(multiPath, eIdx, true);
                        resultGeom = poly;
                    } else if (esriGeom instanceof Polyline) {
                        Polyline line = new Polyline();
                        line.addPath(multiPath, eIdx, true);
                        resultGeom = line;
                    }
                } else {
                    // ringIndex까지 지정된 경우 (주로 Polygon)
                    // ESRI MultiPath에서는 Path가 곧 Ring 역할을 함
                    // Oracle SDO_EXTRACT에서 Polygon의 경우 element는 폴리곤 자체(보통 1), ring은 개별 링을 의미하는 경우가 많음
                    // 하지만 MultiPolygon인 경우 element가 n번째 폴리곤일 수 있음
                    // 여기서는 ESRI의 path 인덱스를 기준으로 처리 (elementIndex는 path 인덱스)
                    // 만약 elementIndex가 지정된 상황에서 ringIndex도 지정되었다면, 
                    // 단순화를 위해 ringIndex를 path 인덱스로 사용하거나 무시할 수 있으나
                    // Oracle 규격에 가깝게 맞추기 위해 eIdx를 사용함.
                    
                    // ESRI MultiPath (Polygon/Polyline)는 path들의 집합임.
                    // Polygon의 경우 첫번째 path는 outer ring, 나머지는 inner ring인 경우가 많음 (ESRI 규칙에 따라 다름)
                    // Oracle SDO_EXTRACT(geom, element, ring)에서 ring은 보통 element 내부의 서브 요소를 의미함.
                    
                    // 여기서는 elementIndex가 가리키는 파트 내에서 ringIndex를 찾는 것이 아니라,
                    // 전체 Path 중 n번째를 찾는 방식으로 일단 구현 (Oracle의 복잡한 SDO_ELEM_INFO_ARRAY 대응은 한계가 있음)
                    // 만약 ringIndex가 제공되면 eIdx(element)는 무시하고 ringIndex를 절대 인덱스로 사용할 수도 있음.
                    
                    int rIdx = ringIndex - 1;
                    if (rIdx < 0 || rIdx >= pathCount) {
                        return null;
                    }
                    
                    if (esriGeom instanceof Polygon) {
                        Polygon poly = new Polygon();
                        poly.addPath(multiPath, rIdx, true);
                        resultGeom = poly;
                    } else if (esriGeom instanceof Polyline) {
                        Polyline line = new Polyline();
                        line.addPath(multiPath, rIdx, true);
                        resultGeom = line;
                    }
                }
            } else if (esriGeom instanceof MultiPoint) {
                MultiPoint multiPoint = (MultiPoint) esriGeom;
                if (eIdx < 0 || eIdx >= multiPoint.getPointCount()) {
                    return null;
                }
                resultGeom = multiPoint.getPoint(eIdx);
            } else if (esriGeom instanceof Point) {
                if (eIdx == 0) {
                    resultGeom = esriGeom;
                } else {
                    return null;
                }
            }

            if (resultGeom == null) {
                return null;
            }

            OGCGeometry ogcResult = OGCGeometry.createFromEsriGeometry(resultGeom, sr);
            return GeometryUtils.geometryToEsriShapeBytesWritable(ogcResult);

        } catch (Exception e) {
            LOG.error("Error in SDO_Extract: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * 특정 요소만 추출합니다.
     *
     * @param geom         ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @param elementIndex 추출할 요소의 인덱스 (1-based)
     * @return 추출된 기하 구조를 포함하는 BytesWritable. 오류 시 null.
     */
    public BytesWritable evaluate(BytesWritable geom, Integer elementIndex) {
        return evaluate(geom, elementIndex, null);
    }
}