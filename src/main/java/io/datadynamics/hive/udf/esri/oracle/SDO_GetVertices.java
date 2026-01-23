package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 기하 구조에서 모든 정점(Vertex)을 추출하여 JSON 문자열 형태로 반환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_UTIL.GETVERTICES 함수와 호환되도록 설계되었으나,
 * Impala 등 UDTF를 지원하지 않는 환경을 위해 UDF로 구현되었습니다.</p>
 *
 * <h3>출력 구조 (JSON Array of Objects)</h3>
 * <pre>
 * [
 *   {"x": 10.0, "y": 20.0, "z": null, "w": null, "id": 1},
 *   ...
 * ]
 * </pre>
 *
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_UTIL-reference.html#GUID-7A7B0A9B-98A8-433E-A52A-3A96C2422C39">Oracle SDO_UTIL.GETVERTICES</a>
 */
@Description(name = "SDO_GetVertices",
        value = "_FUNC_(geometry) - returns a JSON string containing an array of vertices from the geometry",
        extended = "Example:\n"
                + "  SELECT SDO_GetVertices(geom) FROM t;")
public class SDO_GetVertices extends ST_GeometryAccessor {

    static final Logger LOG = LoggerFactory.getLogger(SDO_GetVertices.class.getName());

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 기하 구조에서 모든 정점을 추출하여 JSON 문자열로 반환합니다.
     *
     * @param geom ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @return 정점들의 JSON 문자열. 오류 시 null.
     */
    public String evaluate(BytesWritable geom) {
        if (geom == null || geom.getLength() == 0) {
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

            List<Vertex> vertices = new ArrayList<>();
            boolean hasZ = esriGeom.hasZ();
            boolean hasM = esriGeom.hasM();

            if (esriGeom instanceof Point) {
                Point p = (Point) esriGeom;
                vertices.add(createVertex(p, 1, hasZ, hasM));
            } else if (esriGeom instanceof MultiPoint) {
                MultiPoint mp = (MultiPoint) esriGeom;
                for (int i = 0; i < mp.getPointCount(); i++) {
                    vertices.add(createVertex(mp.getPoint(i), i + 1, hasZ, hasM));
                }
            } else if (esriGeom instanceof MultiPath) {
                MultiPath path = (MultiPath) esriGeom;
                for (int i = 0; i < path.getPointCount(); i++) {
                    vertices.add(createVertex(path.getPoint(i), i + 1, hasZ, hasM));
                }
            }
            return mapper.writeValueAsString(vertices);
        } catch (Exception e) {
            LOG.error("Error in SDO_GetVertices: " + e.getMessage(), e);
            return null;
        }
    }

    private Vertex createVertex(Point p, int id, boolean hasZ, boolean hasM) {
        Vertex v = new Vertex();
        v.x = p.getX();
        v.y = p.getY();
        v.z = hasZ ? p.getZ() : null;
        v.w = hasM ? p.getM() : null;
        v.id = id;
        return v;
    }

    /**
     * 정점 정보를 담는 구조체 클래스입니다.
     * Hive UDF에서 Struct로 인식됩니다.
     */
    public static class Vertex {
        public Double x;
        public Double y;
        public Double z;
        public Double w;
        public Integer id;
    }
}
