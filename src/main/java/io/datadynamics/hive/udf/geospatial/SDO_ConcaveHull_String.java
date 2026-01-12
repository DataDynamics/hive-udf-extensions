package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.locationtech.jts.algorithm.hull.ConcaveHull;
import org.locationtech.jts.geom.Geometry;

public class SDO_ConcaveHull_String extends UDF {

    /**
     * @param wkt 입력 기하학 객체 (WKT)
     * @param tolerance 허용 오차 (JTS에서는 Edge Length Threshold로 해석)
     * @return Concave Hull Geometry (WKT)
     */
    public String evaluate(String wkt, Double tolerance) {
        Geometry geom = GeometryUtils.stringToGeometry(wkt);

        if (geom == null) return null;

        ConcaveHull hull = new ConcaveHull(geom);
        // Oracle tolerance가 null이면 JTS 자동 계산 로직 따름
        if (tolerance != null && tolerance > 0) {
            hull.setMaximumEdgeLength(tolerance);
        }

        Geometry result = hull.getHull();
        return GeometryUtils.geometryToString(result);
    }

}
