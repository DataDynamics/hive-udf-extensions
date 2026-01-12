package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.algorithm.hull.ConcaveHull;
import org.locationtech.jts.geom.Geometry;

/**
 * 점 집합이나 형상을 감싸는 오목한(Concave) 경계 다각형을 생성한다. tolerance 파라미터를 사용하여 오목함의 정도를 제어한다.
 */
public class SDO_ConcaveHull extends UDF {

    /**
     * @param geomBytes 입력 기하학 객체 (WKB)
     * @param tolerance 허용 오차 (JTS에서는 Edge Length Threshold로 해석)
     * @return Concave Hull Geometry (WKB)
     */
    public BytesWritable evaluate(BytesWritable geomBytes, Double tolerance) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        if (geom == null) return null;

        ConcaveHull hull = new ConcaveHull(geom);
        // Oracle tolerance가 null이면 JTS 자동 계산 로직 따름
        if (tolerance != null && tolerance > 0) {
            hull.setMaximumEdgeLength(tolerance);
        }

        Geometry result = hull.getHull();
        return GeometryUtils.geometryToBytes(result);
    }

}