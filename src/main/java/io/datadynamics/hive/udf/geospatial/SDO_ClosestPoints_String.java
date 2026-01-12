package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.operation.distance.DistanceOp;

/**
 * 두 Geometry 간의 최단 거리에 있는 점들을 계산하여 LineString으로 반환하는 UDF (WKT 입력/출력)
 */
public class SDO_ClosestPoints_String extends UDF {

    private final GeometryFactory factory = new GeometryFactory();

    public String evaluate(String wkt1, String wkt2) {
        Geometry g1 = GeometryUtils.stringToGeometry(wkt1);
        Geometry g2 = GeometryUtils.stringToGeometry(wkt2);

        if (g1 == null || g2 == null) return null;

        // 최단 거리 점 쌍 계산
        Coordinate[] nearestCoords = DistanceOp.nearestPoints(g1, g2);

        // 결과가 존재하면 두 점을 잇는 LineString 반환
        if (nearestCoords != null && nearestCoords.length >= 2) {
            Geometry result = factory.createLineString(nearestCoords);
            return GeometryUtils.geometryToString(result);
        }
        return null;
    }

}
