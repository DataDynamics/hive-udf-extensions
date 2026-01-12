package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.operation.distance.DistanceOp;

/**
 * 두 객체 간의 최단 거리를 형성하는 두 점을 반환한다. SDO_CLOSEST_POINTS_TYPE 객체를 반환하지만, Impala 스칼라 UDF에서는 두 점을 잇는 LINESTRING 또는 MULTIPOINT로 반환하는 것이 적합하다.
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
