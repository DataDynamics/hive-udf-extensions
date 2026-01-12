package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.operation.distance.DistanceOp;

/**
 * 두 객체 간의 최단 거리를 형성하는 두 점을 반환한다. SDO_CLOSEST_POINTS_TYPE 객체를 반환하지만, Impala 스칼라 UDF에서는 두 점을 잇는 LINESTRING 또는 MULTIPOINT로 반환하는 것이 적합하다.
 */
public class SDO_ClosestPoints extends UDF {

    private final GeometryFactory factory = new GeometryFactory();

    public BytesWritable evaluate(BytesWritable geom1Bytes, BytesWritable geom2Bytes) {
        Geometry g1 = GeometryUtils.bytesToGeometry(geom1Bytes);
        Geometry g2 = GeometryUtils.bytesToGeometry(geom2Bytes);

        if (g1 == null || g2 == null) return null;

        // 최단 거리 점 쌍 계산
        Coordinate[] nearestCoords = DistanceOp.nearestPoints(g1, g2);

        // 결과가 존재하면 두 점을 잇는 LineString 반환 (시각화 및 거리 측정 용이)
        if (nearestCoords != null && nearestCoords.length >= 2) {
            Geometry result = factory.createLineString(nearestCoords);
            return GeometryUtils.geometryToBytes(result);
        }
        return null;
    }

}