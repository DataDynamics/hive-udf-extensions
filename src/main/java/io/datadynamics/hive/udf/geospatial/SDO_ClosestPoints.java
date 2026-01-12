package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.operation.distance.DistanceOp;

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