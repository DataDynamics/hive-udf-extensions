package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * 객체의 최소 경계 사각형(Envelope)에서 X(1) 또는 Y(2) 축의 최대/최소값을 반환한다
 */
public class SDO_MaxMbrOrdinate extends UDF {

    /**
     * @param ordinatePos 1 for X, 2 for Y
     */
    public Double evaluate(BytesWritable geomBytes, Integer ordinatePos) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);
        if (geom == null || ordinatePos == null) return null;

        Envelope env = geom.getEnvelopeInternal();

        // Oracle Dimension Index: 1 -> X, 2 -> Y
        if (ordinatePos == 1) return env.getMaxX();
        if (ordinatePos == 2) return env.getMaxY();

        // Z축 지원은 추가 구현 필요 (CoordinateSequence 순회)
        return null;
    }
}