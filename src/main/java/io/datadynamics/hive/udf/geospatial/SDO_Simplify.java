package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

/**
 * Douglas-Peucker 등의 알고리즘으로 형상의 정점 수를 줄인다
 */
public class SDO_Simplify extends UDF {

    /**
     * Simplifies geometry using threshold; returns serialized result
     */
    public BytesWritable evaluate(BytesWritable geomBytes, Double threshold) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        if (geom == null || threshold == null) return null;

        Geometry simplified = TopologyPreservingSimplifier.simplify(geom, threshold);
        return GeometryUtils.geometryToBytes(simplified);
    }

}