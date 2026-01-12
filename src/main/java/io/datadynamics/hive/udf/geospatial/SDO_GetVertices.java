package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

/**
 * 객체를 구성하는 모든 정점(Vertex)을 테이블 행(Row) 형태로 반환한다
 */
public class SDO_GetVertices extends UDF {

    public String evaluate(BytesWritable geomBytes) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);
        if (geom == null) return null;

        Coordinate[] coordinates = geom.getCoordinates();

        return "";
    }

}