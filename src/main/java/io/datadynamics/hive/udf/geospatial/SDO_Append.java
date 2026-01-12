package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * 두 기하학 객체를 단순히 하나의 객체로 합친다.
 * 공간적 Union(합집합) 연산이 아니라, 데이터 구조적 병합(예: LineString + LineString = MultiLineString)을 의미한다.
 */
public class SDO_Append extends UDF {

    private final GeometryFactory factory = new GeometryFactory();

    public BytesWritable evaluate(BytesWritable g1Bytes, BytesWritable g2Bytes) {
        Geometry g1 = GeometryUtils.bytesToGeometry(g1Bytes);
        Geometry g2 = GeometryUtils.bytesToGeometry(g2Bytes);

        if (g1 == null) return g2Bytes;
        if (g2 == null) return g1Bytes;

        // 두 객체를 배열로 묶어 Collection 생성
        Geometry[] geometries = new Geometry[]{g1, g2};

        // 더 정교한 구현을 위해서는 입력 타입(Polygon, LineString)을 확인하여
        // MultiPolygon, MultiLineString 등 구체적 타입으로 반환할 수 있음
        return GeometryUtils.geometryToBytes(factory.createGeometryCollection(geometries));
    }

}