package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.linearref.LengthIndexedLine;

/**
 * LRS(Linear Referencing System) 세그먼트를 주어진 Measure(M값)를 기준으로 두 개의 세그먼트로 분할한다.
 */
public class SDO_SplitGeomSegment extends UDF {

    private final GeometryFactory factory = new GeometryFactory();

    public BytesWritable evaluate(BytesWritable geomBytes, Double splitMeasure) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        if (geom == null || splitMeasure == null) return null;

        // JTS LengthIndexedLine 초기화
        LengthIndexedLine lil = new LengthIndexedLine(geom);

        double startInfo = lil.getStartIndex();
        double endInfo = lil.getEndIndex();

        // 분할 지점이 범위 밖인 경우 처리
        if (splitMeasure < startInfo || splitMeasure > endInfo) return null;

        // 분할 수행: Start~Split, Split~End
        Geometry seg1 = lil.extractLine(startInfo, splitMeasure);
        Geometry seg2 = lil.extractLine(splitMeasure, endInfo);

        // 두 세그먼트를 GeometryCollection으로 묶어서 반환
        Geometry[] geometries = new Geometry[]{seg1, seg2};
        Geometry result = factory.createGeometryCollection(geometries);

        return GeometryUtils.geometryToBytes(result);
    }

}