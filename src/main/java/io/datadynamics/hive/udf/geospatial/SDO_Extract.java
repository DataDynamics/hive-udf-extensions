package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

/**
 * 복합 기하학 객체에서 n번째 요소(Element) 또는 폴리곤의 n번째 링(Ring, 외곽선/구멍)을 추출한다. Oracle 인덱스는 1부터 시작한다.
 */
public class SDO_Extract extends UDF {

    // 오버로딩: Element만 추출
    public BytesWritable evaluate(BytesWritable geomBytes, Integer elemIndex) {
        return evaluate(geomBytes, elemIndex, 0);
    }

    // Element 및 Ring 추출
    public BytesWritable evaluate(BytesWritable geomBytes, Integer elemIndex, Integer ringIndex) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);
        if (geom == null || elemIndex == null) return null;

        // Oracle 1-based index -> Java 0-based index 변환
        int eIdx = elemIndex - 1;
        int rIdx = (ringIndex == null) ? -1 : ringIndex - 1;

        if (eIdx < 0 || eIdx >= geom.getNumGeometries()) return null;

        Geometry element = geom.getGeometryN(eIdx);

        // Ring 추출이 필요 없거나 대상이 Polygon이 아닌 경우 Element 반환
        if (rIdx < 0 || !(element instanceof Polygon)) {
            return GeometryUtils.geometryToBytes(element);
        }

        Polygon poly = (Polygon) element;
        // Ring Index 1: Exterior Ring
        if (rIdx == 0) {
            return GeometryUtils.geometryToBytes(poly.getExteriorRing());
        } else {
            // Ring Index 2+: Interior Ring (Holes)
            int innerIdx = rIdx - 1;
            if (innerIdx >= 0 && innerIdx < poly.getNumInteriorRing()) {
                return GeometryUtils.geometryToBytes(poly.getInteriorRingN(innerIdx));
            }
        }
        return null;
    }
}