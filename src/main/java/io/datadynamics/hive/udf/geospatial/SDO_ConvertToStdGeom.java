package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;

/**
 * 3차원/4차원(Measure 포함) 좌표에서 M값을 제거하여 표준 2D/3D로 변환
 */
public class SDO_ConvertToStdGeom extends UDF {
    // M차원 제거 (예시: XYZM -> XYZ, XYM -> XY)
    public BytesWritable evaluate(BytesWritable geomBytes) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);
        if (geom == null) return null;

        // JTS는 기본적으로 XY, XYZ를 지원. 복잡한 차원 축소 로직은
        // CoordinateFilter를 구현하여 Z/M 값을 NaN 또는 0으로 치환하거나
        // 새로운 CoordinateSequenceFactory로 좌표를 복사해야 함.
        // 여기서는 단순화를 위해 생략하며, 실제 구현 시 CoordinateFilter 사용 권장.
        return geomBytes; // Placeholder
    }
}