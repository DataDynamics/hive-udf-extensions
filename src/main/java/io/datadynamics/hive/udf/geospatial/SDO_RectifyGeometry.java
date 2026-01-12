package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.valid.IsValidOp;

/**
 * 자가 교차(Self-intersection)나 꼬인 폴리곤과 같은 기하학적 오류를 수정한다.
 * SDO_SELF_UNION 역시 유사한 목적으로 사용된다.
 */
public class SDO_RectifyGeometry extends UDF {
    public BytesWritable evaluate(BytesWritable geomBytes) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);
        if (geom == null) return null;

        // 이미 유효하면 그대로 반환
        if (new IsValidOp(geom).isValid()) {
            return geomBytes;
        }

        // 유효하지 않은 경우 buffer(0) 트릭을 사용하여 토폴로지 재구성
        try {
            Geometry rectified = geom.buffer(0);
            return GeometryUtils.geometryToBytes(rectified);
        } catch (Exception e) {
            // 수정 불가능한 경우 NULL 반환
            return null;
        }
    }
}