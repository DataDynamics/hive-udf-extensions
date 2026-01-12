package io.datadynamics.hive.udf.geospatial;

/**
 * 유효성 검사 결과를 'TRUE' 또는 오류 원인과 좌표를 포함한 문자열로 반환한다.
 */

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.locationtech.jts.operation.valid.TopologyValidationError;

public class SDO_ValidateGeometryWithContext extends UDF {
    public String evaluate(BytesWritable geomBytes) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);
        if (geom == null) return "NULL GEOMETRY";

        IsValidOp isValidOp = new IsValidOp(geom);
        if (isValidOp.isValid()) {
            return "TRUE";
        } else {
            TopologyValidationError err = isValidOp.getValidationError();
            return String.format("FALSE: %s at %s", err.getMessage(), err.getCoordinate());
        }
    }
}