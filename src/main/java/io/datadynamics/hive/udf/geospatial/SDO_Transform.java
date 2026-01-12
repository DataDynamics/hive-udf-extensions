package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;

public class SDO_Transform extends UDF {
    public BytesWritable evaluate(BytesWritable geomBytes, String sourceCrsCode, String targetCrsCode) {
        Geometry geom = GeometryUtils.bytesToGeometry(geomBytes);

        if (geom == null || sourceCrsCode == null || targetCrsCode == null) return null;

        try {
            // GeoTools를 이용한 좌표계 디코딩 (예: "EPSG:4326")
            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceCrsCode);
            CoordinateReferenceSystem targetCRS = CRS.decode(targetCrsCode);
            boolean lenient = true; // 변환 시 약간의 오차 허용

            MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
            Geometry transformedGeom = JTS.transform(geom, transform);

            // 변환된 SRID 설정 (WKBWriter에서 사용될 수 있음)
            try {
                String[] split = targetCrsCode.split(":");
                int srid = Integer.parseInt(split[1]);
                transformedGeom.setSRID(srid);
            } catch (Exception ignore) {
                // Ignored
            }

            return GeometryUtils.geometryToBytes(transformedGeom);
        } catch (Exception e) {
            // 좌표계 정의를 찾을 수 없거나 변환 실패 시 NULL 반환
            return null;
        }
    }
}