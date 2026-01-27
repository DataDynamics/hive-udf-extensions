package io.datadynamics.hive.udf.esri.oracle;

import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.referencing.CRS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 공간 객체의 좌표 참조 시스템(CRS, Coordinate Reference System)을 다른 좌표계로 변환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_CS.TRANSFORM 함수와 유사한 기능을 제공합니다.
 * GeoTools 라이브러리를 사용하여 EPSG 코드 기반의 좌표 변환을 수행합니다.</p>
 */
public class SDO_Transform extends ST_GeometryAccessor {

    private static final Logger LOG = LoggerFactory.getLogger(SDO_Transform.class);

    /**
     * 공간 객체의 좌표 참조 시스템(CRS, Coordinate Reference System)을 다른 좌표계로 변환합니다.
     *
     * @param geom          ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @param sourceCrsCode 원본 좌표계 EPSG 코드 (예: 'EPSG:4326')
     * @param targetCrsCode 대상 좌표계 EPSG 코드 (예: 'EPSG:3857')
     * @return 변환된 ESRI Shape 형식의 기하 데이터 (BytesWritable)
     */
    public BytesWritable evaluate(BytesWritable geom, String sourceCrsCode, String targetCrsCode) {
        if (geom == null || geom.getLength() == 0 || sourceCrsCode == null || targetCrsCode == null) {
            return null;
        }

        try {
            // 1. ESRI Shape -> OGCGeometry
            com.esri.core.geometry.ogc.OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
            if (ogcGeom == null) {
                return null;
            }

            // 4. 변환된 Geometry를 OGCGeometry로 변환 및 SRID 설정
//            System.setProperty("org.geotools.referencing.forceXY", "false");
//            System.clearProperty("org.geotools.referencing.forceXY");
//            System.out.println("forceXY = " + System.getProperty("org.geotools.referencing.forceXY"));
            int sourceSrid = 0;
            int targetSrid = 0;
            try {
                String[] sourceSplit = sourceCrsCode.split(":");
                if (sourceSplit.length > 1) {
                    sourceSrid = Integer.parseInt(sourceSplit[1]);
                    if (sourceSrid == 4326) {
                        System.setProperty("org.geotools.referencing.forceXY", "true");// 좌표계 변환시 xy 가 뒤집혀야 함
                    }
                }

                String[] split = targetCrsCode.split(":");
                if (split.length > 1) {
                    targetSrid = Integer.parseInt(split[1]);
                    if (targetSrid == 4326) {
                        System.setProperty("org.geotools.referencing.forceXY", "true");// 좌표계 변환시 xy 가 뒤집혀야 함
                    }
                }
            } catch (Exception ignore) {
            }

            // 2. GeoTools를 이용한 좌표계 변환 객체 생성
            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceCrsCode);
            CoordinateReferenceSystem targetCRS = CRS.decode(targetCrsCode);

            // lenient=true: 버사 변환(Bursa-Wolf) 파라미터가 없어도 변환 허용
            boolean lenient = true;
            MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);

            // 3. ESRI Geometry 좌표 변환 수행
            com.esri.core.geometry.Geometry esriGeom = ogcGeom.getEsriGeometry();
            com.esri.core.geometry.Geometry transformedEsriGeom = transformEsriGeometry(esriGeom, transform);

            com.esri.core.geometry.SpatialReference targetSR = targetSrid != 0 ? com.esri.core.geometry.SpatialReference.create(targetSrid) : null;
            com.esri.core.geometry.ogc.OGCGeometry ogcResult = com.esri.core.geometry.ogc.OGCGeometry.createFromEsriGeometry(transformedEsriGeom, targetSR);

            return GeometryUtils.geometryToEsriShapeBytesWritable(ogcResult);

        } catch (Exception e) {
            LOG.error("Error in SDO_Transform: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * ESRI Geometry 객체의 모든 좌표를 주어진 MathTransform을 사용하여 변환합니다.
     */
    private com.esri.core.geometry.Geometry transformEsriGeometry(com.esri.core.geometry.Geometry geom, MathTransform transform) throws Exception {
        com.esri.core.geometry.Geometry resultGeom = geom.copy();
        if (resultGeom instanceof com.esri.core.geometry.Point) {
            com.esri.core.geometry.Point pt = (com.esri.core.geometry.Point) resultGeom;
            double[] src = new double[]{pt.getX(), pt.getY()};
            double[] dst = new double[2];
            transform.transform(src, 0, dst, 0, 1);
            pt.setXY(dst[0], dst[1]);
        } else if (resultGeom instanceof com.esri.core.geometry.MultiVertexGeometry) {
            com.esri.core.geometry.MultiVertexGeometry mvGeom = (com.esri.core.geometry.MultiVertexGeometry) resultGeom;
            int pointCount = mvGeom.getPointCount();
            for (int i = 0; i < pointCount; i++) {
                com.esri.core.geometry.Point pt = mvGeom.getPoint(i);
                double[] src = new double[]{pt.getX(), pt.getY()};
                double[] dst = new double[2];
                transform.transform(src, 0, dst, 0, 1);
                mvGeom.setPoint(i, new com.esri.core.geometry.Point(dst[0], dst[1]));
            }
        }
        return resultGeom;
    }
}
