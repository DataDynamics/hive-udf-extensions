package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.LogUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 표준 기하 구조를 LRS(Linear Referencing System) 기하 구조(M 값이 포함된 형태)로 변환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_LRS.CONVERT_TO_LRS_GEOM 함수와 유사한 기능을 제공합니다.
 * 입력된 기하 구조에 M(Measure) 차원을 추가하고, 선의 길이를 기반으로 M 값을 할당합니다.</p>
 */
public class SDO_ConvertToLrsGeom extends ST_GeometryAccessor {

    private static final Logger LOG = LoggerFactory.getLogger(SDO_ConvertToLrsGeom.class);

    /**
     * 입력 기하 구조를 LRS 기하 구조로 변환합니다. M 값은 0부터 선의 전체 길이까지 할당됩니다.
     *
     * @param geom ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @return LRS화된 기하 데이터를 포함하는 BytesWritable. 오류 시 null.
     */
    public BytesWritable evaluate(BytesWritable geom) {
        return evaluate(geom, 0.0, null);
    }

    /**
     * 입력 기하 구조를 LRS 기하 구조로 변환합니다. M 값은 지정된 startM부터 endM까지 할당됩니다.
     *
     * @param geom   ESRI Shape 형식의 기하 데이터 (BytesWritable)
     * @param startM 시작 Measure 값
     * @param endM   종료 Measure 값 (null인 경우 선의 길이를 기준으로 자동 계산)
     * @return LRS화된 기하 데이터를 포함하는 BytesWritable. 오류 시 null.
     */
    public BytesWritable evaluate(BytesWritable geom, Double startM, Double endM) {
        if (geom == null || geom.getLength() == 0) {
            return null;
        }

        try {
            OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
            if (ogcGeom == null) {
                return null;
            }

            Geometry esriGeom = ogcGeom.getEsriGeometry();
            if (!(esriGeom instanceof Polyline)) {
                LOG.warn("Input geometry is not a Polyline: {}", ogcGeom.geometryType());
                return null;
            }

            Polyline polyline = (Polyline) esriGeom;
            double totalLength = polyline.calculateLength2D();
            double actualStartM = (startM != null) ? startM : 0.0;
            double actualEndM = (endM != null) ? endM : actualStartM + totalLength;

            Polyline lrsPolyline = convertToLrsPolyline(polyline, actualStartM, actualEndM);

            OGCGeometry resultGeom;
            if (ogcGeom instanceof OGCLineString) {
                resultGeom = new OGCLineString(lrsPolyline, 0, ogcGeom.esriSR);
            } else if (ogcGeom instanceof OGCMultiLineString) {
                resultGeom = new OGCMultiLineString(lrsPolyline, ogcGeom.esriSR);
            } else {
                // 기본적으로 Polyline 기반의 적절한 OGC 타입 생성
                resultGeom = OGCGeometry.createFromEsriGeometry(lrsPolyline, ogcGeom.esriSR);
            }

            return GeometryUtils.geometryToEsriShapeBytesWritable(resultGeom);

        } catch (Exception e) {
            LogUtils.Log_ExceptionThrown(LOG, "SDO_ConvertToLrsGeom", e);
            return null;
        }
    }

    private Polyline convertToLrsPolyline(Polyline polyline, double startM, double endM) {
        Polyline result = new Polyline();
        // Polyline에 직접적인 setHasM 메서드가 없을 수 있으므로,
        // 첫 번째 포인트를 추가할 때 M 값이 있으면 자동으로 HasM이 설정되거나,
        // 생성된 Geometry 객체의 속성으로 관리될 수 있습니다.

        double totalLength = polyline.calculateLength2D();
        double mRange = endM - startM;

        int pathCount = polyline.getPathCount();
        double currentLength = 0;

        for (int i = 0; i < pathCount; i++) {
            int start = polyline.getPathStart(i);
            int pointCount = polyline.getPathSize(i);

            if (pointCount == 0) continue;

            for (int j = 0; j < pointCount; j++) {
                Point p = polyline.getPoint(start + j);
                if (j > 0) {
                    Point prevP = polyline.getPoint(start + j - 1);
                    currentLength += Math.sqrt(Math.pow(p.getX() - prevP.getX(), 2) + Math.pow(p.getY() - prevP.getY(), 2));
                }

                double m;
                if (totalLength == 0) {
                    m = startM;
                } else {
                    m = startM + (currentLength / totalLength) * mRange;
                }

                // 새로운 Point 객체를 생성하여 M 값을 설정합니다.
                Point newPt = new Point();
                p.copyTo(newPt);
                newPt.setM(m);

                if (j == 0) {
                    result.startPath(newPt);
                } else {
                    result.lineTo(newPt);
                }
            }
        }

        return result;
    }
}
