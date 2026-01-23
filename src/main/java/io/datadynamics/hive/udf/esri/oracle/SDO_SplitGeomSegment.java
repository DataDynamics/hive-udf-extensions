package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * LRS(Linear Referencing System) 선형 객체를 지정된 Measure(거리) 값을 기준으로
 * 두 개의 세그먼트로 분할하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_LRS.SPLIT_GEOM_SEGMENT 함수와 유사한 기능을 제공합니다.
 * 도로, 파이프라인, 철도 등 선형 네트워크에서 특정 지점을 기준으로 구간을 분리할 때 사용합니다.</p>
 *
 * <pre>
 *   선형 객체 (LINESTRING)
 *
 *   Start                    Split Point                      End
 *     ●━━━━━━━━━━━━━━━━━━━━━━━━━●━━━━━━━━━━━━━━━━━━━━━━━━━━━━━●
 *     |                         |                              |
 *   M=0                      M=50                           M=100
 *     |←─────── Segment 1 ─────→|←─────── Segment 2 ──────────→|
 *
 *   결과: GeometryCollection[Segment1, Segment2]
 * </pre>
 *
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/SDO_LRS-reference.html">Oracle SDO_LRS.SPLIT_GEOM_SEGMENT</a>
 */
public class SDO_SplitGeomSegment extends ST_GeometryAccessor {

    private static final Logger LOG = LoggerFactory.getLogger(SDO_SplitGeomSegment.class);

    /**
     * 선형 객체를 지정된 Measure 값을 기준으로 분할합니다.
     *
     * @param geom         ESRI Shape 형식의 선형 기하 데이터 (BytesWritable)
     * @param splitMeasure 분할 기준 거리 (M값)
     * @return 분할된 두 세그먼트를 포함하는 GeometryCollection (BytesWritable)
     */
    public BytesWritable evaluate(BytesWritable geom, Double splitMeasure) {
        if (geom == null || geom.getLength() == 0 || splitMeasure == null) {
            return null;
        }

        try {
            // 1. ESRI Shape -> OGCGeometry
            OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geom);
            if (ogcGeom == null) {
                return null;
            }

            com.esri.core.geometry.Geometry esriGeom = ogcGeom.getEsriGeometry();
            if (!(esriGeom instanceof Polyline)) {
                LOG.warn("Input geometry is not a Polyline: {}", ogcGeom.geometryType());
                return null;
            }

            Polyline polyline = (Polyline) esriGeom;
            double totalLength = polyline.calculateLength2D();

            // 분할 지점 유효성 검증
            if (splitMeasure < 0 || splitMeasure > totalLength) {
                LOG.warn("Split measure {} is out of range [0, {}]", splitMeasure, totalLength);
                return null;
            }

            // 2. 선형 객체 분할 수행
            List<Polyline> segments = splitPolylineAtDistance(polyline, splitMeasure);
            if (segments.size() < 2) {
                return null;
            }

            // 3. MultiLineString으로 병합
            Polyline resultPolyline = new Polyline();
            for (Polyline segment : segments) {
                resultPolyline.add(segment, false);
            }

            OGCMultiLineString ogcResult = new OGCMultiLineString(resultPolyline, ogcGeom.esriSR);

            return GeometryUtils.geometryToEsriShapeBytesWritable(ogcResult);

        } catch (Exception e) {
            LOG.error("Error in SDO_SplitGeomSegment: " + e.getMessage(), e);
            return null;
        }
    }

    private List<Polyline> splitPolylineAtDistance(Polyline polyline, double splitMeasure) {
        List<Polyline> result = new ArrayList<>();
        int pathCount = polyline.getPathCount();

        // 단순화를 위해 첫 번째 Path만 처리하거나, 전체 길이를 기준으로 해당 Path를 찾음
        // 여기서는 전체 길이를 기준으로 splitMeasure가 위치한 Path와 Point를 찾음
        double currentLength = 0;
        Polyline left = new Polyline();
        Polyline right = new Polyline();
        boolean splitDone = false;

        for (int i = 0; i < pathCount; i++) {
            int start = polyline.getPathStart(i);
            int end = polyline.getPathEnd(i);
            int pointCount = polyline.getPathSize(i);

            if (pointCount < 2) continue;

            if (splitDone) {
                // 이미 분할이 끝났으므로 나머지는 모두 오른쪽에 추가
                right.addPath(polyline, i, false);
                continue;
            }

            double pathLength = 0;
            // Path 내부에서 분할 지점 찾기
            int splitSegmentIndex = -1;
            double lengthAtSplitStart = 0;

            for (int j = 0; j < pointCount - 1; j++) {
                Point p1 = polyline.getPoint(start + j);
                Point p2 = polyline.getPoint(start + j + 1);
                double segmentLength = Math.sqrt(Math.pow(p2.getX() - p1.getX(), 2) + Math.pow(p2.getY() - p1.getY(), 2));

                if (currentLength + pathLength + segmentLength >= splitMeasure && splitSegmentIndex == -1) {
                    splitSegmentIndex = j;
                    lengthAtSplitStart = currentLength + pathLength;
                }
                pathLength += segmentLength;
            }

            if (splitSegmentIndex != -1) {
                // 이 Path에서 분할 발생
                double ratio = (splitMeasure - lengthAtSplitStart) / getSegmentLength(polyline, start + splitSegmentIndex);
                Point splitPoint = interpolate(polyline.getPoint(start + splitSegmentIndex), polyline.getPoint(start + splitSegmentIndex + 1), ratio);

                // Left Path 생성
                left.startPath(polyline.getPoint(start));
                for (int j = 1; j <= splitSegmentIndex; j++) {
                    left.lineTo(polyline.getPoint(start + j));
                }
                left.lineTo(splitPoint);

                // Right Path 생성
                right.startPath(splitPoint);
                for (int j = splitSegmentIndex + 1; j < pointCount; j++) {
                    right.lineTo(polyline.getPoint(start + j));
                }
                splitDone = true;
            } else {
                // 분할 지점이 아직 안 나옴
                left.addPath(polyline, i, false);
                currentLength += pathLength;
            }
        }

        result.add(left);
        result.add(right);
        return result;
    }

    private double getSegmentLength(Polyline polyline, int startPointIndex) {
        Point p1 = polyline.getPoint(startPointIndex);
        Point p2 = polyline.getPoint(startPointIndex + 1);
        return Math.sqrt(Math.pow(p2.getX() - p1.getX(), 2) + Math.pow(p2.getY() - p1.getY(), 2));
    }

    private Point interpolate(Point p1, Point p2, double ratio) {
        if (ratio <= 0) return p1;
        if (ratio >= 1) return p2;
        double x = p1.getX() + (p2.getX() - p1.getX()) * ratio;
        double y = p1.getY() + (p2.getY() - p1.getY()) * ratio;
        return new Point(x, y);
    }
}
