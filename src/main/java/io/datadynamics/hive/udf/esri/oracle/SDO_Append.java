package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.LogUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 두 개의 기하학 객체(Geometry)를 하나의 OGCMultiLineString으로 결합하는 Hive UDF입니다.
 * 주로 Oracle의 SDO_UTIL.APPEND 기능을 Hive에서 에뮬레이션하기 위해 사용됩니다.
 * 현재 구현은 입력받은 기하학 객체들 중 Polyline(LineString, MultiLineString) 타입인 것들을 모아서
 * 하나의 MultiLineString으로 반환합니다.
 *
 * @see <a href="https://github.com/apache/hive/tree/master/ql/src/java/org/apache/hadoop/hive/ql/udf/esri">Hive ESRI UDF</a>
 */
public class SDO_Append extends ST_GeometryAccessor {

    static final Logger LOG = LoggerFactory.getLogger(SDO_Append.class.getName());

    /**
     * 두 개의 ESRI Shape 형식의 기하학 데이터를 입력받아 결합된 MultiLineString을 반환합니다.
     *
     * @param geom1 첫 번째 기하학 데이터 (BytesWritable)
     * @param geom2 두 번째 기하학 데이터 (BytesWritable)
     * @return 결합된 MultiLineString을 포함하는 BytesWritable. 오류 발생 시 null 반환.
     */
    public BytesWritable evaluate(BytesWritable geom1, BytesWritable geom2) {

        // 입력 데이터 유효성 검사: null 이거나 길이가 0인 경우 처리
        if (geom1 == null || geom2 == null
                || geom1.getLength() == 0 || geom2.getLength() == 0) {
            LogUtils.Log_ArgumentsNull(LOG);
            return null;
        }

        // 공간 참조(Spatial Reference, SRID) 일치 여부 확인
        if (!GeometryUtils.compareSpatialReferences(geom1, geom2)) {
            LogUtils.Log_SRIDMismatch(LOG, geom1, geom2);
            return null;
        }

        // ESRI Shape 바이너리 데이터를 OGCGeometry 객체로 변환
        OGCGeometry ogcGeom1 = GeometryUtils.geometryFromEsriShape(geom1);
        OGCGeometry ogcGeom2 = GeometryUtils.geometryFromEsriShape(geom2);
        if (ogcGeom1 == null || ogcGeom2 == null) {
            LogUtils.Log_ArgumentsNull(LOG);
            return null;
        }

        // 결합된 경로를 담을 새로운 Polyline 객체 생성
        Polyline polyline = new Polyline();

        // 첫 번째 기하학 구조가 Polyline(LineString/MultiLineString)인 경우 결과에 추가
        if (ogcGeom1.getEsriGeometry() instanceof Polyline) {
            polyline.add((Polyline) ogcGeom1.getEsriGeometry(), false);
        }

        // 두 번째 기하학 구조가 Polyline(LineString/MultiLineString)인 경우 결과에 추가
        if (ogcGeom2.getEsriGeometry() instanceof Polyline) {
            polyline.add((Polyline) ogcGeom2.getEsriGeometry(), false);
        }

        // 결합된 Polyline과 원본 공간 참조를 사용하여 OGCMultiLineString 생성
        OGCMultiLineString multiLineString = new OGCMultiLineString(polyline, ogcGeom1.esriSR);

        // 최종 OGC 객체를 다시 Hive에서 사용할 수 있는 ESRI Shape 바이너리(BytesWritable)로 변환하여 반환
        return GeometryUtils.geometryToEsriShapeBytesWritable(multiLineString);
    }

}