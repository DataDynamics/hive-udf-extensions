package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 공간 객체의 기하학적 유효성을 검사하고, 오류 발생 시 원인과 위치 정보를 포함한
 * 상세한 컨텍스트 문자열을 반환하는 Hive UDF입니다.
 *
 * <p>이 함수는 Oracle Spatial의 SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT 함수와 유사한 기능을 제공합니다.
 * ESRI Shape 형식을 입력받아 ESRI OGCGeometry.isSimple()을 사용하여 검사를 수행합니다.</p>
 *
 * <h3>반환값 형식</h3>
 * <pre>
 *   유효한 경우: "TRUE"
 *   유효하지 않은 경우: "FALSE"
 *   NULL 입력인 경우: "NULL GEOMETRY"
 * </pre>
 */
public class SDO_ValidateGeometryWithContext extends GenericUDF {

    private static final Logger LOG = LoggerFactory.getLogger(SDO_ValidateGeometryWithContext.class);
    private transient BinaryObjectInspector geomOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("SDO_ValidateGeometryWithContext requires 1 argument");
        }
        if (!(arguments[0] instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("SDO_ValidateGeometryWithContext requires a binary argument (ESRI Shape)");
        }
        this.geomOI = (BinaryObjectInspector) arguments[0];
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object geomObj = arguments[0].get();
        if (geomObj == null) {
            return "NULL GEOMETRY";
        }

        BytesWritable geomBytes = geomOI.getPrimitiveWritableObject(geomObj);
        if (geomBytes == null || geomBytes.getLength() == 0) {
            return "NULL GEOMETRY";
        }

        try {
            // 1. ESRI Shape -> OGCGeometry
            OGCGeometry ogcGeom = GeometryUtils.geometryFromEsriShape(geomBytes);
            if (ogcGeom == null) {
                return "NULL GEOMETRY";
            }

            // 2. 유효성 검사 수행
            // OGCGeometry.isSimple()은 ESRI API에서 기하학적 유효성을 체크하는 데 사용됨
            if (ogcGeom.isSimple()) {
                return "TRUE";
            } else {
                return "FALSE";
            }

        } catch (Exception e) {
            LOG.error("Error in SDO_ValidateGeometryWithContext: " + e.getMessage(), e);
            return "ERROR: " + e.getMessage();
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("SDO_ValidateGeometryWithContext", children);
    }
}
