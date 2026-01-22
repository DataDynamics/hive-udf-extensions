package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import io.datadynamics.hive.udf.esri.GeometryUtils;
import io.datadynamics.hive.udf.esri.LogUtils;
import io.datadynamics.hive.udf.esri.ST_GeometryAccessor;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 두 기하학 객체를 단순히 하나의 객체로 합친다.
 *
 * @see <a href="https://github.com/apache/hive/tree/master/ql/src/java/org/apache/hadoop/hive/ql/udf/esri">Hive ESRI UDF</a>
 */
public class SDO_Append extends ST_GeometryAccessor {

    static final Logger LOG = LoggerFactory.getLogger(SDO_Append.class.getName());

    private transient BinaryObjectInspector g1OI;
    private transient BinaryObjectInspector g2OI;

    public BytesWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2) {

        if (geometryref1 == null || geometryref2 == null
                || geometryref1.getLength() == 0 || geometryref2.getLength() == 0) {
            LogUtils.Log_ArgumentsNull(LOG);
            return null;
        }

        if (!GeometryUtils.compareSpatialReferences(geometryref1, geometryref2)) {
            LogUtils.Log_SRIDMismatch(LOG, geometryref1, geometryref2);
            return null;
        }

        OGCGeometry ogcGeom1 = GeometryUtils.geometryFromEsriShape(geometryref1);
        OGCGeometry ogcGeom2 = GeometryUtils.geometryFromEsriShape(geometryref2);
        if (ogcGeom1 == null || ogcGeom2 == null) {
            LogUtils.Log_ArgumentsNull(LOG);
            return null;
        }

        Polyline polyline = new Polyline();
        if (ogcGeom1.getEsriGeometry() instanceof Polyline) {
            polyline.add((Polyline) ogcGeom1.getEsriGeometry(), false);
        }
        if (ogcGeom2.getEsriGeometry() instanceof Polyline) {
            polyline.add((Polyline) ogcGeom2.getEsriGeometry(), false);
        }

        OGCMultiLineString multiLineString = new OGCMultiLineString(polyline, ogcGeom1.esriSR);
        return GeometryUtils.geometryToEsriShapeBytesWritable(multiLineString);
    }

}