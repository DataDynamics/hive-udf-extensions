package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.locationtech.jts.algorithm.hull.ConcaveHull;
import org.locationtech.jts.geom.Geometry;

/**
 * 점 집합이나 형상을 감싸는 오목한(Concave) 경계 다각형을 생성한다. tolerance 파라미터를 사용하여 오목함의 정도를 제어한다.
 */
public class SDO_ConcaveHull_String extends GenericUDF {

    private transient StringObjectInspector wktOI;
    private transient DoubleObjectInspector toleranceOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("SDO_ConcaveHull_String requires 2 arguments");
        }
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("SDO_ConcaveHull_String requires a string argument as the first parameter");
        }
        if (!(arguments[1] instanceof DoubleObjectInspector)) {
            throw new UDFArgumentException("SDO_ConcaveHull_String requires a double argument as the second parameter");
        }
        this.wktOI = (StringObjectInspector) arguments[0];
        this.toleranceOI = (DoubleObjectInspector) arguments[1];
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String wkt = wktOI.getPrimitiveJavaObject(arguments[0].get());
        Object toleranceObj = arguments[1].get();
        Double tolerance = (toleranceObj == null) ? null : toleranceOI.get(toleranceObj);

        Geometry geom = GeometryUtils.stringToGeometry(wkt);

        if (geom == null) return null;

        ConcaveHull hull = new ConcaveHull(geom);
        // Oracle tolerance가 null이면 JTS 자동 계산 로직 따름
        if (tolerance != null && tolerance > 0) {
            hull.setMaximumEdgeLength(tolerance);
        }

        Geometry result = hull.getHull();
        return GeometryUtils.geometryToString(result);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("SDO_ConcaveHull_String", children);
    }

}
