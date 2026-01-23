package io.datadynamics.hive.udf.geospatial;

import com.esri.core.geometry.OperatorExportToWkb;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * 두 기하학 객체를 단순히 하나의 객체로 합친다.
 * 공간적 Union(합집합) 연산이 아니라, 데이터 구조적 병합(예: LineString + LineString = MultiLineString)을 의미한다.
 */
public class SDO_Append extends GenericUDF {

    private final GeometryFactory factory = new GeometryFactory();
    private final OperatorExportToWkb exporter = OperatorExportToWkb.local();
    private transient BinaryObjectInspector g1OI;
    private transient BinaryObjectInspector g2OI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("SDO_Append requires 2 arguments");
        }
        if (!(arguments[0] instanceof BinaryObjectInspector) || !(arguments[1] instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("SDO_Append requires 2 binary arguments");
        }
        this.g1OI = (BinaryObjectInspector) arguments[0];
        this.g2OI = (BinaryObjectInspector) arguments[1];
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object g1Obj = arguments[0].get();
        Object g2Obj = arguments[1].get();

        BytesWritable g1Bytes = g1OI.getPrimitiveWritableObject(g1Obj);
        System.out.println("g1Bytes = " + g1Bytes);
        BytesWritable g2Bytes = g2OI.getPrimitiveWritableObject(g2Obj);
        System.out.println("g2Bytes = " + g2Bytes);

        OGCGeometry ogcG1 = io.datadynamics.hive.udf.esri.hive.GeometryUtils.geometryFromEsriShape(g1Bytes);
        System.out.println("ogcG1 = " + ogcG1);
        OGCGeometry ogcG2 = io.datadynamics.hive.udf.esri.hive.GeometryUtils.geometryFromEsriShape(g2Bytes);
        System.out.println("ogcG2 = " + ogcG2);

        Geometry g1 = tryGetJTSGeom(ogcG1, g1Bytes);
        Geometry g2 = tryGetJTSGeom(ogcG2, g2Bytes);

        System.out.println("g1 = " + g1);
        System.out.println("g2 = " + g2);

        if (g1 == null) return g2Bytes;
        if (g2 == null) return g1Bytes;

        boolean isEsriG1 = ogcG1 != null;
        boolean isEsriG2 = ogcG2 != null;

        if (isEsriG1 && isEsriG2) {
            return io.datadynamics.hive.udf.esri.hive.GeometryUtils.geometryToEsriShapeBytesWritable(ogcG1.union(ogcG2));
        } else {
            // 두 객체를 배열로 묶어 Collection 생성
            Geometry[] geometries = new Geometry[]{g1, g2};

            // 더 정교한 구현을 위해서는 입력 타입(Polygon, LineString)을 확인하여
            // MultiPolygon, MultiLineString 등 구체적 타입으로 반환할 수 있음
            return GeometryUtils.geometryToBytes(factory.createGeometryCollection(geometries));
        }
    }

    private Geometry tryGetJTSGeom(OGCGeometry ogcGeom, BytesWritable w) {
        Geometry jtsGeom;
        if (ogcGeom == null) {// jts
            jtsGeom = GeometryUtils.bytesToGeometry(w);
        } else {// esri
            com.esri.core.geometry.Geometry esriGeom = ogcGeom.getEsriGeometry();
            byte[] wkb = exporter.execute(0, esriGeom, null).array();
            jtsGeom = GeometryUtils.bytesToGeometry(new BytesWritable(wkb));
        }
        return jtsGeom;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("SDO_Append", children);
    }

}