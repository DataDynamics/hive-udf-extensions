package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 객체를 구성하는 모든 정점(Vertex)을 테이블 행(Row) 형태로 반환한다
 */
public class SDO_GetVertices extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("SDO_GetVertices() takes exactly one argument.");
        }

        // 출력 컬럼 정의: ID, X, Y
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldNames.add("x");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("y");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (args[0] == null) return;

        String wkt = args[0].toString();
        // WKT에서 숫자와 콤마, 공백만 남기고 제거
        String cleanWkt = wkt.replaceAll("[a-zA-Z\\(\\)]", "").trim();
        String[] points = cleanWkt.split(",");

        for (int i = 0; i < points.length; i++) {
            String[] coords = points[i].trim().split("\\s+");
            if (coords.length >= 2) {
                Object[] row = new Object[3];
                row[0] = i + 1;         // ID
                row[1] = coords[0];     // X
                row[2] = coords[1];     // Y
                forward(row);
            }
        }
    }

    @Override
    public void close() throws HiveException {
        // 종료 로직 불필요
    }
}