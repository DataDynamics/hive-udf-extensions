package io.datadynamics.hive.udf.esri.oracle;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import io.datadynamics.hive.udf.esri.hive.GeometryUtils;
import io.datadynamics.hive.udf.esri.hive.ST_GeometryAccessor;
import org.apache.hadoop.io.BytesWritable;

import java.util.List;

public class SDO_Geometry extends ST_GeometryAccessor {

    public BytesWritable evaluate(Double gType, Double srid, BytesWritable pointBytes, List<Double> elemInfoArray, List<Double> ordinates) {
        if (gType == null || gType.isNaN()) {
            return null;
        }
        OGCGeometry pointGeom = GeometryUtils.geometryFromEsriShape(pointBytes);
        if (pointGeom != null && !(pointGeom instanceof OGCPoint)) {
            return null;
        }
//        OGCPoint point = (OGCPoint) pointGeom;

        int type = gType.intValue();
//        int sridInt = (srid != null) ? srid.intValue() : 4326; // 기본값 설정
        StringBuilder sb = new StringBuilder();
        OGCGeometry writeObj = null;
        switch (type) {
            case 2001:
                // 2차원 POINT
            case 3001:
                // 3차원 POINT
                writeObj = pointGeom;
//                return GeometryUtils.geometryToEsriShapeBytesWritable(pointGeom);
                break;
            case 2002:
                // LINESTRING
                sb.append("LINESTRING (");
                this.writeOrdinates(ordinates, sb);
                sb.append(")");
                writeObj = OGCGeometry.fromText(sb.toString());
                break;
            case 2003:
                // POLYGON
                sb.append("POLYGON ((");
                this.writeOrdinates(ordinates, sb);
                sb.append("))");
                writeObj = OGCGeometry.fromText(sb.toString());
                break;
        }
        return GeometryUtils.geometryToEsriShapeBytesWritable(writeObj);
    }

    private void writeOrdinates(List<Double> ordinates, StringBuilder sb) {
        int size = ordinates.size();
        for (int i = 0; i < size; i += 2) {
            sb.append(ordinates.get(i)).append(" ").append(ordinates.get(i + 1));
            if (i < size - 2) {
                sb.append(", ");
            }
        }
    }
}
