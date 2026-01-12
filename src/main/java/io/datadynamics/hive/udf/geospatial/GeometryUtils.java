package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.io.BytesWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/**
 * Impala BINARY (WKB)와 JTS Geometry 간의 변환을 담당하는 유틸리티
 */
public class GeometryUtils {

    // 2D 및 3D 좌표 지원, ByteOrder 보존
    private static final WKBReader wkbReader = new WKBReader();

    // Oracle LRS 데이터 처리를 위해 3차원(Measure 포함) 지원 활성화 필요 가능성 있음
    private static final WKBWriter wkbWriter = new WKBWriter(3, true);

    private static final WKTReader wktReader = new WKTReader();

    private static final WKTWriter wktWriter = new WKTWriter(3);

    public static Geometry bytesToGeometry(BytesWritable wkb) {
        if (wkb == null || wkb.getLength() == 0) return null;
        try {
            // copyBytes()를 사용하여 안전하게 바이트 배열 추출
            return wkbReader.read(wkb.copyBytes());
        } catch (Exception e) {
            // 데이터 오류 시 null 반환하여 쿼리 중단 방지
            return null;
        }
    }

    public static BytesWritable geometryToBytes(Geometry geom) {
        if (geom == null) return null;
        byte[] bytes = wkbWriter.write(geom);
        return new BytesWritable(bytes);
    }

    public static Geometry stringToGeometry(String wkt) {
        if (wkt == null || wkt.isEmpty()) return null;
        try {
            return wktReader.read(wkt);
        } catch (Exception e) {
            // 데이터 오류 시 null 반환하여 쿼리 중단 방지
            return null;
        }
    }

    public static String geometryToString(Geometry geom) {
        if (geom == null) return null;
        return wktWriter.write(geom);
    }
}