package io.datadynamics.hive.udf.custom;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * map_get_or_default(map<K,V>, key, default)
 * <p>
 * - map에서 key를 안전하게 조회(오류/형변환 실패 시 NULL 처리)하고,
 * 값이 NULL이면 default를 반환합니다.
 * - Presto의 COALESCE(TRY(map['key']), default)와 동일 목적.
 * <p>
 * 사용 예:
 * SELECT map_get_or_default(YOUR_COLUMN, 'UNITNM', 'None');
 */
@Description(
        name = "map_get_or_default",
        value = "_FUNC_(map, key, default) - Safely returns map[key] or default when missing/NULL.",
        extended = "Examples:\n"
                + "  > SELECT map_get_or_default(map('UNITNM','KG'), 'UNITNM', 'None'); -- 'KG'\n"
                + "  > SELECT map_get_or_default(map(), 'UNITNM', 'None');              -- 'None'\n"
                + "  > SELECT map_get_or_default(NULL, 'UNITNM', 'None');               -- 'None'\n"
)
public class GenericUDFMapGetOrDefault extends GenericUDF {

    private transient MapObjectInspector mapOI;
    private transient ObjectInspector keyArgOI;
    private transient ObjectInspector defaultArgOI;

    private transient PrimitiveObjectInspector mapKeyPIO;
    private transient PrimitiveObjectInspector mapValPIO;

    private transient ObjectInspector keyTargetJavaOI;   // map key의 Java OI
    private transient ObjectInspector returnJavaOI;      // map value의 Java OI (반환 타입)

    private transient ObjectInspectorConverters.Converter keyConverter;      // key -> map key 타입
    private transient ObjectInspectorConverters.Converter valueConverter;    // map value -> return 타입(Java)
    private transient ObjectInspectorConverters.Converter defaultConverter;  // default arg -> return 타입(Java)

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 3) {
            throw new UDFArgumentLengthException("map_get_or_default(map<K,V>, key, default) requires exactly 3 arguments");
        }

        // arg0: map<K,V>
        if (arguments[0].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentTypeException(0, "First argument must be a MAP");
        }
        mapOI = (MapObjectInspector) arguments[0];

        // key/value OI from map
        if (mapOI.getMapKeyObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Map key must be a primitive type");
        }
        if (mapOI.getMapValueObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Map value must be a primitive type");
        }
        mapKeyPIO = (PrimitiveObjectInspector) mapOI.getMapKeyObjectInspector();
        mapValPIO = (PrimitiveObjectInspector) mapOI.getMapValueObjectInspector();

        // arg1: key (임의 타입이지만 map key 타입으로 변환 가능해야 함)
        keyArgOI = arguments[1];

        // arg2: default (임의 타입이지만 map value 타입으로 변환 가능해야 함)
        defaultArgOI = arguments[2];

        // 반환 OI: map value의 Java OI
        PrimitiveTypeInfo valTypeInfo = (PrimitiveTypeInfo) mapValPIO.getTypeInfo();
        returnJavaOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(valTypeInfo);

        // key 타깃 OI: map key의 Java OI
        PrimitiveTypeInfo keyTypeInfo = (PrimitiveTypeInfo) mapKeyPIO.getTypeInfo();
        keyTargetJavaOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(keyTypeInfo);

        // 컨버터 준비
        keyConverter = ObjectInspectorConverters.getConverter(keyArgOI, keyTargetJavaOI);
        valueConverter = ObjectInspectorConverters.getConverter(mapValPIO, returnJavaOI);
        defaultConverter = ObjectInspectorConverters.getConverter(defaultArgOI, returnJavaOI);

        return (ObjectInspector) returnJavaOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object mapObj = arguments[0].get();
        Object keyObj = arguments[1].get();
        Object defObj = arguments[2].get();

        // 기본값을 미리 변환
        Object defVal;
        try {
            defVal = defaultConverter.convert(defObj);
        } catch (Exception e) {
            // default 변환 자체가 실패하면 NULL 반환(Presto TRY 유사)
            defVal = null;
        }

        if (mapObj == null) {
            return defVal;
        }

        Object convKey;
        try {
            convKey = keyConverter.convert(keyObj);
        } catch (Exception e) {
            // key 변환 실패 -> default
            return defVal;
        }
        if (convKey == null) {
            return defVal;
        }

        try {
            // 맵 조회 (NULL/미존재면 null 반환)
            Object rawVal = mapOI.getMapValueElement(mapObj, convKey);
            if (rawVal == null) {
                return defVal;
            }
            // map value를 반환 타입(Java)으로 변환
            Object ret = valueConverter.convert(rawVal);
            return (ret != null) ? ret : defVal;
        } catch (Exception e) {
            // 조회 중 예외 발생 시 default (TRY 유사)
            return defVal;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "map_get_or_default(" + String.join(", ", children) + ")";
    }
}