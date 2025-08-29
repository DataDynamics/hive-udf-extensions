package io.datadynamics.hive.udf.custom;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

import java.util.ArrayList;
import java.util.List;

/**
 * array_filter_not_null(array<T>) -> array<T>
 * <p>
 * - 입력 배열의 NULL 원소를 제거합니다.
 * - 엘리먼트 타입 T는 Primitive/Struct/Map/List 모두 지원합니다.
 * - 입력이 NULL이면 NULL, 빈 배열이면 빈 배열을 반환합니다.
 * <p>
 * 예)
 * array_filter_not_null(array(1, NULL, 3)) -> [1,3]
 * array_filter_not_null(array(named_struct('a',1), NULL)) -> [{a:1}]
 */
@Description(
        name = "array_filter_not_null",
        value = "_FUNC_(arr) - Returns arr with NULL elements removed.",
        extended = "Example:\n"
                + "  > SELECT array_filter_not_null(array(1, NULL, 3));  -- [1,3]\n"
                + "  > SELECT array_filter_not_null(array('a', NULL));   -- ['a']\n"
)
public class GenericUDFArrayFilterNotNull extends GenericUDF {

    private transient ListObjectInspector listOI;
    private transient ObjectInspector elemOI;
    private transient ObjectInspector elemStdJavaOI;
    private transient ListObjectInspector returnOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("array_filter_not_null(arr) takes exactly 1 argument");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0, "Argument 1 must be an ARRAY");
        }

        listOI = (ListObjectInspector) arguments[0];
        elemOI = listOI.getListElementObjectInspector();

        // 반환: 표준 Java 객체 기반의 array<T>
        elemStdJavaOI = ObjectInspectorUtils.getStandardObjectInspector(elemOI, ObjectInspectorCopyOption.JAVA);
        returnOI = ObjectInspectorFactory.getStandardListObjectInspector(elemStdJavaOI);

        return returnOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object arrObj = arguments[0].get();
        if (arrObj == null) {
            return null; // 입력이 NULL이면 NULL
        }

        int len = listOI.getListLength(arrObj);
        if (len <= 0) {
            return new ArrayList<>(0); // 빈 배열
        }

        List<Object> out = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            Object elem = listOI.getListElement(arrObj, i);
            if (elem != null) {
                // 표준 Java 객체로 복사(Primitive/Struct/Map/List 모두 안전)
                Object std = ObjectInspectorUtils.copyToStandardObject(elem, elemOI, ObjectInspectorCopyOption.JAVA);
                out.add(std);
            }
        }
        return out;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "array_filter_not_null(" + (children != null && children.length > 0 ? children[0] : "") + ")";
    }
}