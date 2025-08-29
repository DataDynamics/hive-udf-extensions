package io.datadynamics.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.DoubleWritable;

/**
 * safe_divide(a, b) :  b = 0 이거나 NULL이면 NULL을 반환하고, 아니면 a / b 결과를 DOUBLE로 반환합니다.
 * 문자열/정수/실수/DECIMAL 등 원시 타입(Primitive)은 내부적으로 DOUBLE로 변환하여 처리합니다.
 */
@Description(
        name = "safe_divide",
        value = "_FUNC_(a, b) - Returns a / b as DOUBLE; returns NULL when b is 0/NULL or on conversion error.",
        extended = "Example:\n"
                + "  > SELECT safe_divide(10, 2);     -- 5.0\n"
                + "  > SELECT safe_divide(10, 0);     -- NULL\n"
                + "  > SELECT safe_divide('9', '3');  -- 3.0\n"
                + "  > SELECT safe_divide(1, NULL);   -- NULL\n"
                + "  > SELECT safe_divide(1.0, 1e-400); -- NULL (underflow/overflow/NaN 방지)"
)
public class GenericUDFSafeDivide extends GenericUDF {

    private transient PrimitiveObjectInspector leftOI;
    private transient PrimitiveObjectInspector rightOI;
    private final DoubleWritable result = new DoubleWritable();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("safe_divide(a, b) takes exactly 2 arguments");
        }

        // 두 인자는 Primitive 여야 합니다(숫자/문자열/DECIMAL 등).
        if (arguments[0].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "First argument must be a primitive type");
        }
        if (arguments[1].getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1, "Second argument must be a primitive type");
        }

        leftOI = (PrimitiveObjectInspector) arguments[0];
        rightOI = (PrimitiveObjectInspector) arguments[1];

        // 허용 가능한 Primitive 카테고리인지(숫자류/DECIMAL/STRING) 최소한의 검증
        if (!isNumericLike(leftOI.getPrimitiveCategory())) {
            throw new UDFArgumentTypeException(0, "First argument must be numeric/decimal/string");
        }

        if (!isNumericLike(rightOI.getPrimitiveCategory())) {
            throw new UDFArgumentTypeException(1, "Second argument must be numeric/decimal/string");
        }

        // 항상 DOUBLE 반환(OI)
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    private boolean isNumericLike(PrimitiveCategory cat) {
        switch (cat) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case STRING:   // 문자열도 숫자로 파싱 시도 (실패 시 NULL 처리)
            case CHAR:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object aObj = arguments[0].get();
        Object bObj = arguments[1].get();

        if (aObj == null || bObj == null) {
            return null;
        }

        try {
            final double denom = PrimitiveObjectInspectorUtils.getDouble(bObj, rightOI);
            // 분모 0 또는 -0.0
            if (denom == 0.0d) {
                return null;
            }

            final double numer = PrimitiveObjectInspectorUtils.getDouble(aObj, leftOI);
            final double value = numer / denom;

            // Inf/NaN 방지: 이런 경우도 NULL을 반환하여 "안전" 보장
            if (!Double.isFinite(value)) {
                return null;
            }

            result.set(value);
            return result;

        } catch (Exception e) {
            // 숫자 변환 실패/기타 런타임 예외는 NULL로 흡수(Presto try(...) 유사 동작)
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        if (children == null || children.length != 2) {
            return "safe_divide(a, b)";
        }
        return "safe_divide(" + children[0] + ", " + children[1] + ")";
    }
}