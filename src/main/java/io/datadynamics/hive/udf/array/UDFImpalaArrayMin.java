package io.datadynamics.hive.udf.array;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.List;

import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

@Description(name = "array_min"
        , value = "_FUNC_(string) - returns the minimum value of input(json array)."
        , extended = "Example:\n > select _FUNC_(string) from src;")
public class UDFImpalaArrayMin extends GenericUDF {
    private static final int ARG_COUNT = 1; // Number of arguments to this UDF
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Text TEXT_NULL = new Text("null");

    private transient StringObjectInspector inputOI;

    public UDFImpalaArrayMin() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if two arguments were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentLengthException(
                    "The function array_min(array) takes exactly " + ARG_COUNT + "arguments.");
        }

        // Check if two argument is of category LIST
        ObjectInspector.Category category = arguments[0].getCategory();
        String typeName = arguments[0].getTypeName();
        if (!category.equals(ObjectInspector.Category.PRIMITIVE) || !typeName.equals(STRING_TYPE_NAME)) {
            throw new UDFArgumentTypeException(0,
                    "\"" + STRING_TYPE_NAME + "\" "
                            + "expected at function array_min, but "
                            + "\"" + typeName + "\" "
                            + "is found");
        }

        inputOI = (StringObjectInspector) arguments[0];
        return inputOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object obj = arguments[0].get();
        String jsonString = inputOI.getPrimitiveJavaObject(obj);
        List<Comparable<Object>> list = null;
        try {
            list = OBJECT_MAPPER.readValue(jsonString, List.class);
        } catch (Exception e) {
            return TEXT_NULL;
        }

        int size = list.size();
        if (size == 0) {
            return TEXT_NULL;
        }
        if (size == 1) {
            return list.get(0);
        }
        Comparable<Object> objectComparable = list.stream().
                min(
                        (o1, o2) -> ComparisonChain.start().compare(o1, o2).result()
                ).get();
        return new Text(objectComparable.toString());
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array_min(" + strings[0] + ")";
    }
}
