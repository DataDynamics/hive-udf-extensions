package io.datadynamics.hive.udf.array;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UDFImpalaArrayMaxTest {
    private UDFImpalaArrayMax udf;

    @Before
    public void setup() throws UDFArgumentException {
        udf = new UDFImpalaArrayMax();
        ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] arguments = {valueOI};
        udf.initialize(arguments);
    }

    @Test
    public void testNull() throws Exception {
        // Try with null args
        GenericUDF.DeferredObject[] nullArgs = {new GenericUDF.DeferredJavaObject(null)};
        Text output = (Text) udf.evaluate(nullArgs);
        assertEquals("array_max(null) test", new Text("null"), output);
    }

    @Test
    public void testEmpty() throws Exception {
        GenericUDF.DeferredObject[] nullArgs = {new GenericUDF.DeferredJavaObject("[]")};
        Text output = (Text) udf.evaluate(nullArgs);
        assertEquals("array_max([]) test", new Text("null"), output);
    }

    @Test
    public void testArrayMaxInt() throws Exception {
        GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject("[3, 1, 2]");
        GenericUDF.DeferredObject[] args = {valueObj};
        Text output = (Text) udf.evaluate(args);
        assertEquals("array_max(int[]) test", new Text("3"), output);
    }

    @Test
    public void testArrayMaxDouble() throws Exception {
        GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject("[1.1,2.2,3.3]");
        GenericUDF.DeferredObject[] args = {valueObj};
        Text output = (Text) udf.evaluate(args);
        assertEquals("array_max(double[]) test", new Text("3.3"), output);
    }

    @Test
    public void testArrayMaxString() throws Exception {
        GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject("[\"11.1\",\"2.2\",\"3.3\"]");
        GenericUDF.DeferredObject[] args = {valueObj};
        Text output = (Text) udf.evaluate(args);
        assertEquals("array_max(string[]) test", new Text("3.3"), output);
    }

    @Test
    public void testArrayMaxInvalid() throws Exception {
        GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject("[\"11.1\",,\"3.3\"]");
        GenericUDF.DeferredObject[] args = {valueObj};
        Text output = (Text) udf.evaluate(args);
        assertEquals("array_max(invalid) test", new Text("null"), output);
    }
}