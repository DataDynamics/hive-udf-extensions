package io.datadynamics.hive.udf.array;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UDFImpalaArrayMinTest {
    private UDFImpalaArrayMin udf;

    @Before
    public void setup() throws UDFArgumentException {
        udf = new UDFImpalaArrayMin();
        ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] arguments = {valueOI};
        udf.initialize(arguments);
    }

    @Test
    public void testNull() throws Exception {
        // Try with null args
        GenericUDF.DeferredObject[] nullArgs = {new GenericUDF.DeferredJavaObject(null)};
        Text output = (Text) udf.evaluate(nullArgs);
        assertEquals("array_min(null) test", new Text("null"), output);
    }

    @Test
    public void testEmpty() throws Exception {
        GenericUDF.DeferredObject[] nullArgs = {new GenericUDF.DeferredJavaObject("[]")};
        Text output = (Text) udf.evaluate(nullArgs);
        assertEquals("array_min([]) test", new Text("null"), output);
    }

    @Test
    public void testArrayMinInt() throws Exception {
        GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject("[3, 1, 2]");
        GenericUDF.DeferredObject[] args = {valueObj};
        Text output = (Text) udf.evaluate(args);
        assertEquals("array_min(int[]) test", new Text("1"), output);
    }

    @Test
    public void testArrayMinDouble() throws Exception {
        GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject("[1.1,2.2,3.3]");
        GenericUDF.DeferredObject[] args = {valueObj};
        Text output = (Text) udf.evaluate(args);
        assertEquals("array_min(double[]) test", new Text("1.1"), output);
    }

    @Test
    public void testArrayMinString() throws Exception {
        GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject("[\"11.1\",\"2.2\",\"3.3\"]");
        GenericUDF.DeferredObject[] args = {valueObj};
        Text output = (Text) udf.evaluate(args);
        assertEquals("array_min(string[]) test", new Text("11.1"), output);
    }

    @Test
    public void testArrayMinInvalid() throws Exception {
        GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject("[\"11.1\",,\"3.3\"]");
        GenericUDF.DeferredObject[] args = {valueObj};
        Text output = (Text) udf.evaluate(args);
        assertEquals("array_min(invalid) test", new Text("null"), output);
    }
}