package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SDO_GetVerticesTest {

    private SDO_GetVertices udtf;

    @Before
    public void setUp() throws Exception {
        udtf = new SDO_GetVertices();
        ObjectInspector[] arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };
        udtf.initialize(arguments);
    }

    @Test
    public void testProcess_LineString() throws HiveException {
        final List<Object[]> results = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                results.add((Object[]) input);
            }
        });

        String wkt = "LINESTRING (0 0, 10 10, 20 25)";
        udtf.process(new Object[]{wkt});

        assertEquals(3, results.size());

        // Point 1
        assertEquals(1, results.get(0)[0]);
        assertEquals("0", results.get(0)[1]);
        assertEquals("0", results.get(0)[2]);

        // Point 2
        assertEquals(2, results.get(1)[0]);
        assertEquals("10", results.get(1)[1]);
        assertEquals("10", results.get(1)[2]);

        // Point 3
        assertEquals(3, results.get(2)[0]);
        assertEquals("20", results.get(2)[1]);
        assertEquals("25", results.get(2)[2]);
    }

    @Test
    public void testProcess_Point() throws HiveException {
        final List<Object[]> results = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                results.add((Object[]) input);
            }
        });

        String wkt = "POINT (5 10)";
        udtf.process(new Object[]{wkt});

        assertEquals(1, results.size());
        assertEquals(1, results.get(0)[0]);
        assertEquals("5", results.get(0)[1]);
        assertEquals("10", results.get(0)[2]);
    }

    @Test
    public void testProcess_Polygon() throws HiveException {
        final List<Object[]> results = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                results.add((Object[]) input);
            }
        });

        String wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
        udtf.process(new Object[]{wkt});

        // POLYGON ((...)) 에서 괄호와 문자가 제거되면 "0 0, 10 0, 10 10, 0 10, 0 0"이 됨
        assertEquals(5, results.size());
        assertEquals(1, results.get(0)[0]);
        assertEquals("0", results.get(0)[1]);
        assertEquals("0", results.get(0)[2]);
    }

    @Test
    public void testProcess_NullInput() throws HiveException {
        final List<Object[]> results = new ArrayList<>();
        udtf.setCollector(new Collector() {
            @Override
            public void collect(Object input) throws HiveException {
                results.add((Object[]) input);
            }
        });

        udtf.process(new Object[]{null});
        assertTrue(results.isEmpty());
    }
}
