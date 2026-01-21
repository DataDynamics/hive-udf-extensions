package io.datadynamics.hive.udf.geospatial;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SDO_ValidateGeometryWithContextTest {

    private SDO_ValidateGeometryWithContext udf;
    private GeometryFactory factory;
    private ObjectInspector[] arguments;

    @Before
    public void setUp() throws Exception {
        udf = new SDO_ValidateGeometryWithContext();
        factory = new GeometryFactory();
        arguments = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
        };
        udf.initialize(arguments);
    }

    @Test
    public void testEvaluate_Valid() throws HiveException {
        Polygon poly = factory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(10, 0),
                new Coordinate(10, 10),
                new Coordinate(0, 10),
                new Coordinate(0, 0)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(poly);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes)
        };

        String result = (String) udf.evaluate(deferredObjects);
        assertEquals("TRUE", result);
    }

    @Test
    public void testEvaluate_Invalid_SelfIntersection() throws HiveException {
        // Self-intersecting polygon (Figure-8)
        Polygon poly = factory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(10, 10),
                new Coordinate(10, 0),
                new Coordinate(0, 10),
                new Coordinate(0, 0)
        });
        BytesWritable geomBytes = GeometryUtils.geometryToBytes(poly);

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(geomBytes)
        };

        String result = (String) udf.evaluate(deferredObjects);
        assertTrue(result.startsWith("FALSE:"));
        assertTrue(result.contains("Self-intersection") || result.contains("Ring Self-intersection"));
    }

    @Test
    public void testEvaluate_Null() throws HiveException {
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(null)
        };

        String result = (String) udf.evaluate(deferredObjects);
        assertEquals("NULL GEOMETRY", result);
    }
}
