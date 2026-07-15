// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.junit.Test;

import static org.junit.Assert.*;

public class ColumnTest {

    @Test
    public void testGetLabelAndType() {
        CURColumn col = new CURColumn("field1", DataTypes.StringType);
        assertEquals("field1", col.getLabel());
        assertEquals(DataTypes.StringType, col.getType());
    }

    @Test
    public void testGetStringAndIsNullAt() {
        final String name = "sfield";
        StructType schema = new StructType(new StructField[]{new StructField(name, DataTypes.StringType, true, Metadata.empty())});

        // non-null value
        Row r1 = new GenericRowWithSchema(new Object[]{"hello"}, schema);

        CURColumn col = new CURColumn(name, DataTypes.StringType);
        assertFalse(col.isNullAt(r1));
        assertEquals("hello", col.getString(r1));

        // null value
        Row r2 = new GenericRowWithSchema(new Object[]{null}, schema);
        assertTrue(col.isNullAt(r2));
        assertNull(col.getString(r2));

        // ask for a field not in the schema but allow it
        CURColumn col2 = new CURColumn("unknown", DataTypes.StringType);
        assertNull(col2.getString(r2, true));

        boolean exception = false;
        try {
            col2.getString(r2, false);
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);
    }

    @Test
    public void testResolveIndexOptionalCachesMisses() {
        StructType withField = new StructType(new StructField[]{new StructField("afield", DataTypes.StringType, true, Metadata.empty())});
        StructType withoutField = new StructType(new StructField[]{new StructField("other", DataTypes.StringType, true, Metadata.empty())});

        CURColumn col = new CURColumn("afield", DataTypes.StringType);

        // absent: -1 when optional, repeatedly (the miss is cached, no exception thrown)
        Row missing = new GenericRowWithSchema(new Object[]{"x"}, withoutField);
        assertEquals(-1, col.resolveIndex(missing, true));
        assertEquals(-1, col.resolveIndex(missing, true));

        // a cached miss still throws for the non-optional variant
        boolean exception = false;
        try {
            col.resolveIndex(missing);
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);

        // switching to a schema that has the column invalidates the cached miss
        Row present = new GenericRowWithSchema(new Object[]{"y"}, withField);
        assertEquals(0, col.resolveIndex(present, true));
        assertEquals(0, col.resolveIndex(present));
        assertEquals("y", col.getString(present, true));
    }

    @Test
    public void testGetDoubleAndIsNullAt() {
        final String name = "dfield";
        StructType schema = new StructType(new StructField[]{new StructField(name, DataTypes.DoubleType, true, Metadata.empty())});

        // non-null double
        Row r1 = new GenericRowWithSchema(new Object[]{3.14159}, schema);
        CURColumn col = new CURColumn(name, DataTypes.DoubleType);
        assertFalse(col.isNullAt(r1));
        assertEquals(3.14159, col.getDouble(r1), 1e-9);

        // null double
        Row r2 = new GenericRowWithSchema(new Object[]{null}, schema);
        assertTrue(col.isNullAt(r2));
    }
}
