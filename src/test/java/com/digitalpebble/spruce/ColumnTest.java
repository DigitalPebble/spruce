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
        TestColumn col = new TestColumn("field1", DataTypes.StringType);
        assertEquals("field1", col.getLabel());
        assertEquals(DataTypes.StringType, col.getType());
    }

    @Test
    public void testGetStringAndIsNullAt() {
        final String name = "sfield";
        StructType schema = new StructType(new StructField[]{new StructField(name, DataTypes.StringType, true, Metadata.empty())});

        // non-null value
        Row r1 = new GenericRowWithSchema(new Object[]{"hello"}, schema);

        TestColumn col = new TestColumn(name, DataTypes.StringType);
        assertFalse(col.isNullAt(r1));
        assertEquals("hello", col.getString(r1));

        // null value
        Row r2 = new GenericRowWithSchema(new Object[]{null}, schema);
        assertTrue(col.isNullAt(r2));
        assertNull(col.getString(r2));

        // ask for a field not in the schema but allow it
        TestColumn col2 = new TestColumn("unknown", DataTypes.StringType);
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
    public void testGetDoubleAndIsNullAt() {
        final String name = "dfield";
        StructType schema = new StructType(new StructField[]{new StructField(name, DataTypes.DoubleType, true, Metadata.empty())});

        // non-null double
        Row r1 = new GenericRowWithSchema(new Object[]{3.14159}, schema);
        TestColumn col = new TestColumn(name, DataTypes.DoubleType);
        assertFalse(col.isNullAt(r1));
        assertEquals(3.14159, col.getDouble(r1), 1e-9);

        // null double
        Row r2 = new GenericRowWithSchema(new Object[]{null}, schema);
        assertTrue(col.isNullAt(r2));
    }

    // minimal concrete implementation for testing
    static class TestColumn extends Column {
        TestColumn(String l, DataType t) {
            super(l, t);
        }
    }
}

