// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class ColumnTest {

    @Test
    void testGetLabelAndType() {
        CURColumn col = new CURColumn("field1", DataTypes.StringType);
        assertEquals("field1", col.getLabel());
        assertEquals(DataTypes.StringType, col.getType());
    }

    @Test
    void testGetStringAndIsNullAt() {
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
    void testResolveIndexOptionalCachesMisses() {
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
    void testResolveIndexIsIndependentBetweenThreads() throws Exception {
        CURColumn col = new CURColumn("target", DataTypes.StringType);
        int threads = 8;
        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Void>> results = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int expected = t;
                results.add(pool.submit(() -> {
                    // the column sits at a different position in the schema of each thread
                    StructField[] fields = new StructField[expected + 1];
                    for (int i = 0; i < expected; i++) {
                        fields[i] = new StructField("other" + i, DataTypes.StringType, true, Metadata.empty());
                    }
                    fields[expected] = new StructField("target", DataTypes.StringType, true, Metadata.empty());
                    Row row = new GenericRowWithSchema(new Object[expected + 1], new StructType(fields));
                    start.await();
                    for (int i = 0; i < 200_000; i++) {
                        assertEquals(expected, col.resolveIndex(row));
                    }
                    return null;
                }));
            }
            for (Future<Void> result : results) {
                result.get();
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void testResolveIndexAfterSerialization() throws Exception {
        StructType schema = new StructType(new StructField[]{new StructField("afield", DataTypes.StringType, true, Metadata.empty())});
        Row row = new GenericRowWithSchema(new Object[]{"x"}, schema);
        CURColumn col = new CURColumn("afield", DataTypes.StringType);
        assertEquals(0, col.resolveIndex(row));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(col);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            CURColumn copy = (CURColumn) in.readObject();
            assertEquals(0, copy.resolveIndex(row));
        }
    }

    @Test
    void testGetDoubleAndIsNullAt() {
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
