// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link Utils#getStringFromProductMap(Row, String, String)}.
 * Verifies both the standard {@code scala.collection.Map} representation
 * and the {@code Seq<Row>} (Parquet key-value array) representation
 * that Spark 4.x may produce for MapType columns.
 */
public class UtilsGetStringFromProductMapTest {

    private static final StructType SCHEMA = new StructType(new StructField[]{
            StructField.apply("product",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                    true, null)
    });

    /** Build a row whose product column holds a scala.collection.Map. */
    private static Row rowWithScalaMap(Map<String, String> map) {
        Object scalaMap = map != null ? JavaConverters.mapAsScalaMapConverter(map).asScala() : null;
        return new GenericRowWithSchema(new Object[]{scalaMap}, SCHEMA);
    }

    /** Build a row whose product column holds an ArraySeq of key-value Rows (Parquet representation). */
    private static Row rowWithSeq(String[][] entries) {
        if (entries == null) {
            return new GenericRowWithSchema(new Object[]{null}, SCHEMA);
        }
        Row[] rows = new Row[entries.length];
        for (int i = 0; i < entries.length; i++) {
            rows[i] = RowFactory.create(entries[i][0], entries[i][1]);
        }
        scala.collection.Seq<Row> seq = scala.collection.mutable.ArraySeq.make(rows);
        return new GenericRowWithSchema(new Object[]{seq}, SCHEMA);
    }

    // --- scala.collection.Map tests ---

    @Test
    void scalaMap_returnsValue() {
        Row row = rowWithScalaMap(Map.of("model", "claude-v2", "region", "us-east-1"));
        assertEquals("claude-v2", Utils.getStringFromProductMap(row, "model", null));
        assertEquals("us-east-1", Utils.getStringFromProductMap(row, "region", null));
    }

    @Test
    void scalaMap_missingKeyReturnsDefault() {
        Row row = rowWithScalaMap(Map.of("model", "claude-v2"));
        assertNull(Utils.getStringFromProductMap(row, "missing", null));
        assertEquals("fallback", Utils.getStringFromProductMap(row, "missing", "fallback"));
    }

    @Test
    void scalaMap_emptyMapReturnsDefault() {
        Row row = rowWithScalaMap(Map.of());
        assertEquals("def", Utils.getStringFromProductMap(row, "model", "def"));
    }

    // --- Seq<Row> (Parquet array) tests ---

    @Test
    void seq_returnsValue() {
        Row row = rowWithSeq(new String[][]{{"model", "claude-v2"}, {"region", "us-east-1"}});
        assertEquals("claude-v2", Utils.getStringFromProductMap(row, "model", null));
        assertEquals("us-east-1", Utils.getStringFromProductMap(row, "region", null));
    }

    @Test
    void seq_missingKeyReturnsDefault() {
        Row row = rowWithSeq(new String[][]{{"model", "claude-v2"}});
        assertNull(Utils.getStringFromProductMap(row, "missing", null));
        assertEquals("fallback", Utils.getStringFromProductMap(row, "missing", "fallback"));
    }

    @Test
    void seq_emptySeqReturnsDefault() {
        Row row = rowWithSeq(new String[][]{});
        assertEquals("def", Utils.getStringFromProductMap(row, "model", "def"));
    }

    // --- null column tests ---

    @Test
    void nullColumn_returnsDefault() {
        Row row = rowWithScalaMap(null);
        assertNull(Utils.getStringFromProductMap(row, "model", null));
        assertEquals("def", Utils.getStringFromProductMap(row, "model", "def"));
    }
}
