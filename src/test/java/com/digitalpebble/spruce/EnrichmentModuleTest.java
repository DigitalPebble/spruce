// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.*;

class EnrichmentModuleTest {

    private Row baseRow;

    @BeforeEach
    void setUp() {
        StructField[] fields = new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField(ENERGY_USED.getLabel(), DataTypes.DoubleType, true),
                DataTypes.createStructField("note", DataTypes.StringType, true)
        };
        StructType schema = new StructType(fields);
        baseRow = new GenericRowWithSchema(new Object[]{1, 10.0, "initial"}, schema);
    }

    @Test
    void withUpdatedValue() {
        // replace value
        Row replaced = EnrichmentModule.withUpdatedValue(baseRow, ENERGY_USED, 5.0, false);
        assertEquals(5.0, replaced.getDouble(replaced.fieldIndex(ENERGY_USED.getLabel())), 0.0001);

        // add to existing value
        Row added = EnrichmentModule.withUpdatedValue(baseRow, ENERGY_USED, 2.5, true);
        assertEquals(12.5, added.getDouble(added.fieldIndex(ENERGY_USED.getLabel())), 0.0001);
    }

    @Test
    void testWithUpdatedValue() {
        // using the simple setter that infers replacement (non-Double add overload)
        Row updated = EnrichmentModule.withUpdatedValue(baseRow, ENERGY_USED, 42.0);
        assertEquals(42.0, updated.getDouble(updated.fieldIndex(ENERGY_USED.getLabel())), 0.0001);

        // update non-numeric field (should simply set new value)
        Column noteCol = new ProxyColumn("note", DataTypes.StringType);

        Row noteUpdated = EnrichmentModule.withUpdatedValue(baseRow, noteCol, "updated");
        assertEquals("updated", noteUpdated.getString(noteUpdated.fieldIndex("note")));
    }

    @Test
    void withUpdatedValues() {
        java.util.Map<Column, Object> updates = new java.util.HashMap<>();
        updates.put(ENERGY_USED, 7.5);
        updates.put(new ProxyColumn("note", DataTypes.StringType), "changed");

        Row updated = EnrichmentModule.withUpdatedValues(baseRow, updates);

        assertEquals(1, updated.getInt(updated.fieldIndex("id")));
        assertEquals(7.5, updated.getDouble(updated.fieldIndex(ENERGY_USED.getLabel())), 0.0001);
        assertEquals("changed", updated.getString(updated.fieldIndex("note")));
    }

    static class ProxyColumn extends Column {
        ProxyColumn(String l, DataType t) {
            super(l, t);
        }
    }
}