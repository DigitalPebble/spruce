// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that modules correctly write to and read from the enrichedValues map.
 */
class EnrichmentModuleTest {

    @Test
    void enrichWritesToMap() {
        // A minimal module that writes a value
        EnrichmentModule module = new EnrichmentModule() {
            @Override
            public Column[] columnsNeeded() { return new Column[0]; }
            @Override
            public Column[] columnsAdded() { return new Column[]{ENERGY_USED}; }
            @Override
            public void enrich(Row inputRow, Map<Column, Object> enrichedValues) {
                enrichedValues.put(ENERGY_USED, 42.0);
            }
        };

        StructType schema = Utils.getSchema(module);
        Row row = new GenericRowWithSchema(new Object[]{null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertEquals(42.0, enriched.get(ENERGY_USED));
    }

    @Test
    void enrichAddsToExistingValue() {
        // Simulates the Accelerators add-to-existing pattern
        EnrichmentModule module = new EnrichmentModule() {
            @Override
            public Column[] columnsNeeded() { return new Column[0]; }
            @Override
            public Column[] columnsAdded() { return new Column[]{ENERGY_USED}; }
            @Override
            public void enrich(Row inputRow, Map<Column, Object> enrichedValues) {
                Double existing = (Double) enrichedValues.get(ENERGY_USED);
                double newVal = 2.5;
                enrichedValues.put(ENERGY_USED, (existing != null ? existing : 0.0) + newVal);
            }
        };

        StructType schema = Utils.getSchema(module);
        Row row = new GenericRowWithSchema(new Object[]{null}, schema);

        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 10.0);
        module.enrich(row, enriched);

        assertEquals(12.5, enriched.get(ENERGY_USED));
    }

    @Test
    void enrichReadsFromMap() {
        // Simulates a downstream module reading a value set by an earlier module
        Column noteCol = new ProxyColumn("note", DataTypes.StringType);

        EnrichmentModule module = new EnrichmentModule() {
            @Override
            public Column[] columnsNeeded() { return new Column[]{ENERGY_USED}; }
            @Override
            public Column[] columnsAdded() { return new Column[]{noteCol}; }
            @Override
            public void enrich(Row inputRow, Map<Column, Object> enrichedValues) {
                Object val = enrichedValues.get(ENERGY_USED);
                if (val != null) {
                    enrichedValues.put(noteCol, "energy=" + val);
                }
            }
        };

        StructType schema = Utils.getSchema(module);
        Row row = new GenericRowWithSchema(new Object[]{null, null}, schema);

        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 7.5);
        module.enrich(row, enriched);

        assertEquals("energy=7.5", enriched.get(noteCol));
    }

    static class ProxyColumn extends Column {
        ProxyColumn(String l, DataType t) {
            super(l, t);
        }
    }
}
