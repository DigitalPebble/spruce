// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PUETest {

    private PUE pue;
    private StructType schema;

    @BeforeEach
    void setUp() {
        pue = new PUE();
        pue.init(new HashMap<>());
        schema = Utils.getSchema(pue);
    }

    @Test
    void processNoValues() {
        Object[] values = new Object[] {null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = pue.process(row);
        assertEquals(row, enriched);
    }

    @Test
    void processValuesDefault() {
        Object[] values = new Object[] {10d, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = pue.process(row);
        assertEquals(1.15, enriched.getDouble(2), 0.001);
    }

    @Test
    void processCustomConfiguration() {
        // Verify that a custom default value passed via config is respected
        PUE customPue = new PUE();
        Map<String, Object> config = new HashMap<>();
        config.put("default", 2.5);
        customPue.init(config);

        // Pass a non-existent region to trigger the fallback
        Object[] values = new Object[] {10d, "Mars-Region", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = customPue.process(row);

        assertEquals(2.5, enriched.getDouble(2), 0.001);
    }

    @Test
    void processRealRegionFromCSV() {
        // Verify exact match lookup from the loaded CSV (e.g. us-east-1 -> 1.15)
        Object[] values = new Object[] {100d, "us-east-1", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = pue.process(row);

        assertEquals(1.15, enriched.getDouble(2), 0.001);
    }

    @Test
    void processRegexRegionFromCSV() {
        // Verify regex pattern matching (e.g. us-west-custom matches "us-.*" -> 1.15)
        Object[] values = new Object[] {100d, "us-west-custom", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = pue.process(row);

        assertEquals(1.15, enriched.getDouble(2), 0.001);
    }
}