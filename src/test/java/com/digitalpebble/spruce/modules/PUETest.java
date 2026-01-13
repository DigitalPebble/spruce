// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
    void processCustomConfiguration() {
        PUE customPue = new PUE();
        Map<String, Object> config = new HashMap<>();
        config.put("default", 2.5);
        customPue.init(config);

        Object[] values = new Object[] {10d, "Mars-Region", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = customPue.process(row);

        assertEquals(2.5, SpruceColumn.PUE.getDouble(enriched), 0.001);
    }

    @ParameterizedTest
    @CsvSource({
        "100, , 1.15",                    // No region -> default
        "100, unknown-region, 1.15",      // Unknown region -> default
        "100, us-east-1, 1.15",           // Exact match
        "100, eu-west-1, 1.11",           // Exact match
        "100, us-gov-west-1, 1.14",       // Regex match (us-.+)
        "100, eu-central-2, 1.11"         // Regex match (eu-.+)
    })
    void processRegionPUEValues(double energyUsed, String region, double expectedPUE) {
        Object[] values = new Object[] {energyUsed, region, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = pue.process(row);

        assertEquals(expectedPUE, SpruceColumn.PUE.getDouble(enriched), 0.001, "Failed for region: " + region);
    }
}