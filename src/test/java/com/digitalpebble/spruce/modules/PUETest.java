// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
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

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
        Row row = new GenericRowWithSchema(new Object[]{null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        pue.enrich(row, enriched);
        assertFalse(enriched.containsKey(SpruceColumn.PUE));
    }

    @Test
    void processCustomConfiguration() {
        PUE customPue = new PUE();
        Map<String, Object> config = new HashMap<>();
        config.put("default", 2.5);
        customPue.init(config);

        Row row = new GenericRowWithSchema(new Object[]{null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 10d);
        enriched.put(REGION, "Mars-Region");
        customPue.enrich(row, enriched);

        assertEquals(2.5, (Double) enriched.get(SpruceColumn.PUE), 0.001);
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
        Row row = new GenericRowWithSchema(new Object[]{null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, energyUsed);
        if (region != null) {
            enriched.put(REGION, region);
        }
        pue.enrich(row, enriched);

        assertEquals(expectedPUE, (Double) enriched.get(SpruceColumn.PUE), 0.001, "Failed for region: " + region);
    }
}
