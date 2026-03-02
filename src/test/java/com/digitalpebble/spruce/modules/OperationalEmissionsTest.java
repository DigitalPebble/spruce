// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.*;

public class OperationalEmissionsTest {

    private OperationalEmissions module = new OperationalEmissions();

    // ENERGY_USED, CARBON_INTENSITY, PUE, OPERATIONAL_EMISSIONS
    private StructType schema = Utils.getSchema(module);

    @Test
    void processNoValues() {
        Object[] values = new Object[] {null, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);
        // missing values - map should not contain OPERATIONAL_EMISSIONS
        assertFalse(enriched.containsKey(OPERATIONAL_EMISSIONS));
    }

    @Test
    void processNoPUE() {
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 10d);
        enriched.put(CARBON_INTENSITY, 321.04d);
        module.enrich(row, enriched);
        double expected = 10 * 321.04 * 1.04 * 1.08;
        assertEquals(expected, (Double) enriched.get(OPERATIONAL_EMISSIONS), 0.0001);
    }

    @Test
    void processWithPUE() {
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 10d);
        enriched.put(CARBON_INTENSITY, 321.04d);
        enriched.put(PUE, 1.15);
        module.enrich(row, enriched);
        double expected = 10 * 321.04 * 1.15 * 1.04 * 1.08;
        assertEquals(expected, (Double) enriched.get(OPERATIONAL_EMISSIONS), 0.0001);
    }
}
