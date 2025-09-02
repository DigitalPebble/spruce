// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.*;

public class OperationalEmissionsTest {

    private OperationalEmissions module = new OperationalEmissions();

    // ENERGY_USED, CARBON_INTENSITY, PUE, OPERATIONAL_EMISSIONS
    private StructType schema = Utils.getSchema(module);

    @Test
    void processNoValues() {
        Object[] values = new Object[] {null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = module.process(row);
        // missing values comes back as it was
        assertEquals(row, enriched);
    }

    @Test
    void processNoPUE() {
        Object[] values = new Object[] {10d, 321.04d, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = module.process(row);
        double expected = 10 * 321.04;
        double result = OPERATIONAL_EMISSIONS.getDouble(enriched);
        assertEquals(expected, result);
    }

    @Test
    void processWithPUE() {
        Object[] values = new Object[] {10d, 321.04d, 1.15, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = module.process(row);
        double expected = 10 * 321.04 * 1.15;
        double result = OPERATIONAL_EMISSIONS.getDouble(enriched);
        assertEquals(expected, result);
    }

}