// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.azure;

import com.digitalpebble.spruce.AzureFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link BoaviztAPIstatic} with the FOCUS column binding, using the bundled static
 * CSV; the shared extraction and lookup logic is covered in {@link AbstractBoaviztaAzureTest}.
 */
class BoaviztAPIstaticTest {

    private BoaviztAPIstatic module;
    private StructType schema;

    @BeforeEach
    void setUp() {
        module = new BoaviztAPIstatic();
        module.bindReportFormat(ReportFormat.FOCUS);
        module.init(new HashMap<>());
        schema = Utils.getSchema(module);
    }

    @Test
    void columnsNeededReflectsFocusColumns() {
        assertArrayEquals(new Column[]{
                AzureFOCUSColumn.X_SKU_METER_CATEGORY,
                AzureFOCUSColumn.X_SKU_METER_NAME,
                AzureFOCUSColumn.X_PRICING_UNIT_DESCRIPTION,
                FOCUSColumn.CONSUMED_QUANTITY
        }, module.columnsNeeded());
    }

    @Test
    void virtualMachineRowIsEnriched() {
        Map<Column, Object> enriched = enrich("Virtual Machines", "D2ads v5", "1 Hour", 24d);
        assertNotNull(enriched.get(ENERGY_USED));
    }

    @Test
    void rowWithoutQuantityIsSkipped() {
        Map<Column, Object> enriched = enrich("Virtual Machines", "D2ads v5", "1 Hour", null);
        assertTrue(enriched.isEmpty());
    }

    private Map<Column, Object> enrich(String meterCategory, String meterName,
                                       String unitOfMeasure, Double quantity) {
        Object[] values = new Object[]{
                meterCategory, meterName, unitOfMeasure, quantity,
                null, null, null
        };
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);
        return enriched;
    }
}
