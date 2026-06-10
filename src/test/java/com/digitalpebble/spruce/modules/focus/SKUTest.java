// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.focus;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.*;

class SKUTest {

    private SKU sku;
    private StructType schema;

    @BeforeEach
    void setUp() {
        sku = new SKU();
        sku.init(new HashMap<>());
        schema = Utils.getSchema(sku);
    }

    @Test
    void columnsNeeded() {
        Column[] columns = sku.columnsNeeded();
        assertEquals(1, columns.length);
        assertEquals(FOCUSColumn.SKU_ID, columns[0]);
    }

    @Test
    void columnsAdded() {
        Column[] columns = sku.columnsAdded();
        assertEquals(3, columns.length);
        assertTrue(columns[0] == ENERGY_USED || columns[1] == ENERGY_USED || columns[2] == ENERGY_USED);
        assertTrue(columns[0] == EMBODIED_EMISSIONS || columns[1] == EMBODIED_EMISSIONS || columns[2] == EMBODIED_EMISSIONS);
        assertTrue(columns[0] == EMBODIED_ADP || columns[1] == EMBODIED_ADP || columns[2] == EMBODIED_ADP);
    }

    @Test
    void enrichWithNullSKU() {
        Object[] values = new Object[]{null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        sku.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
        assertFalse(enriched.containsKey(EMBODIED_EMISSIONS));
        assertFalse(enriched.containsKey(EMBODIED_ADP));
    }

    @Test
    void enrichWithUnknownSKU() {
        Object[] values = new Object[]{"UNKNOWN_SKU_12345"};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        sku.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
        assertFalse(enriched.containsKey(EMBODIED_EMISSIONS));
        assertFalse(enriched.containsKey(EMBODIED_ADP));
    }

    @Test
    void enrichWithValidSKU() {
        Object[] values = new Object[]{"ZH8KU2QB7FHAJJXW"};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        sku.enrich(row, enriched);
        assertTrue(enriched.containsKey(ENERGY_USED));
        assertTrue(enriched.containsKey(EMBODIED_EMISSIONS));
        assertTrue(enriched.containsKey(EMBODIED_ADP));
        assertEquals(0.01444, (Double) enriched.get(ENERGY_USED), 0.0001);
        assertEquals(0.0, (Double) enriched.get(EMBODIED_EMISSIONS), 0.0001);
        assertEquals(0.0, (Double) enriched.get(EMBODIED_ADP), 0.0001);
    }
}
