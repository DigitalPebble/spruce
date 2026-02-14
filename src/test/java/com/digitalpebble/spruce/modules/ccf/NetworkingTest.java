// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.*;

class NetworkingTest {

    private Networking networking = new Networking();
    private StructType schema = Utils.getSchema(networking);

    @Test
    void processNoValues() {
        Object[] values = new Object[] {null, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    @Test
    void processValues() {
        Map<String, String> product = new HashMap<>();
        product.put("transfer_type", "InterRegion");
        Object[] values = new Object[] {"AWSDataTransfer", JavaConverters.asScala(product), 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        double expected = networking.network_coefficient * 10;
        assertEquals(0.01d, (Double) enriched.get(ENERGY_USED));
    }
}
