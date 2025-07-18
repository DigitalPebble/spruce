// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NetworkingTest {

    private Networking networking = new Networking();

    @Test
    void processNoValues() {
        String ddl = "product_servicecode STRING, product MAP<STRING,STRING>, line_item_usage_amount DOUBLE";
        StructType schema =  StructType.fromDDL(ddl);
        Object[] values = new Object[] {null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = networking.process(row);
        // missing values comes back as it was
        assertEquals(row, enriched);
    }

    @Test
    void processValues() {
        String ddl = "product_servicecode STRING, product MAP<STRING,STRING>, line_item_usage_amount DOUBLE, energy_usage_kwh DOUBLE";
        StructType schema =  StructType.fromDDL(ddl);
        // 10 GBs of data transfer between regions
        Map<String, String> product = new HashMap<>();
        product.put("transfer_type", "InterRegion");
        ;
        Object[] values = new Object[] {"AWSDataTransfer", JavaConverters.asScala(product), 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = networking.process(row);
        // should have an estimated 0.01 kwh
        double expected = networking.network_coefficient * 10;
        assertEquals(0.01d, enriched.getDouble(3));
    }

}