/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara.modules.ccf;

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