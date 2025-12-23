// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ServerlessTest {

    private final Serverless serverless = new Serverless();
    private final StructType schema = Utils.getSchema(serverless);

    @Test
    void processEmptyRow() {
        Row row = generateRow(null, null, null);
        Row enriched = serverless.process(row);
        assertEquals(row, enriched);
    }

    @Test
    void processFargateMemory() {
        double quantity = 10d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-GB-Hours");
        Row enriched = serverless.process(row);
        double expected = serverless.memory_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    @Test
    void processFargatevCPU() {
        double quantity = 4d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-vCPU-Hours:perCPU");
        Row enriched = serverless.process(row);
        double expected = serverless.x86_cpu_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    @Test
    void processFargatevCPUARM() {
        double quantity = 4d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-ARM-vCPU-Hours:perCPU");
        Row enriched = serverless.process(row);
        double expected = serverless.arm_cpu_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    @Test
    void processEMRvCPUARM() {
        double quantity = 4d;
        Row row = generateRow("Worker", quantity, "xxx-EMR-SERVERLESS-ARM-vCPUHours");
        Row enriched = serverless.process(row);
        double expected = serverless.arm_cpu_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    @Test
    void processEMRvCPU() {
        double quantity = 4d;
        Row row = generateRow("Worker", quantity, "xx-EMR-SERVERLESS-vCPUHours");
        Row enriched = serverless.process(row);
        double expected = serverless.x86_cpu_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    @Test
    void processEMRMemory() {
        double quantity = 10d;
        Row row = generateRow("Worker", quantity, "EUN1-EMR-SERVERLESS-ARM-MemoryGBHours");
        Row enriched = serverless.process(row);
        double expected = serverless.memory_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    private Row generateRow(String LINE_ITEM_OPERATION, Object USAGE_AMOUNT, String LINE_ITEM_USAGE_TYPE){
        Object[] values = new Object[] {LINE_ITEM_OPERATION, USAGE_AMOUNT, LINE_ITEM_USAGE_TYPE, null};
        return new GenericRowWithSchema(values, schema);
    }
}