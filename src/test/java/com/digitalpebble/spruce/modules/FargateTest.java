// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FargateTest {

    private final Fargate fargate = new Fargate();
    private final StructType schema = Utils.getSchema(fargate);

    @Test
    void processEmptyRow() {
        Row row = generateRow(null, null, null);
        Row enriched = fargate.process(row);
        assertEquals(row, enriched);
    }

    @Test
    void processMemory() {
        double quantity = 10d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-GB-Hours");
        Row enriched = fargate.process(row);
        double expected = fargate.memory_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    @Test
    void processvCPU() {
        double quantity = 4d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-vCPU-Hours:perCPU");
        Row enriched = fargate.process(row);
        double expected = fargate.x86_cpu_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    @Test
    void processvCPUARM() {
        double quantity = 4d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-ARM-vCPU-Hours:perCPU");
        Row enriched = fargate.process(row);
        double expected = fargate.arm_cpu_coefficient_kwh * quantity;
        assertEquals(expected, ENERGY_USED.getDouble(enriched));
    }

    private Row generateRow(String LINE_ITEM_OPERATION, Object USAGE_AMOUNT, String LINE_ITEM_USAGE_TYPE){
        Object[] values = new Object[] {LINE_ITEM_OPERATION, USAGE_AMOUNT, LINE_ITEM_USAGE_TYPE, null};
        return new GenericRowWithSchema(values, schema);
    }
}