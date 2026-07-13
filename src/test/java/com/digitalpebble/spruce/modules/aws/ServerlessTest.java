// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

import com.digitalpebble.spruce.AWSFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ServerlessTest {

    private final Serverless serverless = new Serverless();
    private final StructType schema = Utils.getSchema(serverless);

    @Test
    void processEmptyRow() {
        Row row = generateRow(null, null, null);
        Map<Column, Object> enriched = new HashMap<>();
        serverless.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    @Test
    void processFargateMemory() {
        double quantity = 10d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-GB-Hours");
        Map<Column, Object> enriched = new HashMap<>();
        serverless.enrich(row, enriched);
        double expected = serverless.memory_coefficient_kwh * quantity;
        assertEquals(expected, enriched.get(ENERGY_USED));
    }

    @Test
    void processFargatevCPU() {
        double quantity = 4d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-vCPU-Hours:perCPU");
        Map<Column, Object> enriched = new HashMap<>();
        serverless.enrich(row, enriched);
        double expected = serverless.x86_cpu_coefficient_kwh * quantity;
        assertEquals(expected, enriched.get(ENERGY_USED));
    }

    @Test
    void processFargatevCPUARM() {
        double quantity = 4d;
        Row row = generateRow("FargateTask", quantity, "xx-Fargate-ARM-vCPU-Hours:perCPU");
        Map<Column, Object> enriched = new HashMap<>();
        serverless.enrich(row, enriched);
        double expected = serverless.arm_cpu_coefficient_kwh * quantity;
        assertEquals(expected, enriched.get(ENERGY_USED));
    }

    @Test
    void processEMRvCPUARM() {
        double quantity = 4d;
        Row row = generateRow("Worker", quantity, "xxx-EMR-SERVERLESS-ARM-vCPUHours");
        Map<Column, Object> enriched = new HashMap<>();
        serverless.enrich(row, enriched);
        double expected = serverless.arm_cpu_coefficient_kwh * quantity;
        assertEquals(expected, enriched.get(ENERGY_USED));
    }

    @Test
    void processEMRvCPU() {
        double quantity = 4d;
        Row row = generateRow("Worker", quantity, "xx-EMR-SERVERLESS-vCPUHours");
        Map<Column, Object> enriched = new HashMap<>();
        serverless.enrich(row, enriched);
        double expected = serverless.x86_cpu_coefficient_kwh * quantity;
        assertEquals(expected, enriched.get(ENERGY_USED));
    }

    @Test
    void processEMRMemory() {
        double quantity = 10d;
        Row row = generateRow("Worker", quantity, "EUN1-EMR-SERVERLESS-ARM-MemoryGBHours");
        Map<Column, Object> enriched = new HashMap<>();
        serverless.enrich(row, enriched);
        double expected = serverless.memory_coefficient_kwh * quantity;
        assertEquals(expected, enriched.get(ENERGY_USED));
    }

    private Row generateRow(String LINE_ITEM_OPERATION, Object USAGE_AMOUNT, String LINE_ITEM_USAGE_TYPE){
        Object[] values = new Object[] {LINE_ITEM_OPERATION, USAGE_AMOUNT, LINE_ITEM_USAGE_TYPE, null};
        return new GenericRowWithSchema(values, schema);
    }

    /**
     * The FOCUS binding reads the same values from the FOCUS column names ({@code x_Operation},
     * {@code ConsumedQuantity}, {@code SkuMeter}); the estimation logic is shared with the tests
     * above.
     */
    @Nested
    class FOCUSBinding {

        private final Serverless focusServerless = new Serverless();
        private StructType focusSchema;

        @org.junit.jupiter.api.BeforeEach
        void initialize() {
            focusServerless.bindReportFormat(ReportFormat.FOCUS);
            focusSchema = Utils.getSchema(focusServerless);
        }

        @Test
        void columnsNeededReflectsFOCUSColumns() {
            assertArrayEquals(new Column[]{
                    AWSFOCUSColumn.X_OPERATION,
                    FOCUSColumn.CONSUMED_QUANTITY,
                    FOCUSColumn.SKU_METER
            }, focusServerless.columnsNeeded());
        }

        @Test
        void processFargateMemory() {
            double quantity = 12d;
            Object[] values = new Object[]{"FargateTask", quantity, "USE1-Fargate-GB-Hours", null};
            Row row = new GenericRowWithSchema(values, focusSchema);
            Map<Column, Object> enriched = new HashMap<>();
            focusServerless.enrich(row, enriched);
            assertEquals(focusServerless.memory_coefficient_kwh * quantity, enriched.get(ENERGY_USED));
        }

        @Test
        void processFargatevCPU() {
            double quantity = 4d;
            Object[] values = new Object[]{"FargateTask", quantity, "USE1-Fargate-vCPU-Hours:perCPU", null};
            Row row = new GenericRowWithSchema(values, focusSchema);
            Map<Column, Object> enriched = new HashMap<>();
            focusServerless.enrich(row, enriched);
            assertEquals(focusServerless.x86_cpu_coefficient_kwh * quantity, enriched.get(ENERGY_USED));
        }
    }
}
