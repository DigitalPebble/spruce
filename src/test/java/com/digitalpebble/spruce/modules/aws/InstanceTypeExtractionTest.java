// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.PRODUCT_INSTANCE_TYPE;
import static com.digitalpebble.spruce.SpruceColumn.INSTANCE_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class InstanceTypeExtractionTest {

    private final InstanceTypeExtraction module = new InstanceTypeExtraction();
    private final StructType schema = Utils.getSchema(module);

    @Test
    void copiesProductInstanceType() {
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(generateRow("m5.large"), enriched);
        assertEquals("m5.large", enriched.get(INSTANCE_TYPE));
    }

    @Test
    void emptyInstanceTypeProducesNoValue() {
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(generateRow(""), enriched);
        assertFalse(enriched.containsKey(INSTANCE_TYPE));
    }

    @Test
    void nullInstanceTypeProducesNoValue() {
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(generateRow(null), enriched);
        assertFalse(enriched.containsKey(INSTANCE_TYPE));
    }

    private Row generateRow(String instanceType) {
        return new GenericRowWithSchema(new Object[]{instanceType, null}, schema);
    }

    @Nested
    class FOCUSBinding {

        private final InstanceTypeExtraction focusModule = new InstanceTypeExtraction();
        private StructType focusSchema;

        @BeforeEach
        void initialize() {
            focusModule.bindReportFormat(ReportFormat.FOCUS);
            focusSchema = Utils.getSchema(focusModule);
        }

        @Test
        void columnsNeededReflectsFOCUSColumns() {
            assertArrayEquals(new Column[]{FOCUSColumn.SKU_METER}, focusModule.columnsNeeded());
        }

        @ParameterizedTest
        @CsvSource({
                "EUW2-BoxUsage:t3.xlarge, t3.xlarge",
                "USE1-HeavyUsage:u-6tb1.metal, u-6tb1.metal",
                "USE1-BoxUsage:m7i.metal-24xl, m7i.metal-24xl",
                "USE1-RabbitMQ-3-InstanceUsage:mq.m5.large, mq.m5.large",
                "SoftwareUsage:g4dn.xlarge, g4dn.xlarge",
                "USE1-InstanceUsage:db.m5.large, db.m5.large"
        })
        void extractsInstanceTypeFromSkuMeter(String skuMeter, String expected) {
            Map<Column, Object> enriched = new HashMap<>();
            focusModule.enrich(generateFocusRow(skuMeter), enriched);
            assertEquals(expected, enriched.get(INSTANCE_TYPE));
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "EBS:VolumeUsage.gp3",
                "USE1-Fargate-vCPU-Hours:perCPU",
                "EUW2-PublicIPv4:InUseAddress",
                "ComputeSP:1yrPartialUpfront",
                "CW:MetricMonitorUsage",
                "Requests-Tier1"
        })
        void skipsNonInstanceMeters(String skuMeter) {
            Map<Column, Object> enriched = new HashMap<>();
            focusModule.enrich(generateFocusRow(skuMeter), enriched);
            assertFalse(enriched.containsKey(INSTANCE_TYPE));
        }

        @Test
        void nullSkuMeterProducesNoValue() {
            Map<Column, Object> enriched = new HashMap<>();
            focusModule.enrich(generateFocusRow(null), enriched);
            assertFalse(enriched.containsKey(INSTANCE_TYPE));
        }

        private Row generateFocusRow(String skuMeter) {
            return new GenericRowWithSchema(new Object[]{skuMeter, null}, focusSchema);
        }
    }
}
