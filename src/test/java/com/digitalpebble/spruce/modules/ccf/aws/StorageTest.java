// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf.aws;

import com.digitalpebble.spruce.AWSFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class StorageTest {

    private static final Storage storage = new Storage();
    private static final StructType schema = Utils.getSchema(storage);

    @BeforeAll
    static void initialize() {
        storage.init(Map.of());
    }

    private static Stream<Arguments> provideArgsWithType() {
        return Stream.of(
            Arguments.of("Storage", 0.1d, "EUW2-TimedStorage-ByteHrs", "AmazonS3", "GB-Mo", false),
            Arguments.of("Storage", 0.1d, "SomeUsageType", "AmazonDocDB", "GB-Mo", false),
            Arguments.of("CreateVolume", 10d, "EUW2-EBS:VolumeUsage", "AmazonEC2", "GB-Mo", false),
            Arguments.of("CreateVolume-Gp2", 10d, "EBS:VolumeUsage.gp2", "AmazonEC2", "GB-Mo", true),
            Arguments.of("CreateVolume-Gp3", 10d, "VolumeUsage.gp3", "AmazonEC2", "GB-Mo", true)
        );
    }

    private static Stream<Arguments> provideArgsWrongUnit() {
        return Stream.of(
            Arguments.of("Storage", 10d, "SomeUsageType", "AmazonDocDB", "vCPU-hour"),
            Arguments.of("CreateVolume-Gp3", 0.1, "EBS:VolumeP-IOPS.gp3", "AmazonEC2", "IOPS-Mo"),
            Arguments.of("CreateVolume-Gp2", 0.1, "EBS:VolumeP-Throughput.gp3", "AmazonEC2", "GiBps-mo")
        );
    }

    @ParameterizedTest
    @MethodSource("provideArgsWithType")
    void process(String operation, double amount, String usage, String service, String unit, boolean isSSD) {
        Object[] values = new Object[]{operation, amount, usage, service, unit, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        storage.enrich(row, enriched);
        double gb_hours = Utils.Conversions.GBMonthsToGBHours(amount);
        int replication = storage.getReplicationFactor(service, usage);
        double coef = isSSD ? storage.ssd_gb_coefficient : storage.hdd_gb_coefficient;
        double expected = gb_hours * coef * replication / 1000;
        assertEquals(expected, (Double) enriched.get(ENERGY_USED), 0.0001);
    }

    @ParameterizedTest
    @MethodSource("provideArgsWrongUnit")
    void processSSDServiceWrongUnit(String LINE_ITEM_OPERATION, double USAGE_AMOUNT,
                                     String LINE_ITEM_USAGE_TYPE, String PRODUCT_SERVICE_CODE,
                                     String PRICING_UNIT) {
        Object[] values = new Object[]{LINE_ITEM_OPERATION, USAGE_AMOUNT, LINE_ITEM_USAGE_TYPE,
                                       PRODUCT_SERVICE_CODE, PRICING_UNIT, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        storage.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    /**
     * The FOCUS binding reads the same values from the FOCUS column names; note the
     * FOCUS-normalised pricing unit ({@code GB-Months} rather than the CUR {@code GB-Mo}).
     */
    @Nested
    class FOCUSBinding {

        private final Storage focusStorage = new Storage();
        private StructType focusSchema;

        @BeforeEach
        void initialize() {
            focusStorage.bindReportFormat(ReportFormat.FOCUS);
            focusStorage.init(Map.of());
            focusSchema = Utils.getSchema(focusStorage);
        }

        @Test
        void columnsNeededReflectsFOCUSColumns() {
            assertArrayEquals(new Column[]{
                    AWSFOCUSColumn.X_OPERATION,
                    FOCUSColumn.CONSUMED_QUANTITY,
                    FOCUSColumn.SKU_METER,
                    AWSFOCUSColumn.X_SERVICE_CODE,
                    FOCUSColumn.PRICING_UNIT
            }, focusStorage.columnsNeeded());
        }

        @Test
        void processEbsVolume() {
            Map<Column, Object> enriched = enrich("CreateVolume-Gp3", 10d, "EBS:VolumeUsage.gp3", "AmazonEC2", "GB-Months");
            double gb_hours = Utils.Conversions.GBMonthsToGBHours(10d);
            int replication = focusStorage.getReplicationFactor("AmazonEC2", "EBS:VolumeUsage.gp3");
            double expected = gb_hours * focusStorage.ssd_gb_coefficient * replication / 1000;
            assertEquals(expected, (Double) enriched.get(ENERGY_USED), 0.0001);
        }

        @Test
        void processS3TimedStorage() {
            Map<Column, Object> enriched = enrich("StandardStorage", 5d, "TimedStorage-ByteHrs", "AmazonS3", "GB-Months");
            double gb_hours = Utils.Conversions.GBMonthsToGBHours(5d);
            int replication = focusStorage.getReplicationFactor("AmazonS3", "TimedStorage-ByteHrs");
            double expected = gb_hours * focusStorage.hdd_gb_coefficient * replication / 1000;
            assertEquals(expected, (Double) enriched.get(ENERGY_USED), 0.0001);
        }

        @Test
        void processWrongUnit() {
            Map<Column, Object> enriched = enrich("CreateVolume-Gp3", 0.1d, "EBS:VolumeP-IOPS.gp3", "AmazonEC2", "IOPS-Mo");
            assertFalse(enriched.containsKey(ENERGY_USED));
        }

        private Map<Column, Object> enrich(String operation, double amount, String skuMeter,
                                           String serviceCode, String pricingUnit) {
            Object[] values = new Object[]{operation, amount, skuMeter, serviceCode, pricingUnit, null};
            Row row = new GenericRowWithSchema(values, focusSchema);
            Map<Column, Object> enriched = new HashMap<>();
            focusStorage.enrich(row, enriched);
            return enriched;
        }
    }
}
