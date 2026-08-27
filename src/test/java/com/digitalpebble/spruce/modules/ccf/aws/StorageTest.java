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

import static com.digitalpebble.spruce.SpruceColumn.EMBODIED_EMISSIONS;
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
            // AmazonDocDB is in SSD_SERVICES, so it takes the SSD coefficients
            Arguments.of("Storage", 0.1d, "SomeUsageType", "AmazonDocDB", "GB-Mo", true),
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
        assertEquals(expected, (Double) enriched.get(ENERGY_USED), 1e-12);

        double embodiedCoef = isSSD ? storage.ssd_embodied_g_per_gb_hour : storage.hdd_embodied_g_per_gb_hour;
        double expectedEmbodied = gb_hours * embodiedCoef * replication;
        assertEquals(expectedEmbodied, (Double) enriched.get(EMBODIED_EMISSIONS), 0.0001);
    }

    /** A row the module does not recognise gets neither impact, not a zero for one of them. */
    @ParameterizedTest
    @MethodSource("provideArgsWrongUnit")
    void noEmbodiedWithoutEnergy(String operation, double amount, String usage, String service, String unit) {
        Object[] values = new Object[]{operation, amount, usage, service, unit, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        storage.enrich(row, enriched);
        assertFalse(enriched.containsKey(EMBODIED_EMISSIONS));
    }

    /**
     * HDD embodied carbon is a constant per drive spread over the drive's capacity and life,
     * SSD a rate per GB spread over its life. Defaults: 30 kg per 15 TB drive and 0.055 kg/GB,
     * both over 43800 hours.
     */
    @Test
    void embodiedCoefficientsDerivedFromDefaults() {
        assertEquals(30_000d / (15_000d * 43_800d), storage.hdd_embodied_g_per_gb_hour, 1e-12);
        assertEquals(55d / 43_800d, storage.ssd_embodied_g_per_gb_hour, 1e-12);
    }

    /** The drive size and life are assumptions, so they have to be overridable. */
    @Test
    void embodiedCoefficientsHonourConfig() {
        Storage configured = new Storage();
        configured.init(Map.of(
                "hdd_embodied_kg_per_drive", 28.7d,
                "hdd_capacity_gb", 22_000d,
                "ssd_embodied_kg_per_gb", 0.052d,
                "storage_lifetime_hours", 49_932d));
        assertEquals(28_700d / (22_000d * 49_932d), configured.hdd_embodied_g_per_gb_hour, 1e-12);
        assertEquals(52d / 49_932d, configured.ssd_embodied_g_per_gb_hour, 1e-12);
    }

    /** JSON configs routinely carry whole numbers as integers rather than doubles. */
    @Test
    void embodiedConfigAcceptsIntegerLiterals() {
        Storage configured = new Storage();
        configured.init(Map.of("hdd_embodied_kg_per_drive", 30, "hdd_capacity_gb", 15000));
        assertEquals(30_000d / (15_000d * 43_800d), configured.hdd_embodied_g_per_gb_hour, 1e-12);
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
            assertEquals(gb_hours * focusStorage.hdd_embodied_g_per_gb_hour * replication,
                    (Double) enriched.get(EMBODIED_EMISSIONS), 0.0001);
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
