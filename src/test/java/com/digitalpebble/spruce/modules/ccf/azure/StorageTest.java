// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf.azure;

import com.digitalpebble.spruce.AzureFOCUSColumn;
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

    private static final double ONE_HOUR_MONTH_RATIO = 1d / (30d * 24d);
    private static final double AZURE_PRICING_MONTH_HOURS = 30d * 24d;

    private Storage storage;
    private StructType schema;

    @BeforeEach
    void initialize() {
        storage = new Storage();
        storage.init(Map.of());
        schema = Utils.getSchema(storage);
    }

    private static Stream<Arguments> provideStorageRows() {
        return Stream.of(
                Arguments.of("Storage", "Tables", "LRS Data Stored", "1 GB/Month",
                        10d, Utils.Conversions.GBMonthsToGBHours(10d), 3, false),
                Arguments.of("Storage", "Tables", "RA-GRS Data Stored", "10 GB/Month",
                        2d, Utils.Conversions.GBMonthsToGBHours(20d), 6, false),
                Arguments.of("Storage", "Tables", "LRS Data Stored", "1 TB/Month",
                        1d, Utils.Conversions.GBMonthsToGBHours(1024d), 3, false),
                Arguments.of("Storage", "Tables", "LRS Data Stored", "1 GB/Month",
                        -5d, Utils.Conversions.GBMonthsToGBHours(-5d), 3, false),
                Arguments.of("Storage", "Premium SSD Managed Disks", "P10 LRS Disk", "1/Month",
                        ONE_HOUR_MONTH_RATIO, 128d, 3, false),
                Arguments.of("Storage", "Standard SSD Managed Disks", "E2 LRS Disk", "1/Month",
                        2d, 2d * 8d * AZURE_PRICING_MONTH_HOURS, 3, false),
                Arguments.of("Storage", "Standard HDD Managed Disks", "S4 LRS Disk", "1/Month",
                        2d, 2d * 32d * AZURE_PRICING_MONTH_HOURS, 3, true)
        );
    }

    private static Stream<Arguments> provideIgnoredRows() {
        return Stream.of(
                Arguments.of("Bandwidth", "Inter-Region", "LRS Data Stored", "1 GB/Month", 10d),
                Arguments.of("Storage", "Tables", "LRS Data Stored", "10K", 10d),
                Arguments.of("Storage", "Tables", "LRS Data Stored", "1M", 10d),
                Arguments.of("Storage", "Tables", "LRS Data Stored", "1 GB", 10d),
                Arguments.of("Storage", "Tables", "LRS Data Stored", "10K/Month", 10d),
                Arguments.of("Storage", "Tables", "Read Operations", "10K", 10d),
                Arguments.of("Storage", "Standard SSD Managed Disks", "E4 LRS Disk Operations", "10K", 10d),
                Arguments.of("Storage", "Standard HDD Managed Disks", "S4 LRS Disk Operations", "10K", 10d),
                Arguments.of("Storage", "Premium SSD Managed Disks", "P99 LRS Disk", "1/Month", 10d),
                Arguments.of("Storage", "Premium SSD Managed Disks", "P10 LRS Disk", "100/Month", 10d),
                Arguments.of("Storage", "Premium SSD Managed Disks", null, "1/Month", 10d),
                Arguments.of("Storage", "Premium SSD Managed Disks", "P10 LRS Disk", "1/Month", null)
        );
    }

    @ParameterizedTest
    @MethodSource("provideStorageRows")
    void processStorageRows(String meterCategory, String meterSubCategory, String meterName, String unit,
                            double quantity, double gbHours, int replication, boolean isHDD) {
        Map<Column, Object> enriched = enrich(row(meterCategory, meterSubCategory, meterName, unit, quantity));
        double expected = expected(gbHours, replication, isHDD);
        assertEquals(expected, (Double) enriched.get(ENERGY_USED), 0.0001);
    }

    @ParameterizedTest
    @MethodSource("provideIgnoredRows")
    void processIgnoredRows(String meterCategory, String meterSubCategory, String meterName, String unit,
                            Double quantity) {
        Map<Column, Object> enriched = enrich(row(meterCategory, meterSubCategory, meterName, unit, quantity));
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    private Row row(String meterCategory, String meterSubCategory, String meterName, String unit, Double quantity) {
        Object[] values = new Object[]{meterCategory, meterSubCategory, meterName, unit, quantity, null};
        return new GenericRowWithSchema(values, schema);
    }

    private Map<Column, Object> enrich(Row row) {
        Map<Column, Object> enriched = new HashMap<>();
        storage.enrich(row, enriched);
        return enriched;
    }

    private double expected(double gbHours, int replication, boolean isHDD) {
        double coefficient = isHDD ? storage.hdd_gb_coefficient : storage.ssd_gb_coefficient;
        return gbHours / 1000 * coefficient * replication;
    }

    /**
     * The FOCUS binding reads the same values from the FOCUS column names; the estimation logic
     * is shared with the tests above.
     */
    @Nested
    class FOCUSBinding {

        private Storage focusStorage;
        private StructType focusSchema;

        @BeforeEach
        void initialize() {
            focusStorage = new Storage();
            focusStorage.bindReportFormat(ReportFormat.FOCUS);
            focusStorage.init(Map.of());
            focusSchema = Utils.getSchema(focusStorage);
        }

        @Test
        void columnsNeededReflectsFOCUSColumns() {
            assertArrayEquals(new Column[]{
                    AzureFOCUSColumn.X_SKU_METER_CATEGORY,
                    AzureFOCUSColumn.X_SKU_METER_SUBCATEGORY,
                    AzureFOCUSColumn.X_SKU_METER_NAME,
                    AzureFOCUSColumn.X_PRICING_UNIT_DESCRIPTION,
                    FOCUSColumn.CONSUMED_QUANTITY
            }, focusStorage.columnsNeeded());
        }

        @Test
        void processStorageRow() {
            Map<Column, Object> enriched = enrich(row("Storage", "Tables", "LRS Data Stored", "1 GB/Month", 10d));
            double gbHours = Utils.Conversions.GBMonthsToGBHours(10d);
            double expected = gbHours / 1000 * focusStorage.ssd_gb_coefficient * 3;
            assertEquals(expected, (Double) enriched.get(ENERGY_USED), 0.0001);
        }

        @Test
        void processRowWithoutQuantity() {
            Map<Column, Object> enriched = enrich(row("Storage", "Tables", "LRS Data Stored", "1 GB/Month", null));
            assertFalse(enriched.containsKey(ENERGY_USED));
        }

        private Row row(String meterCategory, String meterSubCategory, String meterName, String unit, Double quantity) {
            Object[] values = new Object[]{meterCategory, meterSubCategory, meterName, unit, quantity, null};
            return new GenericRowWithSchema(values, focusSchema);
        }

        private Map<Column, Object> enrich(Row row) {
            Map<Column, Object> enriched = new HashMap<>();
            focusStorage.enrich(row, enriched);
            return enriched;
        }
    }
}
