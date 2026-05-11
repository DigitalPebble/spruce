// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class StorageTest {

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
                Arguments.of("Storage", "Tables", "LRS Data Stored", "1 GB/Month", 10d, 10d, 3, false),
                Arguments.of("Storage", "Tables", "RA-GRS Data Stored", "10 GB/Month", 2d, 20d, 6, false),
                Arguments.of("Storage", "Tables", "LRS Data Stored", "1 TB/Month", 1d, 1024d, 3, false),
                Arguments.of("Storage", "Tables", "LRS Data Stored", "1 GB/Month", -5d, -5d, 3, false),
                Arguments.of("Storage", "Premium SSD Managed Disks", "P10 LRS Disk", "1/Month", 2d, 256d, 3, false),
                Arguments.of("Storage", "Standard SSD Managed Disks", "E2 LRS Disk", "1/Month", 2d, 16d, 3, false),
                Arguments.of("Storage", "Standard HDD Managed Disks", "S4 LRS Disk", "1/Month", 2d, 64d, 3, true),
                Arguments.of("Storage", "Premium SSD Managed Disks", "P1 LRS Disk", "100/Month", 1d, 400d, 3, false)
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
                Arguments.of("Storage", "Standard HDD Managed Disks", "S4 LRS Disk Operations", "10K", 10d)
        );
    }

    @ParameterizedTest
    @MethodSource("provideStorageRows")
    void processStorageRows(String meterCategory, String meterSubCategory, String meterName, String unit,
                            double quantity, double gbMonths, int replication, boolean isHDD) {
        Map<Column, Object> enriched = enrich(row(meterCategory, meterSubCategory, meterName, unit, quantity));
        double expected = expected(gbMonths, replication, isHDD);
        assertEquals(expected, (Double) enriched.get(ENERGY_USED), 0.0001);
    }

    @ParameterizedTest
    @MethodSource("provideIgnoredRows")
    void processIgnoredRows(String meterCategory, String meterSubCategory, String meterName, String unit,
                            double quantity) {
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

    private double expected(double gbMonths, int replication, boolean isHDD) {
        double coefficient = isHDD ? storage.hdd_gb_coefficient : storage.ssd_gb_coefficient;
        double gbHours = Utils.Conversions.GBMonthsToGBHours(gbMonths);
        return gbHours / 1000 * coefficient * replication;
    }
}
