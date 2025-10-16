// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StorageTest {

    private static final Storage storage = new Storage();

    private static final StructType schema = Utils.getSchema(storage);

    @BeforeAll
    static void initialize() {
        storage.init(Map.of());
    }

    @Test
    void processCreateVolumeSSD() {
        Object[] values = new Object[]{"CreateVolume-Gp3", 10d, "VolumeUsage.gp3", "AmazonEC2", "GB-Mo", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.ssd_gb_coefficient * 2;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @Test
    void processCreateVolumeSSD2() {
        Object[] values = new Object[]{"CreateVolume-Gp2", 10d, "EBS:VolumeUsage.gp2", "AmazonEC2", "GB-Mo", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.ssd_gb_coefficient * 2;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @Test
    void processCreateVolumeHDD() {
        Object[] values = new Object[]{"CreateVolume", 10d, "EUW2-EBS:VolumeUsage", "AmazonEC2", "GB-Mo", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.hdd_gb_coefficient * 2;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @Test
    void processS3() {
        Object[] values = new Object[]{"Storage", 10d, "EUW2-TimedStorage-ByteHrs", null, "GB-Mo", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.hdd_gb_coefficient;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @Test
    void processSSDService() {
        Object[] values = new Object[]{"Storage", 10d, "SomeUsageType", "AmazonDocDB", "GB-Mo", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.ssd_gb_coefficient * 2;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @ParameterizedTest
    @MethodSource("provideArgsWrongUnit")
    void processSSDServiceWrongUnit(String LINE_ITEM_OPERATION, double USAGE_AMOUNT, String LINE_ITEM_USAGE_TYPE, String PRODUCT_SERVICE_CODE, String PRICING_UNIT) {
        Object[] values = new Object[]{LINE_ITEM_OPERATION, USAGE_AMOUNT, LINE_ITEM_USAGE_TYPE, PRODUCT_SERVICE_CODE, PRICING_UNIT, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        assertEquals(row, result);
    }


    private static Stream<Arguments> provideArgsWrongUnit() {
        return Stream.of(
                Arguments.of("Storage", 10d, "SomeUsageType", "AmazonDocDB", "vCPU-hour"),
                Arguments.of("CreateVolume-Gp3", 0.1, "EBS:VolumeP-IOPS.gp3", "AmazonEC2", "IOPS-Mo"),
                Arguments.of("CreateVolume-Gp2", 0.1, "EBS:VolumeP-Throughput.gp3", "AmazonEC2", "GiBps-mo")
        );
    }

}