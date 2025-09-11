// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
        Object[] values = new Object[]{"CreateVolume-Gp3", 10d, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.ssd_gb_coefficient;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @Test
    void processCreateVolumeSSD2() {
        Object[] values = new Object[]{"CreateVolume-Gp2", 10d, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.ssd_gb_coefficient;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @Test
    void processCreateVolumeHDD() {
        Object[] values = new Object[]{"CreateVolume", 10d, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.hdd_gb_coefficient;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @Test
    void processS3() {
        Object[] values = new Object[]{"Storage", 10d, "EUW2-TimedStorage-ByteHrs", null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.hdd_gb_coefficient;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }

    @Test
    void processSSDService() {
        Object[] values = new Object[]{"Storage", 10d, "SomeUsageType", "AmazonFSx", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d * storage.ssd_gb_coefficient;
        assertEquals(expected, ENERGY_USED.getDouble(result));
    }
}