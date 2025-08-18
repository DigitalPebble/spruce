// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StorageTest {

    private final Storage storage = new Storage();

    private final StructType schema = Utils.getSchema(storage);

    @Test
    void processCreateVolumeSSD() {
        Object[] values = new Object[] {"CreateVolume-Gp3", 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d *  storage.ssd_gb_coefficient;
        assertEquals(expected, result.getDouble(2));
    }

    @Test
    void processCreateVolumeSSD2() {
        Object[] values = new Object[] {"CreateVolume-Gp2", 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d *  storage.ssd_gb_coefficient;
        assertEquals(expected, result.getDouble(2));
    }

    @Test
    void processCreateVolumeHDD() {
        Object[] values = new Object[] {"CreateVolume", 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d *  storage.hdd_gb_coefficient;
        assertEquals(expected, result.getDouble(2));
    }
}