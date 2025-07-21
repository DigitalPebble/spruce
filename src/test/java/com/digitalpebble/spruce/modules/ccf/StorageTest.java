// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StorageTest {

    private final Storage storage = new Storage();

    @Test
    void processCreateVolumeSSD() {
        String ddl = "line_item_operation STRING, line_item_usage_amount DOUBLE, energy_usage_kwh DOUBLE";
        StructType schema = StructType.fromDDL(ddl);
        Object[] values = new Object[] {"CreateVolume-Gp3", 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d *  storage.ssd_gb_coefficient;
        assertEquals(expected, result.getDouble(2));
    }

    @Test
    void processCreateVolumeSSD2() {
        String ddl = "line_item_operation STRING, line_item_usage_amount DOUBLE, energy_usage_kwh DOUBLE";
        StructType schema = StructType.fromDDL(ddl);
        Object[] values = new Object[] {"CreateVolume-Gp2", 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d *  storage.ssd_gb_coefficient;
        assertEquals(expected, result.getDouble(2));
    }

    @Test
    void processCreateVolumeHDD() {
        String ddl = "line_item_operation STRING, line_item_usage_amount DOUBLE, energy_usage_kwh DOUBLE";
        StructType schema = StructType.fromDDL(ddl);
        Object[] values = new Object[] {"CreateVolume", 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = storage.process(row);
        double expected = 10d *  storage.hdd_gb_coefficient;
        assertEquals(expected, result.getDouble(2));
    }
}