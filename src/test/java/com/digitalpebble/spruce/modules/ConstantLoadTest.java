// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConstantLoadTest {

    private final ConstantLoad load = new ConstantLoad();
    private final StructType schema = Utils.getSchema(load);

    @Test
    void process() {
        Object[] values = new Object[] {null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = load.process(row);
        assertEquals(50d, enriched.getDouble(0));
    }
}