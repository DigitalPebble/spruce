// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConstantLoadTest {

    private final ConstantLoad load = new ConstantLoad();
    private final StructType schema = Utils.getSchema(load);

    @Test
    void enrich() {
        Object[] values = new Object[] {null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        load.enrich(row, enriched);
        assertEquals(50d, enriched.get(SpruceColumn.CPU_LOAD));
    }
}
