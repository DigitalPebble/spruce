// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PUETest {

    private PUE pue = new PUE();

    private StructType schema = Utils.getSchema(pue);

    @Test
    void processNoValues() {
        Object[] values = new Object[] {null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = pue.process(row);
        // missing values comes back as it was
        assertEquals(row, enriched);
    }

    @Test
    void processValues() {
        Object[] values = new Object[] {10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = pue.process(row);
        assertEquals(SpruceColumn.PUE, enriched.getDouble(1));
    }

}