// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BoaviztAPITest extends AbstractBoaviztaTest {

    BoaviztAPI api = new BoaviztAPI();
    StructType schema = Utils.getSchema(api);

    @Test
    void testGetEC2() {
        Object[] values = new Object[]{"AWSDataTransfer", null, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        // missing values comes back as it was
        assertEquals(row, enriched);
    }

}
