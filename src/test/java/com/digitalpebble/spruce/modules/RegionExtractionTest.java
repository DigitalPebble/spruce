// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static com.digitalpebble.spruce.SpruceColumn.REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Extracts the region information from the input and stores it in a SPRUCE column Region
 **/
public class RegionExtractionTest{

    private final RegionExtraction region = new RegionExtraction();
    private final StructType schema = Utils.getSchema(region);

    @Test
    void processEmpty() {
        Row row = generateRow(null, null, null);
        Row enriched = region.process(row);
        assertNull(REGION.getString(enriched));
    }

    @Test
    void process() {
        String reg = "us-east-1";
        Row row = generateRow(reg, null, null);
        Row enriched = region.process(row);
        assertEquals(reg, REGION.getString(enriched));
    }

    @Test
    void process2() {
        String reg = "us-east-1";
        Row row = generateRow(reg, "ignore_me", null);
        Row enriched = region.process(row);
        assertEquals(reg, REGION.getString(enriched));
    }

    @Test
    void process3() {
        String reg = "us-east-1";
        Row row = generateRow(null, reg, null);
        Row enriched = region.process(row);
        assertEquals(reg, REGION.getString(enriched));
    }

    private Row generateRow(String PRODUCT_REGION_CODE, String PRODUCT_FROM_REGION_CODE, String PRODUCT_TO_REGION_CODE){
        Object[] values = new Object[] {PRODUCT_REGION_CODE, PRODUCT_FROM_REGION_CODE, PRODUCT_TO_REGION_CODE, null};
        return new GenericRowWithSchema(values, schema);
    }
}
