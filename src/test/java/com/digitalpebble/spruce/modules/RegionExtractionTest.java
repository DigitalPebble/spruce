// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

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
        Map<Column, Object> enriched = new HashMap<>();
        region.enrich(row, enriched);
        assertNull(enriched.get(REGION));
    }

    @Test
    void process() {
        String reg = "us-east-1";
        Row row = generateRow(reg, null, null);
        Map<Column, Object> enriched = new HashMap<>();
        region.enrich(row, enriched);
        assertEquals(reg, enriched.get(REGION));
    }

    @Test
    void process2() {
        String reg = "us-east-1";
        Row row = generateRow(reg, "ignore_me", null);
        Map<Column, Object> enriched = new HashMap<>();
        region.enrich(row, enriched);
        assertEquals(reg, enriched.get(REGION));
    }

    @Test
    void process3() {
        String reg = "us-east-1";
        Row row = generateRow(null, reg, null);
        Map<Column, Object> enriched = new HashMap<>();
        region.enrich(row, enriched);
        assertEquals(reg, enriched.get(REGION));
    }

    private Row generateRow(String PRODUCT_REGION_CODE, String PRODUCT_FROM_REGION_CODE, String PRODUCT_TO_REGION_CODE){
        Object[] values = new Object[] {PRODUCT_REGION_CODE, PRODUCT_FROM_REGION_CODE, PRODUCT_TO_REGION_CODE, null};
        return new GenericRowWithSchema(values, schema);
    }
}
