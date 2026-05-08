// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

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
 * Extracts the region information from the input and normalise it into a SPRUCE column Region
 **/
public class RegionExtractionTest{

    private final RegionExtraction region = new RegionExtraction();
    private final StructType schema = Utils.getSchema(region);

    @Test
    void processEmpty() {
        Row row = generateRow(null);
        Map<Column, Object> enriched = new HashMap<>();
        region.enrich(row, enriched);
        assertNull(enriched.get(REGION));
    }

    @Test
    void process() {
        String reg = "eastus";
        Row row = generateRow(reg);
        Map<Column, Object> enriched = new HashMap<>();
        region.enrich(row, enriched);
        assertEquals(reg, enriched.get(REGION));
    }

    private Row generateRow(String RESOURCE_LOCATION){
        Object[] values = new Object[] {RESOURCE_LOCATION, null, null, null, null};
        return new GenericRowWithSchema(values, schema);
    }
}