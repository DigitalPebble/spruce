// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.*;

class PWUETest {

    private PWUE pwue;
    private StructType schema;

    @BeforeEach
    void setUp() {
        pwue = new PWUE();
        pwue.init(new HashMap<>(), Provider.AWS);
        schema = Utils.getSchema(pwue);
    }

    private Row emptyRow() {
        return new GenericRowWithSchema(new Object[schema.length()], schema);
    }

    @Test
    void loadsPUEAndWUEForExactRegionMatch() {
        // us-east-1 has PUE = 1.15 and WUE = 0.12 in aws-pue-wue.csv
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-east-1");
        
        pwue.enrich(row, enriched);

        assertTrue(enriched.containsKey(PUE));
        assertTrue(enriched.containsKey(WUE));
        assertEquals(1.15, (Double) enriched.get(PUE), 0.01);
        assertEquals(0.12, (Double) enriched.get(WUE), 0.01);
    }

    @Test
    void loadsPUEAndWUEForRegexRegionMatch() {
        // us-gov-west-1 matches regex us-.+ with PUE = 1.14 and WUE = 0.13
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-gov-west-1");
        
        pwue.enrich(row, enriched);

        assertTrue(enriched.containsKey(PUE));
        assertTrue(enriched.containsKey(WUE));
        assertEquals(1.14, (Double) enriched.get(PUE), 0.01);
        assertEquals(0.13, (Double) enriched.get(WUE), 0.01);
    }

    @Test
    void loadsPUEAndWUEForRegionWithRegexFallback() {
        // ap-south-1 has exact PUE = 1.42 but no exact WUE, so it falls back to regex ap-.+ with WUE = 0.98
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "ap-south-1");
        
        pwue.enrich(row, enriched);

        assertTrue(enriched.containsKey(PUE));
        assertTrue(enriched.containsKey(WUE));
        assertEquals(1.42, (Double) enriched.get(PUE), 0.01);  // Exact match takes priority for PUE
        assertEquals(0.98, (Double) enriched.get(WUE), 0.01);  // Regex match for WUE
    }

    @Test
    void usesDefaultPUEForUnknownRegion() {
        // Unknown region should get default PUE (1.15) and no WUE
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "unknown-region-99");
        
        pwue.enrich(row, enriched);

        assertTrue(enriched.containsKey(PUE));
        assertFalse(enriched.containsKey(WUE));
        assertEquals(1.15, (Double) enriched.get(PUE), 0.01);
    }

    @Test
    void usesCustomDefaultPUEWhenConfigured() {
        // Test with custom default PUE value
        PWUE customLoader = new PWUE();
        Map<String, Object> config = new HashMap<>();
        config.put("default", 1.20);
        customLoader.init(config, Provider.AWS);
        
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "unknown-region-99");
        
        customLoader.enrich(row, enriched);

        assertTrue(enriched.containsKey(PUE));
        assertEquals(1.20, (Double) enriched.get(PUE), 0.01);
    }

    @Test
    void handlesNullRegion() {
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        // No region set
        
        pwue.enrich(row, enriched);

        assertTrue(enriched.containsKey(PUE));
        assertFalse(enriched.containsKey(WUE));
        assertEquals(1.15, (Double) enriched.get(PUE), 0.01);
    }

    @Test
    void columnsAdded() {
        Column[] columns = pwue.columnsAdded();
        assertEquals(2, columns.length);
        assertTrue(columns[0] == PUE || columns[1] == PUE);
        assertTrue(columns[0] == WUE || columns[1] == WUE);
    }

    @Test
    void columnsNeeded() {
        Column[] columns = pwue.columnsNeeded();
        assertEquals(1, columns.length);
        assertEquals(REGION, columns[0]);
    }
}