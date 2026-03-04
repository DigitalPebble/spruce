// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.SpruceColumn;
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

class WaterTest {

    private Water water;
    private StructType schema;

    @BeforeEach
    void setUp() {
        water = new Water();
        water.init(new HashMap<>());
        schema = Utils.getSchema(water);
    }

    @Test
    void noEnrichmentWithoutEnergy() {
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        water.enrich(row, enriched);
        assertFalse(enriched.containsKey(WATER_COOLING));
        assertFalse(enriched.containsKey(WATER_ENERGY));
    }

    @Test
    void noEnrichmentWithoutRegion() {
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.15);
        water.enrich(row, enriched);
        assertFalse(enriched.containsKey(WATER_COOLING));
        assertFalse(enriched.containsKey(WATER_ENERGY));
    }

    @Test
    void waterCoolingFromExactRegion() {
        // us-east-1 has WUE = 0.12 in aws-pue-wue.csv
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.15);
        enriched.put(REGION, "us-east-1");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_COOLING));
        // 100 * 1.15 * 0.12 = 13.8
        assertEquals(13.8, (Double) enriched.get(WATER_COOLING), 0.01);
    }

    @Test
    void waterCoolingFromRegexRegion() {
        // us-gov-west-1 matches regex us-.+ with WUE = 0.13
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.14);
        enriched.put(REGION, "us-gov-west-1");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_COOLING));
        // 100 * 1.14 * 0.13 = 14.82
        assertEquals(14.82, (Double) enriched.get(WATER_COOLING), 0.01);
    }

    @Test
    void waterEnergyFromRegionMapping() {
        // us-east-1 maps to EM zone US-MIDA-PJM which has WCF = 2.31
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.15);
        enriched.put(REGION, "us-east-1");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_ENERGY));
        // 100 * 1.15 * 2.31 = 265.65
        assertEquals(265.65, (Double) enriched.get(WATER_ENERGY), 0.01);
    }

    @Test
    void noCoolingForRegionWithoutWue() {
        // af-south-1 has no WUE value in aws-pue-wue.csv
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.24);
        enriched.put(REGION, "af-south-1");
        water.enrich(row, enriched);

        assertFalse(enriched.containsKey(WATER_COOLING));
    }

    @Test
    void pueDefaultsToOneWhenAbsent() {
        Row row = new GenericRowWithSchema(new Object[]{null, null, null, null, null}, schema);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(REGION, "us-east-1");
        // No PUE set
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_COOLING));
        // 100 * 1.0 * 0.12 = 12.0
        assertEquals(12.0, (Double) enriched.get(WATER_COOLING), 0.01);
    }
}
