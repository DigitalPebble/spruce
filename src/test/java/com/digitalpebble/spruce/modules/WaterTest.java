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

    private Row emptyRow() {
        return new GenericRowWithSchema(new Object[schema.length()], schema);
    }

    @Test
    void noEnrichmentWithoutEnergy() {
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        water.enrich(row, enriched);
        assertFalse(enriched.containsKey(WATER_COOLING));
        assertFalse(enriched.containsKey(WATER_ENERGY));
        assertFalse(enriched.containsKey(WATER_STRESS));
    }

    @Test
    void noEnrichmentWithoutRegion() {
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.15);
        water.enrich(row, enriched);
        assertFalse(enriched.containsKey(WATER_COOLING));
        assertFalse(enriched.containsKey(WATER_ENERGY));
        assertFalse(enriched.containsKey(WATER_STRESS));
    }

    @Test
    void waterCoolingFromExactRegion() {
        // us-east-1 has WUE = 0.12 in aws-pue-wue.csv
        Row row = emptyRow();
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
        Row row = emptyRow();
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
        Row row = emptyRow();
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
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.24);
        enriched.put(REGION, "af-south-1");
        water.enrich(row, enriched);

        assertFalse(enriched.containsKey(WATER_COOLING));
    }

    @Test
    void pueDefaultsToOneWhenAbsent() {
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(REGION, "us-east-1");
        // No PUE set
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_COOLING));
        // 100 * 1.0 * 0.12 = 12.0
        assertEquals(12.0, (Double) enriched.get(WATER_COOLING), 0.01);
    }

    // --- Water stress tests ---

    @Test
    void waterStressPopulatedForHighStressRegion() {
        // ap-south-1 (Mumbai) -> IN-WE, stress=4 (Extremely High)
        // WUE matches regex ap-.+ = 0.98, WCF = 3.44
        // waterCooling = 100 * 1.42 * 0.98 = 139.16
        // waterEnergy = 100 * 1.42 * 3.44 = 488.48
        // stress = 139.16 + 488.48 = 627.64
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.42);
        enriched.put(REGION, "ap-south-1");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_STRESS));
        assertEquals(627.64, (Double) enriched.get(WATER_STRESS), 0.01);
    }

    @Test
    void waterStressIncludesCoolingAndEnergy() {
        // ap-southeast-2 (Sydney) -> AU-NSW, stress=3 (High), WUE=0.12, WCF=4.73
        // waterCooling = 100 * 1.0 * 0.12 = 12.0
        // waterEnergy = 100 * 1.0 * 4.73 = 473.0
        // stress = 12.0 + 473.0 = 485.0
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(REGION, "ap-southeast-2");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_COOLING));
        assertTrue(enriched.containsKey(WATER_ENERGY));
        assertTrue(enriched.containsKey(WATER_STRESS));
        assertEquals(485.0, (Double) enriched.get(WATER_STRESS), 0.01);
    }

    @Test
    void noWaterStressForMediumStressRegion() {
        // us-east-1 -> US-MIDA-PJM, stress=2 (Medium-High, below threshold)
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.15);
        enriched.put(REGION, "us-east-1");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_COOLING));
        assertTrue(enriched.containsKey(WATER_ENERGY));
        assertFalse(enriched.containsKey(WATER_STRESS));
    }

    @Test
    void noWaterStressForLowStressRegion() {
        // eu-west-1 (Ireland) -> IE, stress=0 (Low)
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.11);
        enriched.put(REGION, "eu-west-1");
        water.enrich(row, enriched);

        assertFalse(enriched.containsKey(WATER_STRESS));
    }
}
