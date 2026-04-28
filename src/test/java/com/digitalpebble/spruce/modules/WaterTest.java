// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
        // wcf.csv: aws,us-east-1 -> WCF = 2.31
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.15);
        enriched.put(REGION, "us-east-1");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_ENERGY));
        // 100 * 1.15 * 2.3127 = 265.96
        assertEquals(265.96, (Double) enriched.get(WATER_ENERGY), 0.01);
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
        // ap-south-1 (Mumbai) -> stress=4 (Extremely High)
        // WUE matches regex ap-.+ = 0.98, WCF = 3.4368 (India, wcf.csv)
        // waterCooling = 100 * 1.42 * 0.98 = 139.16
        // waterEnergy = 100 * 1.42 * 3.4368 = 488.03
        // stress = 139.16 + 488.03 = 627.19
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.42);
        enriched.put(REGION, "ap-south-1");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_STRESS));
        assertEquals(627.19, (Double) enriched.get(WATER_STRESS), 0.01);
    }

    @Test
    void waterStressIncludesCoolingAndEnergy() {
        // ap-southeast-2 (Sydney) -> stress=3 (High), WUE=0.12, WCF=4.7293 (Australia, wcf.csv)
        // waterCooling = 100 * 1.0 * 0.12 = 12.0
        // waterEnergy = 100 * 1.0 * 4.7293 = 472.93
        // stress = 12.0 + 472.93 = 484.93
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(REGION, "ap-southeast-2");
        water.enrich(row, enriched);

        assertTrue(enriched.containsKey(WATER_COOLING));
        assertTrue(enriched.containsKey(WATER_ENERGY));
        assertTrue(enriched.containsKey(WATER_STRESS));
        assertEquals(484.93, (Double) enriched.get(WATER_STRESS), 0.01);
    }

    @Test
    void noWaterStressForMediumStressRegion() {
        // us-east-1 -> stress=2 (Medium-High, below threshold)
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

    @Nested
    class WaterStatsTest {

        // --- WCF tests (keyed by provider + region) ---

        @Test
        void wcfForUsEast1() {
            // us-east-1 (Virginia) → RFCE eGRID subregion
            assertEquals(2.3127, Water.WaterStats.getWCF(Provider.AWS, "us-east-1"), 0.001);
        }

        @Test
        void wcfForUsWest2() {
            // us-west-2 (Oregon) → NWPP eGRID subregion
            assertEquals(9.4825, Water.WaterStats.getWCF(Provider.AWS, "us-west-2"), 0.001);
        }

        @Test
        void wcfForIreland() {
            // eu-west-1 (Ireland) → country-level WRI data
            assertEquals(1.4769, Water.WaterStats.getWCF(Provider.AWS, "eu-west-1"), 0.001);
        }

        @Test
        void wcfForAustralia() {
            // ap-southeast-2 (Sydney) → Australia country-level WRI data
            assertEquals(4.7293, Water.WaterStats.getWCF(Provider.AWS, "ap-southeast-2"), 0.001);
        }

        @Test
        void wcfForJapan() {
            // ap-northeast-1 (Tokyo) → Japan country-level WRI data
            assertEquals(2.3064, Water.WaterStats.getWCF(Provider.AWS, "ap-northeast-1"), 0.001);
        }

        @Test
        void wcfNullForSingapore() {
            // Singapore has no WRI data
            assertNull(Water.WaterStats.getWCF(Provider.AWS, "ap-southeast-1"));
        }

        @Test
        void wcfNullForUnknownRegion() {
            assertNull(Water.WaterStats.getWCF(Provider.AWS, "bogus-region-99"));
        }

        @Test
        void wcfForGcpRegion() {
            // gcp:us-west1 (Oregon) → NWPP eGRID subregion
            assertEquals(9.4825, Water.WaterStats.getWCF(Provider.GOOGLE, "us-west1"), 0.001);
        }

        @Test
        void wcfForAzureRegion() {
            // azure:westeurope (Netherlands) → country-level WRI data
            assertEquals(3.4330, Water.WaterStats.getWCF(Provider.AZURE, "westeurope"), 0.001);
        }

        // --- Water stress tests keyed by (provider, region) ---

        @Test
        void waterStressForUAE() {
            // me-central-1 (UAE) = Extremely High (4)
            assertEquals(4, Water.WaterStats.getWaterStressCategory(Provider.AWS, "me-central-1"));
        }

        @Test
        void waterStressLowForSwitzerland() {
            // eu-central-2 (Zurich) = Low (0)
            assertEquals(0, Water.WaterStats.getWaterStressCategory(Provider.AWS, "eu-central-2"));
        }

        @Test
        void waterStressForAustraliaNSW() {
            // ap-southeast-2 (Sydney, New South Wales) = High (3)
            assertEquals(3, Water.WaterStats.getWaterStressCategory(Provider.AWS, "ap-southeast-2"));
        }

        @Test
        void waterStressForUSCalifornia() {
            // us-west-1 (San Francisco, California) = Extremely High (4)
            assertEquals(4, Water.WaterStats.getWaterStressCategory(Provider.AWS, "us-west-1"));
        }

        @Test
        void waterStressForIndiaHyderabad() {
            // ap-south-2 (Hyderabad, Telangana) = High (3)
            assertEquals(3, Water.WaterStats.getWaterStressCategory(Provider.AWS, "ap-south-2"));
        }

        @Test
        void waterStressForIndiaMumbai() {
            // ap-south-1 (Mumbai, Maharashtra) = Extremely High (4)
            assertEquals(4, Water.WaterStats.getWaterStressCategory(Provider.AWS, "ap-south-1"));
        }

        @Test
        void waterStressForJapan() {
            // ap-northeast-1 (Tokyo, Kanagawa) = Medium-High (2) from province-level Aqueduct data
            assertEquals(2, Water.WaterStats.getWaterStressCategory(Provider.AWS, "ap-northeast-1"));
        }

        @Test
        void waterStressForHongKong() {
            // ap-east-1 (Hong Kong) = Medium-High (2)
            assertEquals(2, Water.WaterStats.getWaterStressCategory(Provider.AWS, "ap-east-1"));
        }

        // --- No data / unknown ---

        @Test
        void waterStressNullForSingapore() {
            // ap-southeast-1 (Singapore): Aqueduct has no data for Singapore
            assertNull(Water.WaterStats.getWaterStressCategory(Provider.AWS, "ap-southeast-1"));
        }

        @Test
        void waterStressNullForUnknownRegion() {
            assertNull(Water.WaterStats.getWaterStressCategory(Provider.AWS, "bogus-region-99"));
        }

    }

}
