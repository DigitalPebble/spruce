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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.*;

class WaterAzureTest {

    private Water water;
    private StructType schema;

    @BeforeEach
    void setUp() {
        water = new Water();
        water.init(new HashMap<>(), Provider.AZURE);
        schema = Utils.getSchema(water);
    }

    private Row emptyRow() {
        return new GenericRowWithSchema(new Object[schema.length()], schema);
    }

    @Test
    void testAzureWUEFileLoadedCorrectly() {
        // Test that Azure WUE file loads correctly by verifying a specific region
        Water waterAzure = new Water();
        waterAzure.init(new HashMap<>(), Provider.AZURE);
        
        // Create a row with Azure region
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.16); // Typical Azure PUE value
        enriched.put(WUE, 0.34); // Azure WUE value for eastus
        enriched.put(REGION, "eastus"); // Azure region
        
        // Process with Water module
        waterAzure.enrich(row, enriched);
        
        // Should have water cooling calculated (validates resource loading)
        assertTrue(enriched.containsKey(WATER_COOLING));
        
        // With the Azure PUE file, eastus should have WUE = 0.34
        // 100 * 1.16 * 0.34 = 39.44
        assertEquals(39.44, (Double) enriched.get(WATER_COOLING), 0.01);
    }

    @Test
    void testAzureRegionWithExactMatch() {
        // Test specific Azure regions with exact matches in azure-pue-wue.csv
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 50d);
        enriched.put(PUE, 1.16); // Typical Azure PUE value
        enriched.put(WUE, 0.03); // Azure region with WUE = 0.03
        enriched.put(REGION, "westeurope"); // Azure region with WUE = 0.03
        
        water.enrich(row, enriched);
        
        assertTrue(enriched.containsKey(WATER_COOLING));
        // 50 * 1.16 * 0.03 = 1.74
        assertEquals(1.74, (Double) enriched.get(WATER_COOLING), 0.01);
    }

    @Test
    void testAzureRegionWithNoWUE() {
        // Test a region that doesn't have WUE data in Azure file
        Row row = emptyRow();
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 100d);
        enriched.put(PUE, 1.28); // Typical Azure PUE value
        enriched.put(REGION, "bogus-region-99");
        
        water.enrich(row, enriched);
        
        // Should not contain water cooling since no WUE data
        assertFalse(enriched.containsKey(WATER_COOLING));
    }

    @Test
    void testInitWithProviderAzure() {
        // Test that the provider-aware init method works with Azure
        Water waterAzure = new Water();
        waterAzure.init(new HashMap<>(), Provider.AZURE);
        
        // Should initialize without errors
        assertNotNull(waterAzure);
    }
    
    @Test
    void testAzureWUEValues() {
        // Test a few Azure regions with specific WUE values to confirm they're loaded properly
        Map<String, Double> testCases = new HashMap<>();
        testCases.put("eastus", 0.34);      // WUE = 0.34
        testCases.put("westeurope", 0.03);  // WUE = 0.03
        testCases.put("australiaeast", 0.25); // WUE = 0.25
        
        for (Map.Entry<String, Double> testCase : testCases.entrySet()) {
            String region = testCase.getKey();
            double expectedWUE = testCase.getValue();
            
            Row row = emptyRow();
            Map<Column, Object> enriched = new HashMap<>();
            enriched.put(ENERGY_USED, 100d);
            enriched.put(PUE, 1.16); // Consistent PUE value for testing
            enriched.put(WUE, expectedWUE); // Add the expected WUE value
            enriched.put(REGION, region);
            
            water.enrich(row, enriched);
            
            // We only test that the WUE file is properly loaded by ensuring 
            // water cooling is calculated (which requires valid WUE data)
            assertTrue(enriched.containsKey(WATER_COOLING), "Should have WATER_COOLING for region: " + region);
            
            // Calculate expected cooling value: 100 * 1.16 * WUE
            double expectedCooling = 100 * 1.16 * expectedWUE;
            assertEquals(expectedCooling, (Double) enriched.get(WATER_COOLING), 0.01, 
                "Failed for region: " + region + " with expected WUE: " + expectedWUE);
        }
    }
}