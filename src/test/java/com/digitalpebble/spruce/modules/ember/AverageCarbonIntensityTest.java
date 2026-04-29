// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ember;

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

class AverageCarbonIntensityTest {

    private AverageCarbonIntensity module;

    @BeforeEach
    void setUp() {
        module = new AverageCarbonIntensity();
        module.init(Map.of(), Provider.AWS);
    }

    @Test
    void awsKnownRegionReturnsIntensity() {
        assertEquals(384.4, module.getIntensity(Provider.AWS, "us-east-1"));
    }

    @Test
    void awsAnotherRegionReturnsIntensity() {
        assertEquals(256.54, module.getIntensity(Provider.AWS, "eu-west-1"));
    }

    @Test
    void gcpKnownRegionReturnsIntensity() {
        assertEquals(259.37, module.getIntensity(Provider.GOOGLE, "us-east1"));
    }

    @Test
    void azureKnownRegionReturnsIntensity() {
        assertEquals(327.17, module.getIntensity(Provider.AZURE, "eastus"));
    }

    @Test
    void unknownRegionReturnsNull() {
        assertNull(module.getIntensity(Provider.AWS, "us-fake-99"));
    }

    @Test
    void wrongProviderReturnsNull() {
        // us-east-1 is valid for AWS but not for GCP
        assertNull(module.getIntensity(Provider.GOOGLE, "us-east-1"));
    }

    @Test
    void enrichSetsCarbonIntensity() {
        StructType schema = Utils.getSchema(module);
        Row row = new GenericRowWithSchema(new Object[schema.fields().length], schema);

        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 1.0);
        enriched.put(REGION, "us-east-1");

        module.enrich(row, enriched);

        assertEquals(384.4, enriched.get(CARBON_INTENSITY));
    }

    @Test
    void enrichSkipsWhenNoEnergy() {
        StructType schema = Utils.getSchema(module);
        Row row = new GenericRowWithSchema(new Object[schema.fields().length], schema);

        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-east-1");

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(CARBON_INTENSITY));
    }

    @Test
    void enrichSkipsWhenNoRegion() {
        StructType schema = Utils.getSchema(module);
        Row row = new GenericRowWithSchema(new Object[schema.fields().length], schema);

        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 1.0);

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(CARBON_INTENSITY));
    }

    @Test
    void enrichSkipsUnknownRegion() {
        StructType schema = Utils.getSchema(module);
        Row row = new GenericRowWithSchema(new Object[schema.fields().length], schema);

        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 1.0);
        enriched.put(REGION, "us-nowhere-99");

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(CARBON_INTENSITY));
    }
}
