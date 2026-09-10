// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ember;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Provider;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.*;

class AverageCarbonIntensityTest {

    private static final StructType SCHEMA = new StructType(new StructField[]{
            StructField.apply(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(), DataTypes.StringType, true, null)});

    private AverageCarbonIntensity module;

    @BeforeEach
    void setUp() {
        module = new AverageCarbonIntensity();
        module.init(Map.of(), Provider.AWS);
    }

    private static Row row(String usageStart) {
        return new GenericRowWithSchema(new Object[]{usageStart}, SCHEMA);
    }

    private Map<Column, Object> enrich(Row row, String region) {
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 1.0);
        enriched.put(REGION, region);
        module.enrich(row, enriched);
        return enriched;
    }

    @Test
    void returnsTheFigureOfTheYear() {
        // Virginia moved from 322.4 in 2024 to 327.17 in 2025
        assertEquals(322.4, module.getIntensity(Provider.AWS, "us-east-1", 2024));
        assertEquals(327.17, module.getIntensity(Provider.AWS, "us-east-1", 2025));
    }

    @Test
    void usesTheLatestYearWhenTheYearIsNotPublishedYet() {
        assertEquals(327.17, module.getIntensity(Provider.AWS, "us-east-1", 2030));
    }

    @Test
    void usesTheFirstYearBeforeTheFileStarts() {
        // the file starts in 2022
        assertEquals(321.4, module.getIntensity(Provider.AWS, "us-east-1", 2019));
    }

    @Test
    void usesTheLatestYearWithoutAYear() {
        assertEquals(327.17, module.getIntensity(Provider.AWS, "us-east-1", null));
    }

    @Test
    void usesTheClosestEarlierYearAcrossAGap() {
        NavigableMap<Integer, Double> byYear = new TreeMap<>(Map.of(2022, 100.0, 2024, 200.0));
        assertEquals(100.0, AbstractEmberCarbonIntensity.forYear(byYear, 2023));
    }

    @Test
    void gcpKnownRegionReturnsIntensity() {
        assertEquals(259.37, module.getIntensity(Provider.GOOGLE, "us-east1", 2025));
    }

    @Test
    void azureKnownRegionReturnsIntensity() {
        assertEquals(327.17, module.getIntensity(Provider.AZURE, "eastus", 2025));
    }

    @Test
    void unknownRegionReturnsNull() {
        assertNull(module.getIntensity(Provider.AWS, "us-fake-99", 2025));
    }

    @Test
    void wrongProviderReturnsNull() {
        // us-east-1 is valid for AWS but not for GCP
        assertNull(module.getIntensity(Provider.GOOGLE, "us-east-1", 2025));
    }

    @Test
    void enrichSetsTheCarbonIntensityOfTheUsageYear() {
        assertEquals(322.4, enrich(row("2024-06-15T00:00:00Z"), "us-east-1").get(CARBON_INTENSITY));
        assertEquals(327.17, enrich(row("2025-06-15T00:00:00Z"), "us-east-1").get(CARBON_INTENSITY));
    }

    @Test
    void enrichUsesTheLatestYearWithoutADate() {
        assertEquals(327.17, enrich(row(null), "us-east-1").get(CARBON_INTENSITY));
    }

    @Test
    void enrichSkipsWhenNoRegion() {
        assertFalse(enrich(row("2025-06-15T00:00:00Z"), null).containsKey(CARBON_INTENSITY));
    }

    @Test
    void enrichSkipsUnknownRegion() {
        assertFalse(enrich(row("2025-06-15T00:00:00Z"), "us-nowhere-99").containsKey(CARBON_INTENSITY));
    }

    @Test
    void enrichSkipsWhenNoEnergy() {
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-east-1");
        module.enrich(row("2025-06-15T00:00:00Z"), enriched);
        assertFalse(enriched.containsKey(CARBON_INTENSITY));
    }
}
