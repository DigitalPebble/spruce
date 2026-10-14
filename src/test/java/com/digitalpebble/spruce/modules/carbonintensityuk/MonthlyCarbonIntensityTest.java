// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.carbonintensityuk;

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

import java.time.YearMonth;
import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.*;

class MonthlyCarbonIntensityTest {

    private static final StructType SCHEMA = new StructType(new StructField[]{
            StructField.apply(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(), DataTypes.StringType, true, null)});

    private MonthlyCarbonIntensity module;

    @BeforeEach
    void setUp() {
        module = new MonthlyCarbonIntensity();
        module.init(Map.of(), Provider.AWS);
    }

    private static Row row(String usageStart) {
        return new GenericRowWithSchema(new Object[]{usageStart}, SCHEMA);
    }

    private Map<Column, Object> enrich(Row row, String region, Double previousIntensity) {
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 1.0);
        enriched.put(REGION, region);
        if (previousIntensity != null) {
            enriched.put(CARBON_INTENSITY, previousIntensity);
        }
        module.enrich(row, enriched);
        return enriched;
    }

    @Test
    void returnsTheFigureOfTheMonth() {
        assertEquals(250.60, module.getIntensity(Provider.AWS, "eu-west-2", YearMonth.of(2022, 1)));
        assertEquals(152.58, module.getIntensity(Provider.AWS, "eu-west-2", YearMonth.of(2022, 2)));
    }

    @Test
    void coversTheUkRegionsOfEveryProvider() {
        YearMonth month = YearMonth.of(2022, 1);
        assertNotNull(module.getIntensity(Provider.GOOGLE, "europe-west2", month));
        assertNotNull(module.getIntensity(Provider.AZURE, "uksouth", month));
        assertNotNull(module.getIntensity(Provider.AZURE, "ukwest", month));
        // London and Cardiff are in different regions of the API
        assertNotEquals(module.getIntensity(Provider.AZURE, "uksouth", month),
                module.getIntensity(Provider.AZURE, "ukwest", month));
    }

    @Test
    void usesTheSameMonthOfTheFirstYearBeforeTheFileStarts() {
        assertEquals(152.58, module.getIntensity(Provider.AWS, "eu-west-2", YearMonth.of(2019, 2)));
    }

    @Test
    void usesTheSameMonthOfTheLatestYearAfterTheFileEnds() {
        YearMonth latest = YearMonth.of(2026, 9);
        Double expected = module.getIntensity(Provider.AWS, "eu-west-2", latest);
        assertNotNull(expected);
        assertEquals(expected, module.getIntensity(Provider.AWS, "eu-west-2", latest.plusYears(3)));
    }

    @Test
    void regionsOutsideGreatBritainAreNotCovered() {
        assertNull(module.getIntensity(Provider.AWS, "us-east-1", YearMonth.of(2024, 1)));
        // eu-west-2 is an AWS region, not an Azure one
        assertNull(module.getIntensity(Provider.AZURE, "eu-west-2", YearMonth.of(2024, 1)));
    }

    @Test
    void enrichOverridesTheYearlyFigure() {
        assertEquals(250.60, enrich(row("2022-01-15T00:00:00Z"), "eu-west-2", 254.98).get(CARBON_INTENSITY));
    }

    @Test
    void enrichKeepsThePreviousFigureOutsideGreatBritain() {
        assertEquals(322.4, enrich(row("2024-06-15T00:00:00Z"), "us-east-1", 322.4).get(CARBON_INTENSITY));
    }

    @Test
    void enrichKeepsThePreviousFigureWithoutADate() {
        assertEquals(254.98, enrich(row(null), "eu-west-2", 254.98).get(CARBON_INTENSITY));
    }

    @Test
    void enrichSkipsWhenNoRegion() {
        assertFalse(enrich(row("2022-01-15T00:00:00Z"), null, null).containsKey(CARBON_INTENSITY));
    }

    @Test
    void enrichSkipsWhenNoEnergy() {
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "eu-west-2");
        module.enrich(row("2022-01-15T00:00:00Z"), enriched);
        assertFalse(enriched.containsKey(CARBON_INTENSITY));
    }
}
