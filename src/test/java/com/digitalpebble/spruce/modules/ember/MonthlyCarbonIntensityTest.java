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

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.*;

class MonthlyCarbonIntensityTest {

    private static final StructType SCHEMA = new StructType(new StructField[]{
            StructField.apply(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(),
                    DataTypes.StringType, true, null)});

    private MonthlyCarbonIntensity module;

    @BeforeEach
    void setUp() {
        module = new MonthlyCarbonIntensity();
        module.init(Map.of(), Provider.AWS);
    }

    private static Row rowDated(String date) {
        return new GenericRowWithSchema(new Object[]{date}, SCHEMA);
    }

    /** The yearly figure the module falls back to, read from the parent so refreshes do not break the test. */
    private Double yearly(String region) {
        return module.getIntensity(Provider.AWS, region);
    }

    private Map<Column, Object> enrich(Row row, String region) {
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 1.0);
        enriched.put(REGION, region);
        module.enrich(row, enriched);
        return enriched;
    }

    @Test
    void usesTheFigureForTheMonthOfTheUsage() {
        assertEquals(371.41, enrich(rowDated("2025-12-15T00:00:00Z"), "us-east-1").get(CARBON_INTENSITY));
        // the whole Ember history ships, not just recent years
        assertEquals(327.41, enrich(rowDated("2021-06-15T00:00:00Z"), "us-east-1").get(CARBON_INTENSITY));
    }

    @Test
    void fallsBackToTheYearlyFigureWhenTheMonthIsNotCovered() {
        // before Ember's records start
        assertEquals(yearly("us-east-1"), enrich(rowDated("1990-06-15T00:00:00Z"), "us-east-1").get(CARBON_INTENSITY));
        // after the latest month Ember has published
        assertEquals(yearly("us-east-1"), enrich(rowDated("2099-01-01T00:00:00Z"), "us-east-1").get(CARBON_INTENSITY));
    }

    @Test
    void fallsBackToTheYearlyFigureForRegionsWithoutMonthlyData() {
        assertNotNull(yearly("me-south-1"));
        assertEquals(yearly("me-south-1"), enrich(rowDated("2025-12-15T00:00:00Z"), "me-south-1").get(CARBON_INTENSITY));
    }

    @Test
    void fallsBackToTheYearlyFigureWhenTheRowHasNoDate() {
        assertEquals(yearly("us-east-1"), enrich(rowDated(null), "us-east-1").get(CARBON_INTENSITY));
    }

    @Test
    void leavesTheColumnUnsetForUnknownRegions() {
        assertFalse(enrich(rowDated("2025-12-15T00:00:00Z"), "us-fake-99").containsKey(CARBON_INTENSITY));
    }

    @Test
    void skipsRowsWithoutEnergy() {
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-east-1");
        module.enrich(rowDated("2025-12-15T00:00:00Z"), enriched);
        assertFalse(enriched.containsKey(CARBON_INTENSITY));
    }

    @Test
    void declaresTheSameColumnsAsTheYearlyModule() {
        AverageCarbonIntensity yearly = new AverageCarbonIntensity();
        assertArrayEquals(yearly.columnsNeeded(), module.columnsNeeded());
        assertArrayEquals(yearly.columnsAdded(), module.columnsAdded());
    }
}
