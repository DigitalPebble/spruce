// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

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

class PWUETest {

    private PWUE pwue;

    /** Only the usage date is read from the row itself; the region comes from the enriched map. */
    private static final StructType SCHEMA = new StructType(new StructField[]{
            StructField.apply(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(),
                    DataTypes.StringType, true, null)});

    @BeforeEach
    void setUp() {
        pwue = new PWUE();
        pwue.init(new HashMap<>(), Provider.AWS);
    }

    /** A row with no usage date, so the most recent figures apply. */
    private Row undatedRow() {
        return new GenericRowWithSchema(new Object[]{null}, SCHEMA);
    }

    private Row rowForYear(int year) {
        return new GenericRowWithSchema(new Object[]{year + "-06-15T00:00:00Z"}, SCHEMA);
    }

    private Map<Column, Object> enrich(Row row, String region) {
        Map<Column, Object> enriched = new HashMap<>();
        if (region != null) {
            enriched.put(REGION, region);
        }
        pwue.enrich(row, enriched);
        return enriched;
    }

    @Test
    void loadsPUEAndWUEForExactRegionMatch() {
        // us-east-1 has PUE = 1.15 and WUE = 0.12 for 2024 in aws-pue-wue.csv
        Map<Column, Object> enriched = enrich(rowForYear(2024), "us-east-1");

        assertTrue(enriched.containsKey(PUE));
        assertTrue(enriched.containsKey(WUE));
        assertEquals(1.15, (Double) enriched.get(PUE), 0.01);
        assertEquals(0.12, (Double) enriched.get(WUE), 0.01);
    }

    @Test
    void usesTheFiguresPublishedForTheUsageYear() {
        // eu-central-1 improved from PUE 1.32 in 2022 to 1.24 in 2025
        assertEquals(1.32, (Double) enrich(rowForYear(2022), "eu-central-1").get(PUE), 0.01);
        assertEquals(1.33, (Double) enrich(rowForYear(2023), "eu-central-1").get(PUE), 0.01);
        assertEquals(1.35, (Double) enrich(rowForYear(2024), "eu-central-1").get(PUE), 0.01);
        assertEquals(1.24, (Double) enrich(rowForYear(2025), "eu-central-1").get(PUE), 0.01);

        // and its WUE went the other way, from 0.01 in 2024 to 0.17 in 2025
        assertEquals(0.01, (Double) enrich(rowForYear(2024), "eu-central-1").get(WUE), 0.01);
        assertEquals(0.17, (Double) enrich(rowForYear(2025), "eu-central-1").get(WUE), 0.01);
    }

    @Test
    void fallsBackToTheClosestYearPublishedForTheRegion() {
        // eu-west-2 only has 2025 figures, used for earlier and later years alike
        assertEquals(1.23, (Double) enrich(rowForYear(2023), "eu-west-2").get(PUE), 0.01);
        assertEquals(1.23, (Double) enrich(rowForYear(2030), "eu-west-2").get(PUE), 0.01);

        // us-east-1 has PUE from 2022 but WUE only from 2024, so 2022 borrows the 2024 WUE
        Map<Column, Object> enriched = enrich(rowForYear(2022), "us-east-1");
        assertEquals(1.16, (Double) enriched.get(PUE), 0.01);
        assertEquals(0.12, (Double) enriched.get(WUE), 0.01);
    }

    @Test
    void usesTheMostRecentFiguresWhenTheRowHasNoDate() {
        // 2025 is the latest year published for us-east-1
        Map<Column, Object> enriched = enrich(undatedRow(), "us-east-1");
        assertEquals(1.15, (Double) enriched.get(PUE), 0.01);
        assertEquals(0.06, (Double) enriched.get(WUE), 0.01);
    }

    @Test
    void loadsPUEAndWUEForRegexRegionMatch() {
        // us-gov-west-1 matches regex (us|ca|mx)-.+, the North America average
        Map<Column, Object> enriched = enrich(rowForYear(2024), "us-gov-west-1");

        assertEquals(1.14, (Double) enriched.get(PUE), 0.01);
        assertEquals(0.13, (Double) enriched.get(WUE), 0.01);
    }

    @Test
    void loadsPUEAndWUEForRegionWithRegexFallback() {
        // ap-south-1 has an exact PUE for 2025 but no WUE at all, so it falls back to the
        // Asia Pacific average for the same year
        Map<Column, Object> enriched = enrich(rowForYear(2025), "ap-south-1");

        assertEquals(1.4, (Double) enriched.get(PUE), 0.01);
        assertEquals(1.1, (Double) enriched.get(WUE), 0.01);
    }

    @Test
    void fallsBackToTheGlobalAverageForUnknownRegion() {
        Map<Column, Object> enriched = enrich(rowForYear(2024), "unknown-region-99");

        assertEquals(1.15, (Double) enriched.get(PUE), 0.01);
        assertEquals(0.15, (Double) enriched.get(WUE), 0.01);

        // 2025 lowered the global average
        enriched = enrich(rowForYear(2025), "unknown-region-99");
        assertEquals(1.14, (Double) enriched.get(PUE), 0.01);
        assertEquals(0.12, (Double) enriched.get(WUE), 0.01);
    }

    @Test
    void handlesNullRegion() {
        // no region: the global average for the usage year
        Map<Column, Object> enriched = enrich(rowForYear(2024), null);

        assertTrue(enriched.containsKey(PUE));
        assertEquals(1.15, (Double) enriched.get(PUE), 0.01);
    }

    @Test
    void undatedCsvEntriesApplyToEveryYear() {
        // Microsoft does not publish these by year, so the same figures apply throughout
        PWUE azure = new PWUE();
        azure.init(new HashMap<>(), Provider.AZURE);

        for (int year : new int[]{2022, 2025}) {
            Map<Column, Object> enriched = new HashMap<>();
            enriched.put(REGION, "westeurope");
            azure.enrich(rowForYear(year), enriched);
            assertEquals(1.16, (Double) enriched.get(PUE), 0.01);
            assertEquals(0.03, (Double) enriched.get(WUE), 0.01);
        }
    }

    @Test
    void usesDefaultPUEWhenNothingMatches() {
        // the Azure CSV has no global entry, so an unknown region falls through to the default
        PWUE azure = new PWUE();
        Map<String, Object> config = new HashMap<>();
        config.put("default", 1.20);
        azure.init(config, Provider.AZURE);

        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "unknown-region-99");
        azure.enrich(undatedRow(), enriched);

        assertEquals(1.20, (Double) enriched.get(PUE), 0.01);
        assertFalse(enriched.containsKey(WUE));
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
