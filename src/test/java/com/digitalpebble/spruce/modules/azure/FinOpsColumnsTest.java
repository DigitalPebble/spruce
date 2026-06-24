// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.FinOpsColumn.BILLED_COST;
import static com.digitalpebble.spruce.FinOpsColumn.CHARGE_CATEGORY;
import static com.digitalpebble.spruce.FinOpsColumn.CHARGE_PERIOD_END;
import static com.digitalpebble.spruce.FinOpsColumn.CHARGE_PERIOD_START;
import static com.digitalpebble.spruce.FinOpsColumn.REGION_ID;
import static com.digitalpebble.spruce.FinOpsColumn.SERVICE_NAME;
import static com.digitalpebble.spruce.FinOpsColumn.SUB_ACCOUNT_ID;
import static com.digitalpebble.spruce.SpruceColumn.REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Maps Azure-native billing columns to provider-neutral FOCUS output columns.
 **/
public class FinOpsColumnsTest {

    private final FinOpsColumns module = new FinOpsColumns();
    private final StructType schema = Utils.getSchema(module);

    @Test
    void mapsAllColumns() {
        Row row = generateRow(12.5, "Storage", "Usage", "sub-123", "2026-06-23");
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "eastus");

        module.enrich(row, enriched);

        assertEquals(12.5, (Double) enriched.get(BILLED_COST));
        assertEquals("eastus", enriched.get(REGION_ID));
        assertEquals("Storage", enriched.get(SERVICE_NAME));
        assertEquals("Usage", enriched.get(CHARGE_CATEGORY));
        assertEquals("sub-123", enriched.get(SUB_ACCOUNT_ID));
        assertEquals("2026-06-23", enriched.get(CHARGE_PERIOD_START));
        assertEquals("2026-06-24", enriched.get(CHARGE_PERIOD_END));
    }

    @Test
    void chargePeriodEndIsNextDay() {
        Row row = generateRow(1.0, "Storage", "Usage", "sub-123", "2026-12-31");
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals("2026-12-31", enriched.get(CHARGE_PERIOD_START));
        assertEquals("2027-01-01", enriched.get(CHARGE_PERIOD_END));
    }

    @Test
    void regionIdComesFromEnrichedRegion() {
        Row row = generateRow(1.0, "Storage", "Usage", "sub-123", "2026-06-23");
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "westeurope");

        module.enrich(row, enriched);

        assertEquals("westeurope", enriched.get(REGION_ID));
    }

    @Test
    void regionIdFallsBackToResourceLocation() {
        StructType withLocation = new StructType(new StructField[]{
                new StructField("CostInBillingCurrency", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("MeterCategory", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ChargeType", DataTypes.StringType, true, Metadata.empty()),
                new StructField("SubscriptionId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ResourceLocation", DataTypes.StringType, true, Metadata.empty())
        });
        Row row = new GenericRowWithSchema(
                new Object[]{1.0, "Storage", "Usage", "sub-123", "2026-06-23", "EastUS"}, withLocation);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals("eastus", enriched.get(REGION_ID));
    }

    @Test
    void nullCostProducesNoBilledCost() {
        Row row = generateRow(null, "Storage", "Usage", "sub-123", "2026-06-23");
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(BILLED_COST));
    }

    @Test
    void unparseableDateLeavesEndNull() {
        Row row = generateRow(1.0, "Storage", "Usage", "sub-123", "not-a-date");
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals("not-a-date", enriched.get(CHARGE_PERIOD_START));
        assertNull(enriched.get(CHARGE_PERIOD_END));
    }

    @Test
    void missingRegionLeavesRegionIdNull() {
        Row row = generateRow(1.0, "Storage", "Usage", "sub-123", "2026-06-23");
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertNull(enriched.get(REGION_ID));
    }

    @Test
    void declaresAddedColumns() {
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(BILLED_COST));
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(CHARGE_PERIOD_END));
        assertFalse(java.util.Arrays.asList(module.columnsAdded())
                .contains(com.digitalpebble.spruce.FinOpsColumn.TAGS));
        assertFalse(java.util.Arrays.asList(module.columnsAdded())
                .contains(com.digitalpebble.spruce.FinOpsColumn.BILLING_CURRENCY));
    }

    /**
     * Builds a row matching the module schema: the columnsNeeded() come first
     * (CostInBillingCurrency, MeterCategory, ChargeType, SubscriptionId, Date),
     * followed by the columnsAdded() left null.
     **/
    private Row generateRow(Double cost, String meterCategory, String chargeType,
                            String subscriptionId, String date) {
        Object[] values = new Object[]{
                cost, meterCategory, chargeType, subscriptionId, date,
                null, null, null, null, null, null, null
        };
        return new GenericRowWithSchema(values, schema);
    }
}
