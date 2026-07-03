// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

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
import static com.digitalpebble.spruce.FinOpsColumn.LIST_COST;
import static com.digitalpebble.spruce.FinOpsColumn.REGION_ID;
import static com.digitalpebble.spruce.FinOpsColumn.SERVICE_NAME;
import static com.digitalpebble.spruce.FinOpsColumn.SUB_ACCOUNT_ID;
import static com.digitalpebble.spruce.FinOpsColumn.TAGS;
import static com.digitalpebble.spruce.SpruceColumn.REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Maps AWS-native billing columns to provider-neutral FOCUS output columns.
 **/
public class FinOpsColumnsTest {

    private final FinOpsColumns module = new FinOpsColumns();
    private final StructType schema = Utils.getSchema(module);

    @Test
    void mapsAllColumns() {
        Row row = generateRow(12.5, "AmazonEC2", "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 15.0);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-east-1");

        module.enrich(row, enriched);

        assertEquals(12.5, (Double) enriched.get(BILLED_COST));
        assertEquals(15.0, (Double) enriched.get(LIST_COST));
        assertEquals("us-east-1", enriched.get(REGION_ID));
        assertEquals("AmazonEC2", enriched.get(SERVICE_NAME));
        assertEquals("Usage", enriched.get(CHARGE_CATEGORY));
        assertEquals("sub-123", enriched.get(SUB_ACCOUNT_ID));
        assertEquals("2026-06-23T00:00:00Z", enriched.get(CHARGE_PERIOD_START));
        assertEquals("2026-06-24T00:00:00Z", enriched.get(CHARGE_PERIOD_END));
    }

    @Test
    void regionIdComesFromEnrichedRegion() {
        Row row = generateRow(1.0, "AmazonEC2", "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-west-2");

        module.enrich(row, enriched);

        assertEquals("us-west-2", enriched.get(REGION_ID));
    }

    @Test
    void regionIdFallsBackToProductRegionCode() {
        StructType withFallback = new StructType(new StructField[]{
                new StructField("line_item_unblended_cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("product_servicecode", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_line_item_type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_account_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_start_date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_end_date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("pricing_public_on_demand_cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("product_region_code", DataTypes.StringType, true, Metadata.empty())
        });
        Row row = new GenericRowWithSchema(
                new Object[]{1.0, "AmazonEC2", "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0, "US-EAST-1"}, withFallback);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals("us-east-1", enriched.get(REGION_ID));
    }

    @Test
    void nullCostProducesNoBilledCost() {
        Row row = generateRow(null, "AmazonEC2", "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(BILLED_COST));
    }

    @Test
    void nullListCostProducesNoListCost() {
        Row row = generateRow(1.0, "AmazonEC2", "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", null);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(LIST_COST));
    }

    @Test
    void missingRegionLeavesRegionIdNull() {
        Row row = generateRow(1.0, "AmazonEC2", "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertNull(enriched.get(REGION_ID));
    }

    @Test
    void resourceTagsPopulatedConvertsToTagsJSON() {
        StructType withTags = new StructType(new StructField[]{
                new StructField("line_item_unblended_cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("product_servicecode", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_line_item_type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_account_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_start_date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_end_date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("pricing_public_on_demand_cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("resource_tags", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true, Metadata.empty())
        });

        Map<String, String> tagsMap = new HashMap<>();
        tagsMap.put("env", "production");
        tagsMap.put("team", "spruce");

        Row row = new GenericRowWithSchema(
                new Object[]{1.0, "AmazonEC2", "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0, tagsMap}, withTags);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertTrue(enriched.containsKey(TAGS));
        String tagsJson = (String) enriched.get(TAGS);
        assertTrue(tagsJson.contains("\"env\":\"production\""));
        assertTrue(tagsJson.contains("\"team\":\"spruce\""));
    }

    @Test
    void resourceTagsNullDoesNotProduceTags() {
        StructType withTags = new StructType(new StructField[]{
                new StructField("line_item_unblended_cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("product_servicecode", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_line_item_type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_account_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_start_date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("line_item_usage_end_date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("pricing_public_on_demand_cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("resource_tags", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true, Metadata.empty())
        });

        Row row = new GenericRowWithSchema(
                new Object[]{1.0, "AmazonEC2", "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0, null}, withTags);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertNull(enriched.get(TAGS));
    }

    @Test
    void declaresAddedColumns() {
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(BILLED_COST));
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(CHARGE_PERIOD_END));
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(TAGS));
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(LIST_COST));
        assertFalse(java.util.Arrays.asList(module.columnsAdded())
                .contains(com.digitalpebble.spruce.FinOpsColumn.BILLING_CURRENCY));
    }

    /**
     * Builds a row matching the module schema: the columnsNeeded() come first,
     * followed by the columnsAdded() left null.
     **/
    private Row generateRow(Double cost, String serviceCode, String lineItemType,
                             String subAccountId, String startDate, String endDate, Double onDemandCost) {
        Object[] values = new Object[]{
                cost, serviceCode, lineItemType, subAccountId, startDate, endDate, onDemandCost,
                null, null, null, null, null, null, null, null
        };
        return new GenericRowWithSchema(values, schema);
    }
}
