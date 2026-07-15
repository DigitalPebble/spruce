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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.AWSFOCUSColumn.X_OPERATION;
import static com.digitalpebble.spruce.AWSFOCUSColumn.X_SERVICE_CODE;
import static com.digitalpebble.spruce.FOCUSColumn.BILLED_COST;
import static com.digitalpebble.spruce.FOCUSColumn.CHARGE_CATEGORY;
import static com.digitalpebble.spruce.FOCUSColumn.CHARGE_PERIOD_END;
import static com.digitalpebble.spruce.FOCUSColumn.CHARGE_PERIOD_START;
import static com.digitalpebble.spruce.FOCUSColumn.LIST_COST;
import static com.digitalpebble.spruce.FOCUSColumn.REGION_ID;
import static com.digitalpebble.spruce.FOCUSColumn.SERVICE_NAME;
import static com.digitalpebble.spruce.FOCUSColumn.SKU_METER;
import static com.digitalpebble.spruce.FOCUSColumn.SUB_ACCOUNT_ID;
import static com.digitalpebble.spruce.FOCUSColumn.TAGS;
import static com.digitalpebble.spruce.SpruceColumn.REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Maps AWS-native billing columns to provider-neutral FOCUS output columns.
 **/
public class FOCUSColumnsTest {

    private final FOCUSColumns module = new FOCUSColumns();
    private final StructType schema = Utils.getSchema(module);

    @Test
    void mapsAllColumns() {
        Row row = generateRow(12.5, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge", "Usage",
                "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 15.0);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-east-1");

        module.enrich(row, enriched);

        assertEquals(12.5, (Double) enriched.get(BILLED_COST));
        assertEquals(15.0, (Double) enriched.get(LIST_COST));
        assertEquals("us-east-1", enriched.get(REGION_ID));
        assertEquals("AmazonEC2", enriched.get(X_SERVICE_CODE));
        assertEquals("RunInstances", enriched.get(X_OPERATION));
        assertEquals("EUW2-BoxUsage:t3.xlarge", enriched.get(SKU_METER));
        assertEquals("Usage", enriched.get(CHARGE_CATEGORY));
        assertEquals("sub-123", enriched.get(SUB_ACCOUNT_ID));
        assertEquals("2026-06-23T00:00:00Z", enriched.get(CHARGE_PERIOD_START));
        assertEquals("2026-06-24T00:00:00Z", enriched.get(CHARGE_PERIOD_END));
    }

    @Test
    void serviceNameComesFromProductMap() {
        StructType withProduct = schemaWithExtra("product",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        Map<String, String> product = new HashMap<>();
        product.put("product_name", "Amazon Elastic Compute Cloud");
        Row row = new GenericRowWithSchema(values(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge",
                "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0,
                JavaConverters.mapAsScalaMapConverter(product).asScala()), withProduct);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals("Amazon Elastic Compute Cloud", enriched.get(SERVICE_NAME));
        assertEquals("AmazonEC2", enriched.get(X_SERVICE_CODE));
    }

    @Test
    void serviceNameFallsBackToProductCode() {
        Row row = generateRow(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge", "Usage",
                "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals("AmazonEC2", enriched.get(SERVICE_NAME));
    }

    @ParameterizedTest
    @CsvSource({
            "Usage, Usage",
            "DiscountedUsage, Usage",
            "SavingsPlanCoveredUsage, Usage",
            "SavingsPlanNegation, Usage",
            "Fee, Purchase",
            "RIFee, Purchase",
            "SavingsPlanUpfrontFee, Purchase",
            "SavingsPlanRecurringFee, Purchase",
            "Tax, Tax",
            "Credit, Credit",
            "Refund, Credit",
            "EdpDiscount, Credit",
            "SomeFutureType, SomeFutureType"
    })
    void normalisesChargeCategory(String lineItemType, String expected) {
        Row row = generateRow(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge", lineItemType,
                "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals(expected, enriched.get(CHARGE_CATEGORY));
    }

    @Test
    void emptyStringsProduceNoValues() {
        Row row = generateRow(1.0, "", "", "", "",
                "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(SERVICE_NAME));
        assertFalse(enriched.containsKey(X_SERVICE_CODE));
        assertFalse(enriched.containsKey(X_OPERATION));
        assertFalse(enriched.containsKey(SKU_METER));
        assertFalse(enriched.containsKey(CHARGE_CATEGORY));
    }

    @Test
    void timestampDatesRenderAsIsoStrings() {
        // Parquet CURs carry the usage dates as timestamps, not strings
        StructField[] fields = schema.fields().clone();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].name().startsWith("line_item_usage_") && fields[i].name().endsWith("_date")) {
                fields[i] = new StructField(fields[i].name(), DataTypes.TimestampType, true, Metadata.empty());
            }
        }
        Row row = new GenericRowWithSchema(values(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge",
                "Usage", "sub-123", java.sql.Timestamp.from(java.time.Instant.parse("2026-06-23T00:00:00Z")),
                java.sql.Timestamp.from(java.time.Instant.parse("2026-06-24T00:00:00Z")), 1.0),
                new StructType(fields));
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals("2026-06-23T00:00:00Z", enriched.get(CHARGE_PERIOD_START));
        assertEquals("2026-06-24T00:00:00Z", enriched.get(CHARGE_PERIOD_END));
    }

    @Test
    void regionIdComesFromEnrichedRegion() {
        Row row = generateRow(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge", "Usage",
                "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(REGION, "us-west-2");

        module.enrich(row, enriched);

        assertEquals("us-west-2", enriched.get(REGION_ID));
    }

    @Test
    void regionIdFallsBackToProductRegionCode() {
        StructType withFallback = schemaWithExtra("product_region_code", DataTypes.StringType);
        Row row = new GenericRowWithSchema(values(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge",
                "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0, "US-EAST-1"), withFallback);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertEquals("us-east-1", enriched.get(REGION_ID));
    }

    @Test
    void nullCostProducesNoBilledCost() {
        Row row = generateRow(null, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge", "Usage",
                "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(BILLED_COST));
    }

    @Test
    void nullListCostProducesNoListCost() {
        Row row = generateRow(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge", "Usage",
                "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", null);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(LIST_COST));
    }

    @Test
    void missingRegionLeavesRegionIdNull() {
        Row row = generateRow(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge", "Usage",
                "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertNull(enriched.get(REGION_ID));
    }

    @Test
    void resourceTagsPopulatedConvertsToTagsJSON() {
        StructType withTags = schemaWithExtra("resource_tags",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        Map<String, String> tagsMap = new HashMap<>();
        tagsMap.put("env", "production");
        tagsMap.put("team", "spruce");

        Row row = new GenericRowWithSchema(values(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge",
                "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0, tagsMap), withTags);
        Map<Column, Object> enriched = new HashMap<>();

        module.enrich(row, enriched);

        assertTrue(enriched.containsKey(TAGS));
        String tagsJson = (String) enriched.get(TAGS);
        assertTrue(tagsJson.contains("\"env\":\"production\""));
        assertTrue(tagsJson.contains("\"team\":\"spruce\""));
    }

    @Test
    void resourceTagsNullDoesNotProduceTags() {
        StructType withTags = schemaWithExtra("resource_tags",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        Row row = new GenericRowWithSchema(values(1.0, "AmazonEC2", "RunInstances", "EUW2-BoxUsage:t3.xlarge",
                "Usage", "sub-123", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", 1.0, null), withTags);
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
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(X_SERVICE_CODE));
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(X_OPERATION));
        assertTrue(java.util.Arrays.asList(module.columnsAdded()).contains(SKU_METER));
        assertFalse(java.util.Arrays.asList(module.columnsAdded())
                .contains(com.digitalpebble.spruce.FOCUSColumn.BILLING_CURRENCY));
    }

    /**
     * Values for the columnsNeeded() in schema order, the columnsAdded() left null, plus any
     * extra values appended for columns beyond the module schema.
     **/
    private Object[] values(Object... needed) {
        int neededCount = module.columnsNeeded().length;
        int addedCount = module.columnsAdded().length;
        Object[] values = new Object[needed.length + addedCount];
        System.arraycopy(needed, 0, values, 0, neededCount);
        System.arraycopy(needed, neededCount, values, neededCount + addedCount, needed.length - neededCount);
        return values;
    }

    /** The module schema with one extra column appended after the columnsAdded(). */
    private StructType schemaWithExtra(String name, org.apache.spark.sql.types.DataType type) {
        StructType extended = schema;
        return extended.add(new StructField(name, type, true, Metadata.empty()));
    }

    /**
     * Builds a row matching the module schema: the columnsNeeded() come first,
     * followed by the columnsAdded() left null.
     **/
    private Row generateRow(Double cost, String productCode, String operation, String usageType,
                            String lineItemType, String subAccountId, String startDate, String endDate,
                            Double onDemandCost) {
        return new GenericRowWithSchema(
                values(cost, productCode, operation, usageType, lineItemType, subAccountId, startDate, endDate, onDemandCost),
                schema);
    }
}
