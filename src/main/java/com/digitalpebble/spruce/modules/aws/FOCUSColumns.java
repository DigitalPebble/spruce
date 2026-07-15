// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.time.format.DateTimeFormatter;
import java.util.Map;

import static com.digitalpebble.spruce.AWSFOCUSColumn.X_OPERATION;
import static com.digitalpebble.spruce.AWSFOCUSColumn.X_SERVICE_CODE;
import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_OPERATION;
import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_PRODUCT_CODE;
import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_TYPE;
import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_USAGE_TYPE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_FROM_REGION_CODE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_REGION_CODE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_TO_REGION_CODE;
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

/**
 * Bridges AWS-native billing columns to the FOCUS (FinOps Open Cost &amp; Usage Specification)
 * output columns so the reporting scripts and dashboard can consume AWS-enriched data with the
 * same column names and values as other providers or FOCUS reports:
 *
 * <ul>
 *   <li>{@code ServiceName} carries the friendly name from the {@code product} map (e.g.
 *   "Amazon Elastic Compute Cloud"), as in AWS FOCUS exports, falling back to the product
 *   code;</li>
 *   <li>{@code ChargeCategory} normalises {@code line_item_line_item_type} to the FOCUS
 *   vocabulary. {@code SavingsPlanNegation} maps to {@code Usage} so that, as in AWS FOCUS
 *   exports, covered usage and its negation net out and commitment fees carry the spend as
 *   {@code Purchase};</li>
 *   <li>{@code x_ServiceCode}, {@code x_Operation} and {@code SkuMeter} carry the same values
 *   as their counterparts in AWS FOCUS exports.</li>
 * </ul>
 *
 * <p>Runs last in the AWS pipeline as it reads the normalised {@code region} produced by
 * {@link RegionExtraction}.
 **/
public class FOCUSColumns implements EnrichmentModule {

    public static final CURColumn LINE_ITEM_UNBLENDED_COST = new CURColumn("line_item_unblended_cost", DataTypes.DoubleType);
    public static final CURColumn LINE_ITEM_USAGE_ACCOUNT_ID = new CURColumn("line_item_usage_account_id", DataTypes.StringType);
    public static final CURColumn LINE_ITEM_USAGE_START_DATE = new CURColumn("line_item_usage_start_date", DataTypes.StringType);
    public static final CURColumn LINE_ITEM_USAGE_END_DATE = new CURColumn("line_item_usage_end_date", DataTypes.StringType);
    public static final CURColumn RESOURCE_TAGS = new CURColumn("resource_tags", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    public static final CURColumn PRICING_PUBLIC_ON_DEMAND_COST = new CURColumn("pricing_public_on_demand_cost", DataTypes.DoubleType);

    private static final CURColumn[] location_columns = new CURColumn[]{
            PRODUCT_REGION_CODE, PRODUCT_FROM_REGION_CODE, PRODUCT_TO_REGION_CODE
    };

    /**
     * line_item_line_item_type → FOCUS ChargeCategory, mirroring the mapping AWS applies in its
     * own FOCUS exports. Unknown types are passed through unchanged.
     **/
    private static final Map<String, String> CHARGE_CATEGORIES = Map.ofEntries(
            Map.entry("Usage", "Usage"),
            Map.entry("DiscountedUsage", "Usage"),
            Map.entry("SavingsPlanCoveredUsage", "Usage"),
            Map.entry("SavingsPlanNegation", "Usage"),
            Map.entry("Fee", "Purchase"),
            Map.entry("RIFee", "Purchase"),
            Map.entry("SavingsPlanUpfrontFee", "Purchase"),
            Map.entry("SavingsPlanRecurringFee", "Purchase"),
            Map.entry("Tax", "Tax"),
            Map.entry("Credit", "Credit"),
            Map.entry("Refund", "Credit"),
            Map.entry("BundledDiscount", "Credit"),
            Map.entry("DistributorDiscount", "Credit"),
            Map.entry("EdpDiscount", "Credit"),
            Map.entry("PrivateRateDiscount", "Credit"),
            Map.entry("RiVolumeDiscount", "Credit"));

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public boolean processesAllRows() {
        // billing values must be carried for non-usage rows too (taxes, fees, negations)
        return true;
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{
                LINE_ITEM_UNBLENDED_COST,
                LINE_ITEM_PRODUCT_CODE,
                LINE_ITEM_OPERATION,
                LINE_ITEM_USAGE_TYPE,
                LINE_ITEM_TYPE,
                LINE_ITEM_USAGE_ACCOUNT_ID,
                LINE_ITEM_USAGE_START_DATE,
                LINE_ITEM_USAGE_END_DATE,
                PRICING_PUBLIC_ON_DEMAND_COST
        };
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{BILLED_COST, REGION_ID, SERVICE_NAME, CHARGE_CATEGORY,
                SUB_ACCOUNT_ID, CHARGE_PERIOD_START, CHARGE_PERIOD_END, TAGS, LIST_COST,
                X_SERVICE_CODE, X_OPERATION, SKU_METER};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (!LINE_ITEM_UNBLENDED_COST.isNullAt(row)) {
            enrichedValues.put(BILLED_COST, LINE_ITEM_UNBLENDED_COST.getDouble(row));
        }

        if (!PRICING_PUBLIC_ON_DEMAND_COST.isNullAt(row)) {
            enrichedValues.put(LIST_COST, PRICING_PUBLIC_ON_DEMAND_COST.getDouble(row));
        }

        // RegionId falls back to location columns when RegionExtraction did not set region.
        String region = REGION.getString(enrichedValues);
        if (region == null) {
            for (CURColumn c : location_columns) {
                region = c.getString(row, true);
                if (region != null) {
                    break;
                }
            }
        }
        if (region != null) {
            enrichedValues.put(REGION_ID, region.toLowerCase());
        }

        String productCode = emptyToNull(LINE_ITEM_PRODUCT_CODE.getString(row));
        if (productCode != null) {
            enrichedValues.put(X_SERVICE_CODE, productCode);
        }

        // The product map is absent from some CURs; fall back to the product code.
        String serviceName = null;
        try {
            serviceName = emptyToNull(Utils.getStringFromProductMap(row, "product_name", null));
        } catch (IllegalArgumentException ignored) {
        }
        if (serviceName == null) {
            serviceName = productCode;
        }
        if (serviceName != null) {
            enrichedValues.put(SERVICE_NAME, serviceName);
        }

        String operation = emptyToNull(LINE_ITEM_OPERATION.getString(row));
        if (operation != null) {
            enrichedValues.put(X_OPERATION, operation);
        }

        String usageType = emptyToNull(LINE_ITEM_USAGE_TYPE.getString(row));
        if (usageType != null) {
            enrichedValues.put(SKU_METER, usageType);
        }

        String chargeType = emptyToNull(LINE_ITEM_TYPE.getString(row));
        if (chargeType != null) {
            enrichedValues.put(CHARGE_CATEGORY, CHARGE_CATEGORIES.getOrDefault(chargeType, chargeType));
        }

        String accountId = LINE_ITEM_USAGE_ACCOUNT_ID.getString(row);
        if (accountId != null) {
            enrichedValues.put(SUB_ACCOUNT_ID, accountId);
        }

        String startDate = getDateAsString(row, LINE_ITEM_USAGE_START_DATE);
        if (startDate != null) {
            enrichedValues.put(CHARGE_PERIOD_START, startDate);
        }

        String endDate = getDateAsString(row, LINE_ITEM_USAGE_END_DATE);
        if (endDate != null) {
            enrichedValues.put(CHARGE_PERIOD_END, endDate);
        }

        int tagsIndex = -1;
        try {
            tagsIndex = RESOURCE_TAGS.resolveIndex(row);
        } catch (IllegalArgumentException ignored) {
        }

        if (tagsIndex != -1) {
            try {
                Map<String, String> tagsMap = getMapValue(row, tagsIndex);
                if (tagsMap != null) {
                    String json = OBJECT_MAPPER.writeValueAsString(tagsMap);
                    enrichedValues.put(TAGS, json);
                }
            } catch (Exception ignored) {
            }
        }
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    /**
     * Returns the usage date as an ISO-8601 UTC string (the rendering AWS FOCUS exports use for
     * charge periods). The column holds a timestamp in Parquet CURs and a string in CSV ones.
     **/
    private static String getDateAsString(Row row, CURColumn column) {
        int index = column.resolveIndex(row);
        if (row.isNullAt(index)) {
            return null;
        }
        Object raw = row.get(index);
        if (raw instanceof java.sql.Timestamp timestamp) {
            return DateTimeFormatter.ISO_INSTANT.format(timestamp.toInstant());
        }
        if (raw instanceof java.time.Instant instant) {
            return DateTimeFormatter.ISO_INSTANT.format(instant);
        }
        return emptyToNull(raw.toString());
    }

    private static Map<String, String> getMapValue(Row row, int index) {
        if (row.isNullAt(index)) {
            return null;
        }
        Object raw = row.get(index);
        if (raw instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) raw;
            Map<String, String> result = new java.util.HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() != null) {
                    result.put(entry.getKey().toString(), entry.getValue() != null ? entry.getValue().toString() : null);
                }
            }
            return result;
        } else if (raw instanceof scala.collection.Map) {
            scala.collection.Map map = (scala.collection.Map) raw;
            Map<String, String> result = new java.util.HashMap<>();
            scala.collection.Iterator<?> keys = map.keysIterator();
            while (keys.hasNext()) {
                Object key = keys.next();
                if (key != null) {
                    scala.Option<?> valueOpt = map.get(key);
                    Object val = valueOpt.isDefined() ? valueOpt.get() : null;
                    result.put(key.toString(), val != null ? val.toString() : null);
                }
            }
            return result;
        }
        return null;
    }
}
