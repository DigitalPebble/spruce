// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
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
import static com.digitalpebble.spruce.CURColumn.PRODUCT_SERVICE_CODE;
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
 * Bridges AWS-native billing columns to provider-neutral FOCUS
 * (FinOps Open Cost &amp; Usage Specification) output columns so the reporting scripts and
 * dashboard can consume AWS-enriched data with the same column names as other providers.
 * Only the columns the reporting scripts and dashboard rely on are covered — this is not a
 * CUR-to-FOCUS converter. Values are copied as-is: {@code ChargeCategory} carries the raw
 * {@code line_item_line_item_type} vocabulary, {@code x_ServiceCode}/{@code x_Operation}
 * mirror the extension columns of AWS FOCUS exports, and {@code SkuMeter} carries the usage
 * type (e.g. {@code EUW2-BoxUsage:t3.xlarge}), from which the report derives instance types.
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

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{
                LINE_ITEM_UNBLENDED_COST,
                PRODUCT_SERVICE_CODE,
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

        String service = emptyToNull(PRODUCT_SERVICE_CODE.getString(row));
        if (service != null) {
            enrichedValues.put(SERVICE_NAME, service);
        }

        String productCode = emptyToNull(LINE_ITEM_PRODUCT_CODE.getString(row));
        if (productCode != null) {
            enrichedValues.put(X_SERVICE_CODE, productCode);
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
            enrichedValues.put(CHARGE_CATEGORY, chargeType);
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

        // copied as a map of tag key/values, the representation AWS FOCUS exports use
        int tagsIndex = RESOURCE_TAGS.resolveIndex(row, true);
        if (tagsIndex != -1 && !row.isNullAt(tagsIndex)) {
            Object tags = row.get(tagsIndex);
            if (tags instanceof Map<?, ?> javaMap) {
                tags = scala.jdk.javaapi.CollectionConverters.asScala(javaMap);
            }
            if (tags instanceof scala.collection.Map) {
                enrichedValues.put(TAGS, tags);
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
}
