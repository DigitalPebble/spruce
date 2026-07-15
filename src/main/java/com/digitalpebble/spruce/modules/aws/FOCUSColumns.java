// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_TYPE;
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
import static com.digitalpebble.spruce.FOCUSColumn.SUB_ACCOUNT_ID;
import static com.digitalpebble.spruce.FOCUSColumn.TAGS;
import static com.digitalpebble.spruce.SpruceColumn.REGION;

/**
 * Bridges AWS-native billing columns to provider-neutral FOCUS
 * (FinOps Open Cost &amp; Usage Specification) output columns so the reporting scripts and
 * dashboard can consume AWS-enriched data with the same column names as other providers.
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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{
                LINE_ITEM_UNBLENDED_COST,
                PRODUCT_SERVICE_CODE,
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
                SUB_ACCOUNT_ID, CHARGE_PERIOD_START, CHARGE_PERIOD_END, TAGS, LIST_COST};
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

        String service = PRODUCT_SERVICE_CODE.getString(row);
        if (service != null) {
            enrichedValues.put(SERVICE_NAME, service);
        }

        String chargeType = LINE_ITEM_TYPE.getString(row);
        if (chargeType != null) {
            enrichedValues.put(CHARGE_CATEGORY, chargeType);
        }

        String accountId = LINE_ITEM_USAGE_ACCOUNT_ID.getString(row);
        if (accountId != null) {
            enrichedValues.put(SUB_ACCOUNT_ID, accountId);
        }

        String startDate = LINE_ITEM_USAGE_START_DATE.getString(row);
        if (startDate != null) {
            enrichedValues.put(CHARGE_PERIOD_START, startDate);
        }

        String endDate = LINE_ITEM_USAGE_END_DATE.getString(row);
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
