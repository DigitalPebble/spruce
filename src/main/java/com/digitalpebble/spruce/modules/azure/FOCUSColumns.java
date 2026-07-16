// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.time.LocalDate;
import java.util.Map;

import static com.digitalpebble.spruce.AzureColumn.CHARGE_TYPE;
import static com.digitalpebble.spruce.AzureColumn.COST_IN_BILLING_CURRENCY;
import static com.digitalpebble.spruce.AzureColumn.DATE;
import static com.digitalpebble.spruce.AzureColumn.METER_CATEGORY;
import static com.digitalpebble.spruce.AzureColumn.RESOURCE_LOCATION;
import static com.digitalpebble.spruce.AzureColumn.SUBSCRIPTION_ID;
import static com.digitalpebble.spruce.FOCUSColumn.BILLED_COST;
import static com.digitalpebble.spruce.FOCUSColumn.CHARGE_CATEGORY;
import static com.digitalpebble.spruce.FOCUSColumn.CHARGE_PERIOD_END;
import static com.digitalpebble.spruce.FOCUSColumn.CHARGE_PERIOD_START;
import static com.digitalpebble.spruce.FOCUSColumn.REGION_ID;
import static com.digitalpebble.spruce.FOCUSColumn.SERVICE_NAME;
import static com.digitalpebble.spruce.FOCUSColumn.SUB_ACCOUNT_ID;
import static com.digitalpebble.spruce.SpruceColumn.REGION;

/**
 * Bridges Azure-native billing columns to provider-neutral FOCUS
 * (FinOps Open Cost &amp; Usage Specification) output columns so the reporting scripts and
 * dashboard can consume Azure-enriched data with the same column names as other providers.
 *
 * <p>Runs last in the Azure pipeline as it reads the normalised {@code region} produced by
 * {@link RegionExtraction}. Columns that already carry a FOCUS-compatible name in the Azure input
 * (e.g. {@code BillingCurrency}, {@code Tags}) are left untouched and pass through unchanged.
 **/
public class FOCUSColumns implements EnrichmentModule {

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{COST_IN_BILLING_CURRENCY, METER_CATEGORY, CHARGE_TYPE, SUBSCRIPTION_ID, DATE};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{BILLED_COST, REGION_ID, SERVICE_NAME, CHARGE_CATEGORY,
                SUB_ACCOUNT_ID, CHARGE_PERIOD_START, CHARGE_PERIOD_END};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (!COST_IN_BILLING_CURRENCY.isNullAt(row)) {
            enrichedValues.put(BILLED_COST, COST_IN_BILLING_CURRENCY.getDouble(row));
        }

        // RegionId falls back to the raw ResourceLocation when RegionExtraction did not set region.
        String region = REGION.getString(enrichedValues);
        if (region == null) {
            region = RESOURCE_LOCATION.getString(row, true);
            if (region != null) {
                region = region.toLowerCase();
            }
        }
        if (region != null) {
            enrichedValues.put(REGION_ID, region);
        }

        String service = METER_CATEGORY.getString(row);
        if (service != null) {
            enrichedValues.put(SERVICE_NAME, service);
        }

        String chargeType = CHARGE_TYPE.getString(row);
        if (chargeType != null) {
            enrichedValues.put(CHARGE_CATEGORY, chargeType);
        }

        String subscriptionId = SUBSCRIPTION_ID.getString(row);
        if (subscriptionId != null) {
            enrichedValues.put(SUB_ACCOUNT_ID, subscriptionId);
        }

        // Azure usage rows have daily granularity, so the charge period spans one day
        LocalDate date = DATE.getDate(row);
        if (date != null) {
            enrichedValues.put(CHARGE_PERIOD_START, date.toString());
            enrichedValues.put(CHARGE_PERIOD_END, date.plusDays(1).toString());
        }
    }
}
