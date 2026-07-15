// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
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

    private static final List<DateTimeFormatter> DATE_FORMATS = List.of(
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("M/d/yyyy"));

    /**
     * ChargeType → FOCUS ChargeCategory, mirroring the mapping Microsoft applies in its own
     * FOCUS exports. Unknown types are passed through unchanged.
     **/
    private static final Map<String, String> CHARGE_CATEGORIES = Map.of(
            "Usage", "Usage",
            "UnusedReservation", "Usage",
            "UnusedSavingsPlan", "Usage",
            "Purchase", "Purchase",
            "Refund", "Purchase",
            "RoundingAdjustment", "Adjustment",
            "Tax", "Tax");

    @Override
    public boolean processesAllRows() {
        // billing values must be carried for non-usage rows too (purchases, refunds)
        return true;
    }

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
            enrichedValues.put(CHARGE_CATEGORY, CHARGE_CATEGORIES.getOrDefault(chargeType, chargeType));
        }

        String subscriptionId = SUBSCRIPTION_ID.getString(row);
        if (subscriptionId != null) {
            enrichedValues.put(SUB_ACCOUNT_ID, subscriptionId);
        }

        String date = DATE.getString(row);
        if (date != null) {
            enrichedValues.put(CHARGE_PERIOD_START, date);
            String end = nextDay(date);
            if (end != null) {
                enrichedValues.put(CHARGE_PERIOD_END, end);
            }
        }
    }

    /**
     * Returns the day after the given Azure date string, in the same format, or null if it cannot
     * be parsed. Azure usage rows have daily granularity, so the charge period spans one day.
     **/
    private static String nextDay(String date) {
        String trimmed = date.trim();
        for (DateTimeFormatter format : DATE_FORMATS) {
            try {
                return LocalDate.parse(trimmed, format).plusDays(1).format(format);
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }
}
