// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;

import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Provider-neutral FOCUS (FinOps Open Cost &amp; Usage Specification) columns written by the
 * EnrichmentModules into the shared enriched values map. These bridge the CSP-specific billing
 * columns (cost, region, service, account, time) to the neutral FOCUS names used by the reporting
 * scripts and dashboard.
 **/
public class FOCUSColumn extends Column {

    public static FOCUSColumn BILLED_COST = new FOCUSColumn("BilledCost", DoubleType);
    public static FOCUSColumn BILLING_CURRENCY = new FOCUSColumn("BillingCurrency", StringType);
    public static FOCUSColumn REGION_ID = new FOCUSColumn("RegionId", StringType);
    public static FOCUSColumn SERVICE_NAME = new FOCUSColumn("ServiceName", StringType);
    public static FOCUSColumn CHARGE_CATEGORY = new FOCUSColumn("ChargeCategory", StringType);
    public static FOCUSColumn SUB_ACCOUNT_ID = new FOCUSColumn("SubAccountId", StringType);
    public static FOCUSColumn CHARGE_PERIOD_START = new FOCUSColumn("ChargePeriodStart", StringType);
    public static FOCUSColumn CHARGE_PERIOD_END = new FOCUSColumn("ChargePeriodEnd", StringType);
    public static FOCUSColumn TAGS = new FOCUSColumn("Tags", StringType);

    private FOCUSColumn(String l, DataType t) {
        super(l, t);
    }

    /** Returns the String value for this column from the enriched values map, or null if absent. */
    public String getString(Map<Column, Object> enrichedValues) {
        return (String) enrichedValues.get(this);
    }

    /** Returns the Double value for this column from the enriched values map, or null if absent. */
    public Double getDouble(Map<Column, Object> enrichedValues) {
        return (Double) enrichedValues.get(this);
    }

}
