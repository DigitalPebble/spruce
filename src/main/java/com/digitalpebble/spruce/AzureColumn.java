// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Defines columns added by the EnrichmentModules for Azure reports **/
public class AzureColumn extends NativeColumn {

    public static AzureColumn CHARGE_TYPE = new AzureColumn("ChargeType", StringType);
    public static AzureColumn RESOURCE_LOCATION = new AzureColumn("ResourceLocation", StringType);
    public static AzureColumn METER_CATEGORY = new AzureColumn("MeterCategory", StringType);
    public static AzureColumn METER_SUBCATEGORY = new AzureColumn("MeterSubCategory", StringType);
    public static AzureColumn METER_NAME = new AzureColumn("MeterName", StringType);
    public static AzureColumn UNIT_OF_MEASURE = new AzureColumn("UnitOfMeasure", StringType);
    public static AzureColumn QUANTITY = new AzureColumn("Quantity", DoubleType);
    public static AzureColumn COST_IN_BILLING_CURRENCY = new AzureColumn("CostInBillingCurrency", DoubleType);
    public static AzureColumn SUBSCRIPTION_ID = new AzureColumn("SubscriptionId", StringType);
    public static AzureColumn DATE = new AzureColumn("Date", StringType);

    AzureColumn(String l, DataType t) {
        super(l, t);
    }
}
