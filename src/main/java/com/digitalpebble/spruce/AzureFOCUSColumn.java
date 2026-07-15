// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Non-standard {@code x_} columns found in Azure FOCUS reports. They carry the same values as
 * their legacy counterparts in {@link AzureColumn} (e.g. {@code x_SkuMeterCategory} holds what
 * {@code MeterCategory} does in an EA/MCA cost details export), so the Azure enrichment logic is
 * reused unchanged with these bindings. Standard FOCUS columns live in {@link FOCUSColumn}.
 **/
public class AzureFOCUSColumn extends RowColumn {

    public static AzureFOCUSColumn X_SKU_METER_CATEGORY = new AzureFOCUSColumn("x_SkuMeterCategory", StringType);
    public static AzureFOCUSColumn X_SKU_METER_SUBCATEGORY = new AzureFOCUSColumn("x_SkuMeterSubcategory", StringType);
    public static AzureFOCUSColumn X_SKU_METER_NAME = new AzureFOCUSColumn("x_SkuMeterName", StringType);
    /** Same values as the legacy {@code UnitOfMeasure} (e.g. "1 Hour", "10 GB/Month"). */
    public static AzureFOCUSColumn X_PRICING_UNIT_DESCRIPTION = new AzureFOCUSColumn("x_PricingUnitDescription", StringType);

    public AzureFOCUSColumn(String l, DataType t) {
        super(l, t);
    }
}
