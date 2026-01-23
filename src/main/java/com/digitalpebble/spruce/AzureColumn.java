// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Columns from Azure reports **/
public class AzureColumn extends Column {

    public static Column CHARGE_TYPE = new AzureColumn("ChargeType", StringType);
    public static Column RESOURCE_LOCATION = new AzureColumn("ResourceLocation", StringType);
    public static Column METER_CATEGORY = new AzureColumn("MeterCategory", StringType);
    public static Column METER_SUBCATEGORY = new AzureColumn("MeterSubCategory", StringType);
    public static Column QUANTITY = new AzureColumn("Quantity", StringType);

    AzureColumn(String l, DataType t) {
        super(l, t);
    }
}
