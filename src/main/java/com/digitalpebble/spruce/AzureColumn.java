// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Columns from Azure reports **/
public class AzureColumn extends Column {

    public static AzureColumn CHARGE_TYPE = new AzureColumn("ChargeType", StringType);
    public static AzureColumn RESOURCE_LOCATION = new AzureColumn("ResourceLocation", StringType);
    public static AzureColumn METER_CATEGORY = new AzureColumn("MeterCategory", StringType);
    public static AzureColumn METER_SUBCATEGORY = new AzureColumn("MeterSubCategory", StringType);
    public static AzureColumn QUANTITY = new AzureColumn("Quantity", StringType);

    AzureColumn(String l, DataType t) {
        super(l, t);
    }

    /** Returns the double value for this column in the given row. */
    public double getDouble(Row r) {
        return r.getDouble(resolveIndex(r));
    }

    /**
     * Returns the String value for this column in the given row.
     * If optional is true, returns null when the field is not in the schema;
     * otherwise propagates the exception.
     */
    public String getString(Row r, boolean optional) {
        try {
            return r.getString(resolveIndex(r));
        } catch (SparkIllegalArgumentException e) {
            if (optional) {
                return null;
            }
            throw e;
        }
    }

    /** Returns the String value for this column in the given row. */
    public String getString(Row r) {
        return getString(r, false);
    }

    /** Returns true if the value for this column is null in the given row. */
    public boolean isNullAt(Row r) {
        return r.isNullAt(resolveIndex(r));
    }
}
