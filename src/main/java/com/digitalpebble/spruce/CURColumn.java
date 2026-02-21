// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;

import static org.apache.spark.sql.types.DataTypes.*;

/** Columns from CUR reports **/
public class CURColumn extends Column {

    public static CURColumn LINE_ITEM_OPERATION = new CURColumn("line_item_operation", StringType);
    public static CURColumn LINE_ITEM_PRODUCT_CODE = new CURColumn("line_item_product_code", StringType);
    public static CURColumn LINE_ITEM_TYPE = new CURColumn("line_item_line_item_type", StringType);
    public static CURColumn LINE_ITEM_USAGE_TYPE = new CURColumn("line_item_usage_type", StringType);
    public static CURColumn PRICING_UNIT= new CURColumn("pricing_unit", StringType);
    public static CURColumn PRODUCT = new CURColumn("product", MapType.apply(StringType,StringType));
    public static CURColumn PRODUCT_INSTANCE_TYPE = new CURColumn("product_instance_type", StringType);
    public static CURColumn PRODUCT_INSTANCE_FAMILY = new CURColumn("product_instance_family", StringType);
    public static CURColumn PRODUCT_PRODUCT_FAMILY = new CURColumn("product_product_family", StringType);
    public static CURColumn PRODUCT_REGION_CODE = new CURColumn("product_region_code", StringType);
    public static CURColumn PRODUCT_FROM_REGION_CODE = new CURColumn("product_from_region_code", StringType);
    public static CURColumn PRODUCT_TO_REGION_CODE = new CURColumn("product_to_region_code", StringType);
    public static CURColumn PRODUCT_SERVICE_CODE = new CURColumn("product_servicecode", StringType);
    public static CURColumn USAGE_AMOUNT = new CURColumn("line_item_usage_amount", DoubleType);

    CURColumn(String l, DataType t) {
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
