// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;

import static org.apache.spark.sql.types.DataTypes.*;

/** Columns from CUR reports **/
public class CURColumn extends Column {

    public static Column LINE_ITEM_OPERATION = new CURColumn("line_item_operation", StringType);
    public static Column LINE_ITEM_PRODUCT_CODE = new CURColumn("line_item_product_code", StringType);
    public static Column LINE_ITEM_TYPE = new CURColumn("line_item_line_item_type", StringType);
    public static Column PRODUCT = new CURColumn("product", MapType.apply(StringType,StringType));
    public static Column PRODUCT_INSTANCE_TYPE = new CURColumn("product_instance_type", StringType);
    public static Column PRODUCT_REGION_CODE = new CURColumn("product_region_code", StringType);
    public static Column PRODUCT_FROM_REGION_CODE = new CURColumn("product_from_region_code", StringType);
    public static Column PRODUCT_TO_REGION_CODE = new CURColumn("product_to_region_code", StringType);
    public static Column PRODUCT_SERVICE_CODE = new CURColumn("product_servicecode", StringType);
    public static Column USAGE_AMOUNT = new CURColumn("line_item_usage_amount", DoubleType);

    CURColumn(String l, DataType t) {
        super(l, t);
    }
}
