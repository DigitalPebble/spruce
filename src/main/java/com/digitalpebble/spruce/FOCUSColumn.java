// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**  Columns required for enriching FOCUS reports **/
public class FOCUSColumn extends NativeColumn {

    public static FOCUSColumn CHARGE_CATEGORY = new FOCUSColumn("ChargeCategory", StringType);
    public static FOCUSColumn SKU_ID = new FOCUSColumn("SkuId", StringType);
    public static FOCUSColumn REGION_ID = new FOCUSColumn("RegionId", StringType);
    public static FOCUSColumn PROVIDER_NAME = new FOCUSColumn("ProviderName", StringType);

    FOCUSColumn(String l, DataType t) {
        super(l, t);
    }
}
