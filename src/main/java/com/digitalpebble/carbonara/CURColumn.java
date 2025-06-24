/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;

import static org.apache.spark.sql.types.DataTypes.*;

/** Columns from CUR reports **/
public class CURColumn extends Column {

    public static Column LINE_ITEM_TYPE = new CURColumn("line_item_line_item_type", StringType);
    public static Column PRODUCT_SERVICE_CODE = new CURColumn("product_servicecode", StringType);
    public static Column USAGE_AMOUNT = new CURColumn("line_item_usage_amount", DoubleType);
    public static Column PRODUCT = new CURColumn("product", MapType.apply(StringType,StringType));
    public static Column PRODUCT_REGION_CODE = new CURColumn("product_region_code", StringType);
    public static Column PRODUCT_FROM_REGION_CODE = new CURColumn("product_from_region_code", StringType);
    public static Column PRODUCT_TO_REGION_CODE = new CURColumn("product_to_region_code", StringType);

    CURColumn(String l, DataType t) {
        super(l, t);
    }
}
