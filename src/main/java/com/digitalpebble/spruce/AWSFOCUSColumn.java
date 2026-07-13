// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Non-standard {@code x_} columns found in AWS FOCUS reports. They carry the same values as
 * their CUR counterparts in {@link CURColumn}: {@code x_Operation} holds what
 * {@code line_item_operation} does and {@code x_ServiceCode} holds what
 * {@code line_item_product_code} does (not {@code product_servicecode} — e.g. data transfer
 * rows carry the originating service, not {@code AWSDataTransfer}). Standard FOCUS columns
 * live in {@link FOCUSColumn}.
 **/
public class AWSFOCUSColumn extends RowColumn {

    public static AWSFOCUSColumn X_OPERATION = new AWSFOCUSColumn("x_Operation", StringType);
    public static AWSFOCUSColumn X_SERVICE_CODE = new AWSFOCUSColumn("x_ServiceCode", StringType);

    AWSFOCUSColumn(String l, DataType t) {
        super(l, t);
    }
}
