// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

/**
 * Abstract base class for native column types that work with Spark Row objects.
 * Provides shared functionality for extracting values from Row objects.
 */
public abstract class RowColumn extends Column {

    RowColumn(String l, DataType t) {
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