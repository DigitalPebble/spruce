// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;

/**
 * Reads the date a line item was incurred on, whichever column the report carries it in.
 * Shared by the modules that key their factors by year (PUE / WUE, carbon intensity) so they
 * agree on which column wins.
 */
public final class UsageDate {

    /**
     * Columns a usage date can be read from, in the order they are probed. All are optional: a
     * report only carries the ones its provider and format define, and the FOCUS columns exist
     * but are still null in native reports, where the FOCUS bridge module fills them in later.
     */
    private static final RowColumn[] DATE_COLUMNS = {
            FOCUSColumn.CHARGE_PERIOD_START,
            CURColumn.LINE_ITEM_USAGE_START_DATE,
            AzureColumn.DATE,
            CURColumn.BILLING_PERIOD
    };

    private UsageDate() {
    }

    /** Returns the year the line item was incurred in, or null if the row has no usable date. */
    public static Integer year(Row row) {
        for (RowColumn column : DATE_COLUMNS) {
            Integer year = column.getYear(row);
            if (year != null) {
                return year;
            }
        }
        return null;
    }
}
