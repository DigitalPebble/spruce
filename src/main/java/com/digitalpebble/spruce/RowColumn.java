// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

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
     * otherwise throws.
     */
    public String getString(Row r, boolean optional) {
        int index = resolveIndex(r, optional);
        if (index == -1) {
            return null;
        }
        return r.getString(index);
    }

    /** Returns the String value for this column in the given row. */
    public String getString(Row r) {
        return getString(r, false);
    }

    private static final List<DateTimeFormatter> DATE_FORMATS = List.of(
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("M/d/yyyy"));

    /**
     * Returns the LocalDate value for this column in the given row, or null if the value is
     * null or a string that cannot be parsed as a date. Handles both Spark date representations
     * (java.sql.Date, or java.time.LocalDate when the Java 8 datetime API is enabled) and
     * normalises string values, for inputs where schema inference left the column as a string.
     */
    public LocalDate getDate(Row r) {
        Object value = r.get(resolveIndex(r));
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate();
        }
        if (value instanceof LocalDate date) {
            return date;
        }
        String trimmed = value.toString().trim();
        for (DateTimeFormatter format : DATE_FORMATS) {
            try {
                return LocalDate.parse(trimmed, format);
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }

    /** Returns true if the value for this column is null in the given row. */
    public boolean isNullAt(Row r) {
        return r.isNullAt(resolveIndex(r));
    }
}