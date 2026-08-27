// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /** Matches a four-digit year not embedded in a longer number, e.g. in 2025-01-01T00:00:00Z,
     *  01/15/2025 or the yyyy-MM of a billing period. */
    private static final Pattern YEAR = Pattern.compile("(?<!\\d)(?:19|20)\\d{2}(?!\\d)");

    /**
     * Returns the year of the date or timestamp held by this column in the given row, or null
     * when the column is absent from the schema, holds null, or cannot be read as a date.
     * Covers the representations the reports use: Spark timestamps and dates in Parquet, and
     * strings in CSV exports (ISO instants, ISO or US-formatted dates, and yyyy-MM periods).
     */
    public Integer getYear(Row r) {
        int index = resolveIndex(r, true);
        if (index == -1) {
            return null;
        }
        Object value = r.get(index);
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Timestamp timestamp) {
            return timestamp.toLocalDateTime().getYear();
        }
        if (value instanceof java.time.Instant instant) {
            return instant.atZone(ZoneOffset.UTC).getYear();
        }
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate().getYear();
        }
        if (value instanceof LocalDate date) {
            return date.getYear();
        }
        if (value instanceof LocalDateTime dateTime) {
            return dateTime.getYear();
        }
        Matcher matcher = YEAR.matcher(value.toString());
        return matcher.find() ? Integer.valueOf(matcher.group()) : null;
    }

    /** Returns true if the value for this column is null in the given row. */
    public boolean isNullAt(Row r) {
        return r.isNullAt(resolveIndex(r));
    }
}