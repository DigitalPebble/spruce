// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RowColumnTest {

    private static final String FIELD = "usage_date";
    private static final StructType SCHEMA = new StructType(new StructField[]{
            new StructField(FIELD, DataTypes.StringType, true, Metadata.empty())});

    private static final CURColumn COLUMN = new CURColumn(FIELD, DataTypes.StringType);

    private static Row row(Object value) {
        return new GenericRowWithSchema(new Object[]{value}, SCHEMA);
    }

    @Test
    void readsTheYearFromTheRepresentationsTheReportsUse() {
        // strings, as CSV exports carry them
        assertEquals(2025, COLUMN.getYear(row("2025-01-01T00:00:00Z")));
        assertEquals(2024, COLUMN.getYear(row("2024-12-31")));
        assertEquals(2023, COLUMN.getYear(row("12/31/2023")));
        // the yyyy-MM of a billing period
        assertEquals(2022, COLUMN.getYear(row("2022-07")));

        // date and timestamp types, as Parquet reports carry them
        assertEquals(2025, COLUMN.getYear(row(java.sql.Timestamp.valueOf("2025-06-15 12:00:00"))));
        assertEquals(2025, COLUMN.getYear(row(java.sql.Date.valueOf("2025-06-15"))));
        assertEquals(2025, COLUMN.getYear(row(java.time.LocalDate.of(2025, 6, 15))));
        assertEquals(2025, COLUMN.getYear(row(java.time.LocalDateTime.of(2025, 6, 15, 12, 0))));
        assertEquals(2025, COLUMN.getYear(row(java.time.Instant.parse("2025-06-15T12:00:00Z"))));
    }

    @Test
    void returnsNullWhenThereIsNoYearToRead() {
        assertNull(COLUMN.getYear(row(null)));
        assertNull(COLUMN.getYear(row("not a date")));
        // a column the report does not carry
        assertNull(new CURColumn("absent", DataTypes.StringType).getYear(row("2025-01-01")));
    }
}
