// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.time.YearMonth;

import static org.junit.jupiter.api.Assertions.*;

class UsageDateTest {

    private static final StructType SCHEMA = new StructType(new StructField[]{
            StructField.apply(FOCUSColumn.CHARGE_PERIOD_START.getLabel(), DataTypes.StringType, true, null),
            StructField.apply(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(), DataTypes.StringType, true, null),
            StructField.apply(CURColumn.BILLING_PERIOD.getLabel(), DataTypes.StringType, true, null)});

    private static Row row(String focusStart, String curStart, String billingPeriod) {
        return new GenericRowWithSchema(new Object[]{focusStart, curStart, billingPeriod}, SCHEMA);
    }

    @Test
    void readsTheMonthFromTheFirstDateColumnThatIsFilled() {
        assertEquals(YearMonth.of(2025, 3), UsageDate.yearMonth(row("2025-03-01T00:00:00Z", "2024-01-01T00:00:00Z", "2023-01")));
        assertEquals(YearMonth.of(2024, 1), UsageDate.yearMonth(row(null, "2024-01-01T00:00:00Z", "2023-01")));
        assertEquals(YearMonth.of(2023, 1), UsageDate.yearMonth(row(null, null, "2023-01")));
    }

    @Test
    void readsTheYearTheSameWay() {
        assertEquals(2025, UsageDate.year(row("2025-03-01T00:00:00Z", null, null)));
        assertEquals(2023, UsageDate.year(row(null, null, "2023-01")));
    }

    @Test
    void returnsNullWhenNoDateColumnIsFilled() {
        assertNull(UsageDate.yearMonth(row(null, null, null)));
        assertNull(UsageDate.year(row(null, null, null)));
    }

    @Test
    void ignoresColumnsTheReportDoesNotCarry() {
        StructType curOnly = new StructType(new StructField[]{
                StructField.apply(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(), DataTypes.StringType, true, null)});
        Row row = new GenericRowWithSchema(new Object[]{"2024-11-05"}, curOnly);
        assertEquals(YearMonth.of(2024, 11), UsageDate.yearMonth(row));
    }
}
