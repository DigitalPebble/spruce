// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import com.digitalpebble.spruce.modules.azure.FOCUSColumns;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.FOCUSColumn.CHARGE_PERIOD_END;
import static com.digitalpebble.spruce.FOCUSColumn.CHARGE_PERIOD_START;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Reads real Azure CSVs through Spark schema inference, which unit tests building rows by hand
 * cannot cover: Spark 4 infers ISO date columns as DateType while US-formatted ones stay strings
 * (https://github.com/DigitalPebble/spruce/issues/246).
 **/
public class SparkJobTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("SparkJobTest")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    private Dataset<Row> readCsv(String resource) {
        String path = getClass().getResource(resource).getPath();
        return spark.read().option("header", "true").option("inferSchema", "true")
                .option("quote", "\"")
                .option("escape", "\"").csv(path);
    }

    @Test
    void normalizesInferredDateColumn() {
        Dataset<Row> dataframe = readCsv("/azure/native-iso-dates.csv");
        // the premise of issue #246: Spark 4 infers ISO dates as DateType, not StringType
        assertEquals(DataTypes.DateType, dataframe.schema().apply("Date").dataType());

        dataframe = SparkJob.normalizeAzureColumns(dataframe, ReportFormat.NATIVE);

        assertEquals(DataTypes.DateType, dataframe.schema().apply("Date").dataType());
        assertEquals(DataTypes.DoubleType, dataframe.schema().apply("Quantity").dataType());
        assertEquals(DataTypes.DoubleType,
                dataframe.schema().apply("CostInBillingCurrency").dataType());

        // running the module on the inferred rows used to throw a ClassCastException
        FOCUSColumns module = new FOCUSColumns();
        Row first = dataframe.collectAsList().get(0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(first, enriched);

        assertEquals("2026-06-29", enriched.get(CHARGE_PERIOD_START));
        assertEquals("2026-06-30", enriched.get(CHARGE_PERIOD_END));
    }

    @Test
    void normalizesUsFormattedDateColumn() {
        Dataset<Row> dataframe = readCsv("/azure/native-us-dates.csv");
        // US-formatted dates are not recognised by schema inference and stay strings
        assertEquals(DataTypes.StringType, dataframe.schema().apply("Date").dataType());

        dataframe = SparkJob.normalizeAzureColumns(dataframe, ReportFormat.NATIVE);

        assertEquals(DataTypes.DateType, dataframe.schema().apply("Date").dataType());

        FOCUSColumns module = new FOCUSColumns();
        Row first = dataframe.collectAsList().get(0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(first, enriched);

        assertEquals("2026-06-29", enriched.get(CHARGE_PERIOD_START));
        assertEquals("2026-06-30", enriched.get(CHARGE_PERIOD_END));
    }
}
