// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.commons.cli.*;
import org.apache.spark.sql.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Spark job to anonymize CUR reports by keeping only known columns.
 * Known columns are those defined in CURColumn, AzureColumn, SpruceColumn, and FOCUSColumn
 * classes, plus any additional columns specified by the user.
 * Input format is determined by provider: Parquet for AWS/GOOGLE, CSV for AZURE.
 * Output is always written as Parquet.
 */
public class AnonymizeJob {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(AnonymizeJob.class);

    public static void main(String[] args) {

        final Options options = new Options();
        options.addRequiredOption("i", "input", true, "input path");
        options.addRequiredOption("o", "output", true, "output path");
        options.addOption("p", "provider", true, "cloud provider (AWS, GOOGLE, AZURE) — defaults to AWS");
        options.addOption("k", "keep", true, "comma-separated list of additional columns to keep");

        String inputPath = null;
        String outputPath = null;
        Provider provider = Provider.AWS;
        String userColumns = null;

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            inputPath = cmd.getOptionValue("i");
            outputPath = cmd.getOptionValue("o");
            String providerStr = cmd.getOptionValue("p", "AWS");
            try {
                provider = Provider.valueOf(providerStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                LOG.error("Invalid provider: '{}'", providerStr);
                System.exit(3);
            }
            userColumns = cmd.getOptionValue("k");
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("AnonymizeJob", options);
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("SPRUCE-anonymize")
                .getOrCreate();

        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // Read input based on provider (matching SparkJob logic)
        Dataset<Row> dataframe;
        if (provider == Provider.AZURE) {
            dataframe = spark.read().option("header", "true").option("inferSchema", "true")
                    .option("quote", "\"").option("escape", "\"").csv(inputPath);
        } else {
            dataframe = spark.read().parquet(inputPath);
        }

        // Build whitelist via reflection
        Set<String> whitelist = new HashSet<>();
        whitelist.addAll(getColumnLabels(CURColumn.class));
        whitelist.addAll(getColumnLabels(SpruceColumn.class));
        whitelist.addAll(getColumnLabels(FOCUSColumn.class));
        whitelist.addAll(getColumnLabels(AWSFOCUSColumn.class));

        if (provider == Provider.AZURE) {
            whitelist.addAll(getColumnLabels(AzureColumn.class));
            whitelist.addAll(getColumnLabels(AzureFOCUSColumn.class));
        }

        // Add user-specified columns
        if (userColumns != null) {
            whitelist.addAll(Arrays.asList(userColumns.split(",")));
        }

        // Process columns preserving original order
        String[] inputColumns = dataframe.columns();
        List<String> columnsToKeep = new ArrayList<>();
        Set<String> keptSet = new HashSet<>();

        for (String col : inputColumns) {
            if (whitelist.contains(col)) {
                columnsToKeep.add(col);
                keptSet.add(col);
            }
        }

        // Fail if nothing matches
        if (columnsToKeep.isEmpty()) {
            LOG.error("No columns match the whitelist. Input columns: {}", String.join(", ", inputColumns));
            System.exit(1);
        }

        // Log removed columns
        Set<String> removed = new HashSet<>(Arrays.asList(inputColumns));
        removed.removeAll(keptSet);
        if (!removed.isEmpty()) {
            LOG.info("Removing {} columns: {}", removed.size(), String.join(", ", removed));
        }

        // Select columns preserving original order
        Dataset<Row> anonymized = dataframe.selectExpr(columnsToKeep.toArray(new String[0]));

        // Write output as Parquet
        DataFrameWriter<Row> writer = anonymized.write().mode("overwrite");

        if (!dataframe.schema().getFieldIndex("BILLING_PERIOD").isEmpty()) {
            writer = writer.partitionBy("BILLING_PERIOD");
        }

        writer.parquet(outputPath);

        spark.stop();
    }

    /**
     * Extract all column labels from a Column subclass using reflection.
     */
    private static Set<String> getColumnLabels(Class<? extends Column> columnClass) {
        Set<String> labels = new HashSet<>();
        for (Field field : columnClass.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers)) {
                try {
                    Object value = field.get(null);
                    if (value instanceof Column) {
                        labels.add(((Column) value).getLabel());
                    }
                } catch (IllegalAccessException e) {
                    LOG.warn("Could not access field {}", field.getName());
                }
            }
        }
        return labels;
    }
}
