// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.commons.cli.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import scala.Option;

import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.lit;

public class SparkJob {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SparkJob.class);

    public static void main(String[] args) {

        final Options options = new Options();
        options.addOption("c", "config", true, "config file");
        options.addRequiredOption("i", "input", true, "input path");
        options.addRequiredOption("o", "output", true, "output path");
        options.addOption("p", "provider", true, "cloud provider (AWS, GOOGLE, AZURE) — defaults to AWS");
        options.addOption("f", "format", true, "report format (NATIVE, FOCUS) — defaults to NATIVE");

        String configPath = null;
        String inputPath = null;
        String outputPath = null;
        Provider provider = Provider.AWS;
        ReportFormat reportFormat = ReportFormat.NATIVE;

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            configPath = cmd.getOptionValue("c");
            inputPath = cmd.getOptionValue("i");
            outputPath = cmd.getOptionValue("o");
            String providerStr = cmd.getOptionValue("p", "AWS");
            try {
                provider = Provider.valueOf(providerStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                LOG.error("Invalid provider: '{}'", providerStr);
                System.exit(3);
            }
            String formatStr = cmd.getOptionValue("f", "NATIVE");
            try {
                reportFormat = ReportFormat.fromString(formatStr);
            } catch (IllegalArgumentException e) {
                LOG.error("Invalid report format: '{}'", formatStr);
                System.exit(3);
            }
            if (reportFormat == ReportFormat.FOCUS && provider != Provider.AZURE && provider != Provider.AWS) {
                LOG.error("FOCUS report format is currently only supported for AWS and AZURE");
                System.exit(3);
            }
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SparkJob", options);
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("SPRUCE")
        //        .master("local[*]")
                .getOrCreate();

        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // Read the input: Parquet for AWS, CSV for AZURE
        Dataset<Row> dataframe;
        if (provider == Provider.AZURE) {
            dataframe = spark.read().option("header", "true").option("inferSchema", "true")
                    .option("quote", "\"")
                    .option("escape", "\"").csv(inputPath);
            // inferSchema may read numeric columns as strings; force them to double
            RowColumn[] numericColumns = reportFormat == ReportFormat.FOCUS
                    ? new RowColumn[]{FOCUSColumn.CONSUMED_QUANTITY, FOCUSColumn.BILLED_COST}
                    : new RowColumn[]{AzureColumn.QUANTITY, AzureColumn.COST_IN_BILLING_CURRENCY};
            java.util.List<String> columns = java.util.Arrays.asList(dataframe.columns());
            for (RowColumn column : numericColumns) {
                if (columns.contains(column.getLabel())) {
                    dataframe = dataframe.withColumn(column.getLabel(),
                            dataframe.col(column.getLabel()).cast("double"));
                }
            }
        } else {
            dataframe = spark.read().parquet(inputPath);
        }

        // define and configure modules via configuration
        Config config = null;
        try {
            // explicitly set by user
            if (configPath != null) {
                config = Config.fromJsonFile(Paths.get(configPath), provider, reportFormat);
            } else {
                // load default config
                config = Config.loadDefault(provider, reportFormat);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            System.exit(1);
        }

        for (EnrichmentModule module : config.getModules()) {
            // check that the data contains the columns needed by this module
            for (Column c : module.columnsNeeded()) {
                Option<Object> index = dataframe.schema().getFieldIndex(c.getLabel());
                if (index.isEmpty()) {
                    LOG.error("Missing column: '{}' needed by module '{}'", c.getLabel(), module.getClass().getName());
                    System.exit(2);
                }
            }

            // add new columns for the current module
            // with the correct type but a value of null
            for (Column c : module.columnsAdded()) {
                dataframe = dataframe.withColumn(c.getLabel(), lit(null).cast(c.getType()));
            }
        }

        EnrichmentPipeline pipeline = new EnrichmentPipeline(config);
        Encoder<Row> encoder = RowEncoder.encoderFor(dataframe.schema());

        Dataset<Row> enriched = dataframe.mapPartitions(pipeline, encoder);

        // Write the result as Parquet, with one subdirectory per billing period, similar to the input
        DataFrameWriter<Row> writer = enriched.write().mode("overwrite");

        final boolean hasBillingPeriods = !dataframe.schema().getFieldIndex("BILLING_PERIOD").isEmpty();
        if (hasBillingPeriods) {
            writer = writer.partitionBy("BILLING_PERIOD");
        }

        writer.parquet(outputPath);

        spark.stop();
    }
}
