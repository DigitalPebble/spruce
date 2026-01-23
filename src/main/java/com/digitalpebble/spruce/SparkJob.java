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
        options.addOption("p", "provider", true, "provider");

        String configPath = null;
        String inputPath = null;
        String outputPath = null;
        Provider provider = null;

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
            dataframe = spark.read().option("header", "true").option("inferSchema", "true").csv(inputPath);
        } else {
            dataframe = spark.read().parquet(inputPath);
        }

        // define and configure modules via configuration
        Config config = null;
        try {
            // explicitly set by user
            if (configPath != null) {
                config = Config.fromJsonFile(Paths.get(configPath));
            } else {
                // load default config
                config = Config.loadDefault(provider);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            System.exit(1);
        }

        final boolean hasBillingPeriods = !dataframe.schema().getFieldIndex("BILLING_PERIOD").isEmpty();

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

        if (hasBillingPeriods) {
            writer = writer.partitionBy("BILLING_PERIOD");
        }

        if (provider == Provider.AZURE) {
            writer.csv(outputPath);
        } else {
            writer.parquet(outputPath);
        }

        spark.stop();
    }
}
