// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.utils;

import com.digitalpebble.spruce.*;
import com.digitalpebble.spruce.Column;
import org.apache.commons.cli.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import java.io.IOException;

import static org.apache.spark.sql.functions.lit;

/**
 * Generates mapping files SKUs -> impacts to be used with FOCUS
 * Takes as input ndjson files from the AWS price catalog. Runs a custom pipeline
 * This is meant to be used by SPRUCE maintainers to populate the resource data.
 **/

public class AwsSkuJob {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(AwsSkuJob.class);

    public static void main(String[] args) {

        final Options options = new Options();
        options.addRequiredOption("i", "input", true, "input path");
        options.addRequiredOption("o", "output", true, "output path");

        String inputPath = null;
        String outputPath = null;
        Provider provider = Provider.AWS;

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            inputPath = cmd.getOptionValue("i");
            outputPath = cmd.getOptionValue("o");
            String providerStr = cmd.getOptionValue("p", "AWS");
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("AwsSkuJob", options);
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("AwsSkuJob")
                .master("local[*]")
                .getOrCreate();

        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // Read the input: ndjson

        Dataset<Row> dataframe = spark.read().json(inputPath);

        // define and configure modules via configuration
        Config config = null;
        try {
            config = Config.loadDefault(provider);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (EnrichmentModule module : config.getModules()) {
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
        writer.csv(outputPath);

        spark.stop();
    }
}
