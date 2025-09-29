// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.commons.cli.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

        String configPath = null;
        String inputPath = null;
        String outputPath = null;

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            configPath = cmd.getOptionValue("c");
            inputPath = cmd.getOptionValue("i");
            outputPath = cmd.getOptionValue("o");
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SparkJob", options);
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("Spruce")
                .getOrCreate();

        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // Read the input Parquet file(s)
        Dataset<Row> dataframe = spark.read().parquet(inputPath);

        // define and configure modules via configuration
        Config config = null;
        try {
            // explicitly set by user
            if (configPath != null) {
                config = Config.fromJsonFile(Paths.get(configPath));
            } else {
                // load default config
                config = Config.loadDefault();
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
        enriched.write().partitionBy("BILLING_PERIOD").mode("overwrite").parquet(outputPath);

        spark.stop();
    }
}
