// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.lit;


public class SparkJob {

    public static void main(String[] args) {

        // TODO use a command line parser
        if (args.length < 3) {
            System.err.println("Usage: SparkJob <config_file> <inputPath> <outputPath>");
            System.exit(1);
        }

        String configPath = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("Spruce")
                .getOrCreate();

        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // Read the input Parquet file(s)
        Dataset<Row> dataframe = spark.read().parquet(inputPath);

        // define and configure modules via configuration
        Config config = null;
        try {
            config = Config.fromJsonFile(Paths.get(configPath));
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
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
        enriched.write().partitionBy("BILLING_PERIOD").mode("overwrite").parquet(outputPath);

        spark.stop();
    }
}
