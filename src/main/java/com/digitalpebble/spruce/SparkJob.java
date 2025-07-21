// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import com.digitalpebble.spruce.modules.ConstantLoad;
import com.digitalpebble.spruce.modules.ccf.Networking;
import com.digitalpebble.spruce.modules.ccf.Storage;
import com.digitalpebble.spruce.modules.electricitymaps.AverageCarbonIntensity;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.lit;


public class SparkJob {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SparkJob <inputPath> <outputPath>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        SparkSession spark = SparkSession.builder()
                .appName("Carbonara")
                .getOrCreate();

        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // Read the input Parquet file(s)
        Dataset<Row> dataframe = spark.read().parquet(inputPath);

        // TODO define and configure modules via configuration

        // TODO create configuration object to pass parameters to modules
        final Map<String, String> config = ImmutableMap.of();

        // Add energy usage estimates
        // Add carbon estimates
        // Add embedded carbon
        // compute emissions

        final List<EnrichmentModule> modules = List.of(
                new Storage(),
                new Networking(),
                new AverageCarbonIntensity()
        );

        for (EnrichmentModule module : modules) {
            // add new columns for the current module
            // with the correct type but a value of null
            for (Column c : module.columnsAdded()) {
                dataframe = dataframe.withColumn(c.getLabel(), lit(null).cast(c.getType()));
            }
        }

        StructType finalSchema = dataframe.schema();

        EnrichmentPipeline pipeline = new EnrichmentPipeline(modules, config);
        Encoder<Row> encoder = RowEncoder.encoderFor(finalSchema);

        Dataset<Row> enriched = dataframe.mapPartitions(pipeline, encoder);

        // Write the result as Parquet, with one subdirectory per billing period, similar to the input
        enriched.write().partitionBy("BILLING_PERIOD").mode("overwrite").parquet(outputPath);

        spark.stop();
    }
}
