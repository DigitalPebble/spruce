/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara;

import com.digitalpebble.carbonara.modules.ConstantLoad;
import com.digitalpebble.carbonara.modules.boavizta.BoaviztAPI;
import com.digitalpebble.carbonara.modules.ccf.Networking;
import com.digitalpebble.carbonara.modules.electricitymaps.AverageCarbonIntensity;
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
                .master("local[*]")
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
                new ConstantLoad(),
                new Networking(),
                new BoaviztAPI(),
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

        EnrichementPipeline pipeline = new EnrichementPipeline(modules, config);
        Encoder<Row> encoder = RowEncoder.encoderFor(finalSchema);

        Dataset<Row> enriched = dataframe.mapPartitions(pipeline, encoder);

        // Write the result as Parquet, with one subdirectory per billing period, similar to the input
        enriched.write().partitionBy("BILLING_PERIOD").mode("overwrite").parquet(outputPath);

        spark.stop();
    }
}
