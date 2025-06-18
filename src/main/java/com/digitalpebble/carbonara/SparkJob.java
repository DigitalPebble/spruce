/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara;

import com.digitalpebble.carbonara.modules.ccf.Networking;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;



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

        // Read the input Parquet file(s)
        Dataset<Row> dataframe = spark.read().parquet(inputPath);

        // TODO define and configure modules via configuration

        // Add energy usage estimates
        // Add carbon estimates
        // Add embedded carbon
        // compute emissions

        List<EnrichmentModule> modules = List.of(
                new Networking()
        );

        for (EnrichmentModule module : modules) {
            // add new columns for the current module
            // with the correct type but a value of null
            for ( Column c : module.columnsAdded()){
                dataframe = dataframe.withColumn(c.label, lit(null).cast(c.type));
            }
        }

        StructType finalSchema = dataframe.schema();

        EnrichementPipeline pipeline = new EnrichementPipeline(modules);
        Encoder<Row> encoder = RowEncoder.encoderFor(finalSchema);

        Dataset<Row> enriched = dataframe.mapPartitions(pipeline, encoder);

        // Write the result as Parquet, with one subdirectory per billing period, similar to the input
        enriched.write().partitionBy("BILLING_PERIOD").mode("overwrite").parquet(outputPath);

        spark.stop();
    }
}
