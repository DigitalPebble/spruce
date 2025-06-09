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

package com.digitalpebble;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

public class Carbonara {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: Carbonara <inputPath> <outputPath>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        SparkSession spark = SparkSession.builder()
                .appName("Carbonara")
                .master("local[*]")
                .getOrCreate();

        // Read the input Parquet file(s)
        Dataset<Row> parquetFileDF = spark.read().parquet(inputPath);

        // Add energy usage estimates

        StructType newType = parquetFileDF.schema().add("energy_usage_kwh", DoubleType);

        Dataset<Row> energyDF = parquetFileDF.map(
                (MapFunction<Row, Row>) row -> row,
                Encoders.row(newType));

        energyDF.printSchema();

        // Add carbon estimates

        // Add embedded carbon

        // Add water estimates

        // Write the result to Parquet
        energyDF.write().mode("overwrite").parquet(outputPath);

        spark.stop();
    }
}
