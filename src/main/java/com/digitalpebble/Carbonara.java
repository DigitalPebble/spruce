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
        energyDF.write().parquet(outputPath);

        spark.stop();
    }
}
