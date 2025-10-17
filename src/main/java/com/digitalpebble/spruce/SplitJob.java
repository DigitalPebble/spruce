// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import com.esotericsoftware.minlog.Log;
import org.apache.commons.cli.*;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import scala.Option;

import java.io.Serializable;
import java.util.*;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.apache.spark.sql.functions.lit;

/**
 * Reads an enriched file containing split line items and give them a share of the impacts of the resources
 * they are related to.
 **/
public class SplitJob {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SplitJob.class);

    public static void main(String[] args) {

        final Options options = new Options();
        options.addRequiredOption("i", "input", true, "input path");
        options.addRequiredOption("o", "output", true, "output path");

        String inputPath = null;
        String outputPath = null;

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            inputPath = cmd.getOptionValue("i");
            outputPath = cmd.getOptionValue("o");
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SplitJob", options);
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("SPRUCE-split").master("local[*]") // run locally, can step through
                .getOrCreate();

        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // Read the input Parquet file(s)
        // csv for now
        Dataset<Row> dataframe = spark.read().option("header", true).csv(inputPath);

        final boolean hasBillingPeriods = !dataframe.schema().getFieldIndex("BILLING_PERIOD").isEmpty();

        // check that the input contains splits
        boolean noSplits = dataframe.schema().getFieldIndex("split_line_item_parent_resource_id").isEmpty();

        if (noSplits) {
            LOG.error("Input files do not have split line items");
            System.exit(1);
        }

        final Column[] impactColumns = new Column[]{ENERGY_USED, OPERATIONAL_EMISSIONS, EMBODIED_EMISSIONS};
        for (Column c : impactColumns) {
            Option<Object> index = dataframe.schema().getFieldIndex(c.getLabel());
            if (index.isEmpty()) {
                LOG.error("Missing column: '{}'", c.getLabel());
                // System.exit(2);
            }
            // create separate columns to avoid double counting
            // dataframe = dataframe.withColumn("split_" + c.getLabel(), lit(null).cast(c.getType()));
        }

        Encoder<Row> encoder = RowEncoder.encoderFor(dataframe.schema());

        KeyValueGroupedDataset<GroupKey, Row> grouped = dataframe.groupByKey((MapFunction<Row, GroupKey>) row -> {
                    String date = row.getAs("identity_time_interval");

                    // COALESCE: if split_line_item_parent_resource_id is null, use identity_line_item_id
                    String resourceId = row.getAs("split_line_item_parent_resource_id");
                    if (resourceId == null) {
                        resourceId = row.getAs("identity_line_item_id");
                    }

                    return new GroupKey(date, resourceId);
                }, Encoders.javaSerialization(GroupKey.class) // Encoder for the custom key
        );

        // Aggregate parent values
        final String[] impactNames = Arrays.stream(impactColumns)
                .map(Column::getLabel)
                .toArray(String[]::new);

        // Step 3: FlatMapGroups to perform custom aggregation
        Dataset<Row> enriched = grouped.flatMapGroups((FlatMapGroupsFunction<GroupKey, Row, Row>) (key, iterator) -> {

            LOG.info("Group key resource {} date {}", key.getResourceId(), key.getDate());

            List<Row> rows = new ArrayList<>();
            iterator.forEachRemaining(rows::add);

            Map<String, Double> agg = ParentAggregator.aggregate(rows, impactNames);

            List<Row> output = new ArrayList<>();
            for (Row r : rows) {
                // Only enrich children
                if (r.getAs("split_line_item_parent_resource_id") != null) {
                    // parents can go straight out
                    output.add(r);
                    continue;
                }

                // TODO apply logic

                // TODO add to output
                // output.add(RowFactory.create(values));
            }
            return output.iterator();
        }, encoder);


        // Write the result as Parquet, with one subdirectory per billing period, similar to the input
        DataFrameWriter<Row> writer = enriched.write().mode("overwrite");

        if (hasBillingPeriods) {
            writer = writer.partitionBy("BILLING_PERIOD");
        }

        writer.parquet(outputPath);

        spark.stop();
    }
}

class ParentAggregator {

    /**
     * Aggregate parent values for a group
     * There should be 1 EC2 instance run and potentially several other items e.g. network
     * Impacts contain energy, and emission estimates
     */
    public static Map<String, Double> aggregate(List<Row> rows, String[] impactColumns) {
        Map<String, Double> agg = new HashMap<>();

        for (String column : impactColumns){
            agg.put(column, 0.0);
        }

        for (Row r : rows) {
            // parents only
            if (!r.isNullAt(r.fieldIndex("split_line_item_parent_resource_id"))) {
                continue;
            }
            // for each impact
            for (String column : impactColumns){
                Double val = r.getAs(column);
                agg.compute(column, (label, value) -> value + val);
            }
        }
        return agg;
    }
}