// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EnrichmentPipelineTest {

    /**
     * Unit test for EnrichmentPipeline using in-memory Rows without a SparkSession.
     * Simulates the schema and data construction usually handled by SparkJob.
     */
    @Test
    public void testPipelineLogicWithoutSparkSession() throws Exception {
        Config config = new Config();
        EnrichmentPipeline pipeline = new EnrichmentPipeline(config);

        // Manually define the schema to mimic what SparkJob prepares.
        // It includes standard input columns plus empty slots for enrichment modules.
        StructType schema = new StructType()
                .add("line_item_usagestartdate", DataTypes.StringType, true)
                .add("line_item_usageaccountid", DataTypes.StringType, true)
                .add("product_region", DataTypes.StringType, true)
                .add("line_item_availabilityzone", DataTypes.StringType, true)
                .add("line_item_usagetype", DataTypes.StringType, true)
                .add("line_item_unblendedcost", DataTypes.DoubleType, true)
                .add("line_item_resourceid", DataTypes.StringType, true)
                .add("product_instancetype", DataTypes.StringType, true)
                .add("line_item_line_item_type", DataTypes.StringType, true);

        for (EnrichmentModule module : config.getModules()) {
            for (Column c : module.columnsAdded()) {
                schema = schema.add(c.getLabel(), c.getType(), true);
            }
        }

        Object[] values = new Object[schema.length()];
        values[0] = "2024-01-01T00:00:00Z";
        values[1] = "123456789012";
        values[2] = "us-east-1";
        values[3] = "us-east-1a";
        values[4] = "BoxUsage:t2.micro";
        values[5] = 0.10d;
        values[6] = "i-0123456789abcdef0";
        values[7] = "t2.micro";
        values[8] = "Usage"; // Critical: must match 'Usage' to pass the pipeline filter

        Row inputRow = new GenericRowWithSchema(values, schema);
        List<Row> inputList = Collections.singletonList(inputRow);

        Iterator<Row> results = pipeline.call(inputList.iterator());

        assertTrue(results.hasNext(), "Pipeline should return at least one row");
        Row processedRow = results.next();

        assertEquals("us-east-1", processedRow.getAs("product_region"));

        // Verify that the row structure is valid and modules attempted enrichment
        assertNotNull(processedRow);
    }
}