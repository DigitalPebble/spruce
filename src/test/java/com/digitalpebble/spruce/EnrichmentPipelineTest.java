// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EnrichmentPipelineTest {

    /**
     * Parameterized Unit Test using in-memory Rows.
     * Uses @CsvSource to simulate different input scenarios (Filtering & Enrichment).
     *
     * @param lineItemType The type of the line item (e.g., "Usage", "Tax").
     * @param shouldBeEnriched Boolean flag indicating if the row should be enriched (true) or just passed through (false).
     */
    @ParameterizedTest
    @CsvSource({
            "Usage, true",   // Should be enriched
            "Tax, false",    // Should be passed through but NOT enriched
            "Fee, false"     // Should be passed through but NOT enriched
    })
    public void testPipelineLogic(String lineItemType, boolean shouldBeEnriched) throws Exception {
        Config config = new Config();
        EnrichmentPipeline pipeline = new EnrichmentPipeline(config);

        // We use CURColumn constants where available. For missing columns (e.g. Cost, Dates),
        // we use manual string definition to match standard CUR report formats.
        StructType schema = new StructType()
                .add("line_item_usagestartdate", DataTypes.StringType, true)
                .add("line_item_usageaccountid", DataTypes.StringType, true)
                .add("product_region", DataTypes.StringType, true)
                .add("line_item_availabilityzone", DataTypes.StringType, true)
                .add(CURColumn.LINE_ITEM_USAGE_TYPE.getLabel(), CURColumn.LINE_ITEM_USAGE_TYPE.getType(), true)
                .add("line_item_unblendedcost", DataTypes.DoubleType, true)
                .add("line_item_resourceid", DataTypes.StringType, true)
                .add(CURColumn.PRODUCT_INSTANCE_TYPE.getLabel(), CURColumn.PRODUCT_INSTANCE_TYPE.getType(), true)
                .add(CURColumn.LINE_ITEM_TYPE.getLabel(), CURColumn.LINE_ITEM_TYPE.getType(), true);

        for (EnrichmentModule module : config.getModules()) {
            for (Column c : module.columnsAdded()) {
                schema = schema.add(c.getLabel(), c.getType(), true);
            }
        }

        // IMPORTANT: The order of values must match the order of .add() calls above
        Object[] values = new Object[schema.length()];
        values[0] = "2024-01-01T00:00:00Z";       // usage_start_date
        values[1] = "123456789012";               // usage_account_id
        values[2] = "us-east-1";                  // product_region
        values[3] = "us-east-1a";                 // availability_zone
        values[4] = "BoxUsage:t2.micro";          // usage_type (CURColumn)
        values[5] = 0.10d;                        // unblended_cost
        values[6] = "i-0123456789abcdef0";        // resource_id
        values[7] = "t2.micro";                   // instance_type (CURColumn)
        values[8] = lineItemType;                 // line_item_type (CURColumn) - Dynamic

        Row inputRow = new GenericRowWithSchema(values, schema);
        List<Row> inputList = Collections.singletonList(inputRow);

        Iterator<Row> results = pipeline.call(inputList.iterator());

        // The pipeline ALWAYS returns the row (it's a Map function, not a Filter),
        // but it only populates extra columns if it's a Usage.
        assertTrue(results.hasNext(), "Row should always be returned (not dropped)");
        Row processedRow = results.next();

        // Check basic integrity (always present)
        assertEquals("us-east-1", processedRow.getAs("product_region"));
        assertNotNull(processedRow);

        if (shouldBeEnriched) {
            // Positive Case: Spruce columns must be POPULATED (not null)
            for (EnrichmentModule module : config.getModules()) {
                for (Column c : module.columnsAdded()) {
                    int fieldIndex = processedRow.fieldIndex(c.getLabel());
                    assertFalse(processedRow.isNullAt(fieldIndex),
                            "Enriched column '" + c.getLabel() + "' should NOT be null for Usage");
                }
            }
        } else {
            // Negative Case: Spruce columns must remain NULL (skipped)
            for (EnrichmentModule module : config.getModules()) {
                for (Column c : module.columnsAdded()) {
                    int fieldIndex = processedRow.fieldIndex(c.getLabel());
                    assertTrue(processedRow.isNullAt(fieldIndex),
                            "Enriched column '" + c.getLabel() + "' MUST be null for non-usage types like " + lineItemType);
                }
            }
        }
    }
}