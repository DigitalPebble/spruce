// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
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

/**
 * Unit tests for the {@link EnrichmentPipeline}.
 * Verifies the integration between Spark schema definitions and the enrichment logic.
 */
public class EnrichmentPipelineTest {

    /**
     * Verifies that the pipeline correctly filters which rows to enrich based on the Line Item Type.
     * <p>
     * Only 'Usage' items should trigger the enrichment modules (populating Spruce columns).
     * Other types (Tax, Fee) should be passed through with Spruce columns remaining null.
     *
     * @param lineItemType     The input line_item_type (e.g., "Usage", "Tax")
     * @param expectEnrichment True if the pipeline is expected to populate extra columns, false otherwise
     */
    @ParameterizedTest
    @CsvSource({
            "Usage, true",
            "Tax, false",
            "Fee, false"
    })
    public void testPipelineEnrichmentLogic(String lineItemType, boolean expectEnrichment) throws Exception {
        Config config = new Config();
        EnrichmentPipeline pipeline = new EnrichmentPipeline(config);

        // Define input schema using CURColumn constants to ensure consistency with production code
        StructType schema = new StructType()
                .add(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(), CURColumn.LINE_ITEM_USAGE_START_DATE.getType(), true)
                .add(CURColumn.LINE_ITEM_USAGE_ACCOUNT_ID.getLabel(), CURColumn.LINE_ITEM_USAGE_ACCOUNT_ID.getType(), true)
                .add(CURColumn.PRODUCT_REGION.getLabel(), CURColumn.PRODUCT_REGION.getType(), true)
                .add(CURColumn.LINE_ITEM_AVAILABILITY_ZONE.getLabel(), CURColumn.LINE_ITEM_AVAILABILITY_ZONE.getType(), true)
                .add(CURColumn.LINE_ITEM_USAGE_TYPE.getLabel(), CURColumn.LINE_ITEM_USAGE_TYPE.getType(), true)
                .add(CURColumn.LINE_ITEM_UNBLENDED_COST.getLabel(), CURColumn.LINE_ITEM_UNBLENDED_COST.getType(), true)
                .add(CURColumn.LINE_ITEM_RESOURCE_ID.getLabel(), CURColumn.LINE_ITEM_RESOURCE_ID.getType(), true)
                .add(CURColumn.PRODUCT_INSTANCE_TYPE.getLabel(), CURColumn.PRODUCT_INSTANCE_TYPE.getType(), true)
                .add(CURColumn.LINE_ITEM_TYPE.getLabel(), CURColumn.LINE_ITEM_TYPE.getType(), true);

        for (EnrichmentModule module : config.getModules()) {
            for (Column c : module.columnsAdded()) {
                schema = schema.add(c.getLabel(), c.getType(), true);
            }
        }

        // WARNING: The array order MUST strictly match the schema definition order above.
        // Any change to the StructType requires an update here.
        Object[] values = new Object[schema.length()];
        values[0] = "2024-01-01T00:00:00Z";       // usage_start_date
        values[1] = "123456789012";               // usage_account_id
        values[2] = "us-east-1";                  // product_region
        values[3] = "us-east-1a";                 // availability_zone
        values[4] = "BoxUsage:t2.micro";          // usage_type
        values[5] = 0.10d;                        // unblended_cost
        values[6] = "i-0123456789abcdef0";        // resource_id
        values[7] = "t2.micro";                   // instance_type
        values[8] = lineItemType;                 // line_item_type (Dynamic Parameter)

        Row inputRow = new GenericRowWithSchema(values, schema);
        List<Row> inputList = Collections.singletonList(inputRow);

        Iterator<Row> results = pipeline.call(inputList.iterator());

        assertTrue(results.hasNext(), "Pipeline should always return a row (map operation, not filter)");
        Row processedRow = results.next();

        assertNotNull(processedRow);
        assertEquals("us-east-1", processedRow.getAs(CURColumn.PRODUCT_REGION.getLabel()));

        // Verify enrichment logic
        for (EnrichmentModule module : config.getModules()) {
            for (Column c : module.columnsAdded()) {
                int fieldIndex = processedRow.fieldIndex(c.getLabel());
                if (expectEnrichment) {
                    assertFalse(processedRow.isNullAt(fieldIndex),
                            () -> "Enrichment failed: Column " + c.getLabel() + " should be populated for Usage");
                } else {
                    assertTrue(processedRow.isNullAt(fieldIndex),
                            () -> "Enrichment leak: Column " + c.getLabel() + " should be null for " + lineItemType);
                }
            }
        }
    }
}