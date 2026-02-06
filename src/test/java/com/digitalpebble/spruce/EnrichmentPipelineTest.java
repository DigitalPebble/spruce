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
 */
public class EnrichmentPipelineTest {

    /**
     * Verifies the selective enrichment logic based on Line Item Type.
     * <p>
     * Ensures that only 'Usage' items trigger the enrichment process.
     * Non-usage items must bypass enrichment, leaving Spruce-specific columns null.
     *
     * @param lineItemType     The input line item type to test (e.g., "Usage", "Tax")
     * @param expectEnrichment Whether the pipeline is expected to populate enrichment columns
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

        // Minimal schema: use ONLY existing constants in CURColumn.
        StructType schema = new StructType()
                .add(CURColumn.PRODUCT_REGION_CODE.getLabel(), CURColumn.PRODUCT_REGION_CODE.getType(), true)
                .add(CURColumn.LINE_ITEM_TYPE.getLabel(), CURColumn.LINE_ITEM_TYPE.getType(), true);

        for (EnrichmentModule module : config.getModules()) {
            for (Column c : module.columnsAdded()) {
                schema = schema.add(c.getLabel(), c.getType(), true);
            }
        }

        Object[] values = new Object[schema.length()];
        values[0] = "us-east-1";          // Matches CURColumn.PRODUCT_REGION_CODE
        values[1] = lineItemType;         // The dynamic parameter

        Row inputRow = new GenericRowWithSchema(values, schema);
        List<Row> inputList = Collections.singletonList(inputRow);

        Iterator<Row> results = pipeline.call(inputList.iterator());

        assertTrue(results.hasNext(), "Pipeline should always return a row");
        Row processedRow = results.next();

        assertNotNull(processedRow);
        assertEquals("us-east-1", processedRow.getAs(CURColumn.PRODUCT_REGION_CODE.getLabel()));

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