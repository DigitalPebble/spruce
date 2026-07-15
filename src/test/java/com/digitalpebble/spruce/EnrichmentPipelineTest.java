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
import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@link EnrichmentPipeline}.
 */
public class EnrichmentPipelineTest {

    /** A lightweight test column used by DummyModule to verify enrichment. */
    private static final Column DUMMY_ENRICHED = new Column("dummy_enriched", DataTypes.DoubleType) {};

    /**
     * Minimal {@link EnrichmentModule} for testing pipeline logic.
     * Declares a single output column and sets it to a fixed value during process().
     */
    static class DummyModule implements EnrichmentModule {

        @Override
        public Column[] columnsNeeded() {
            return new Column[0];
        }

        @Override
        public Column[] columnsAdded() {
            return new Column[]{ DUMMY_ENRICHED };
        }

        @Override
        public void enrich(Row row, Map<Column, Object> enrichedValues) {
            enrichedValues.put(DUMMY_ENRICHED, 42.0);
        }
    }

    /**
     * Creates a Config pre-loaded with a single DummyModule.
     * This avoids loading the real production modules (which require the full CUR schema)
     * and keeps the test focused on the pipeline's Usage-filter logic.
     */
    private static Config createMockConfig() {
        Config config = new Config();
        config.setProvider(Provider.AWS);
        DummyModule module = new DummyModule();
        config.getModules().add(module);

        // Also inject matching config entry via reflection so configureModules() works
        try {
            var configsField = Config.class.getDeclaredField("configs");
            configsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> configs =
                    (List<Map<String, Object>>) configsField.get(config);
            configs.add(Collections.emptyMap());
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up mock config", e);
        }

        return config;
    }

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
        Config config = createMockConfig();
        EnrichmentPipeline pipeline = new EnrichmentPipeline(config);

        List<EnrichmentModule> modules = config.getModules();
        assertFalse(modules.isEmpty(), "Config should have at least one enrichment module for this test");

        // Minimal schema: base CUR columns + columns added by the DummyModule
        StructType schema = new StructType()
                .add(PRODUCT_REGION_CODE.getLabel(), PRODUCT_REGION_CODE.getType(), true)
                .add(LINE_ITEM_TYPE.getLabel(), LINE_ITEM_TYPE.getType(), true);

        for (EnrichmentModule module : modules) {
            for (Column c : module.columnsAdded()) {
                schema = schema.add(c.getLabel(), c.getType(), true);
            }
        }

        Object[] values = new Object[schema.length()];
        values[schema.fieldIndex(PRODUCT_REGION_CODE.getLabel())] = "us-east-1";
        values[schema.fieldIndex(LINE_ITEM_TYPE.getLabel())] = lineItemType;

        Row row = new GenericRowWithSchema(values, schema);
        List<Row> inputList = Collections.singletonList(row);

        Iterator<Row> results = pipeline.call(inputList.iterator());

        assertTrue(results.hasNext(), "Pipeline should always return a row");
        Row processedRow = results.next();

        assertNotNull(processedRow);
        assertEquals("us-east-1", PRODUCT_REGION_CODE.getString(processedRow));

        // Verify enrichment logic
        for (EnrichmentModule module : modules) {
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

    /**
     * Verifies the usage filter for FOCUS reports, which relies on the standard ChargeCategory
     * column regardless of the provider.
     */
    @ParameterizedTest
    @CsvSource({
            "Usage, true",
            "Purchase, false",
            "Tax, false",
            "Credit, false",
            "Adjustment, false"
    })
    public void testPipelineFocusUsageFilter(String chargeCategory, boolean expectEnrichment) throws Exception {
        Config config = createMockConfig();
        config.setProvider(Provider.AZURE);
        config.setReportFormat(ReportFormat.FOCUS);
        EnrichmentPipeline pipeline = new EnrichmentPipeline(config);

        StructType schema = new StructType()
                .add(FOCUSColumn.CHARGE_CATEGORY.getLabel(), FOCUSColumn.CHARGE_CATEGORY.getType(), true)
                .add(DUMMY_ENRICHED.getLabel(), DUMMY_ENRICHED.getType(), true);

        Object[] values = new Object[]{chargeCategory, null};
        Row row = new GenericRowWithSchema(values, schema);

        Iterator<Row> results = pipeline.call(Collections.singletonList(row).iterator());

        assertTrue(results.hasNext(), "Pipeline should always return a row");
        Row processedRow = results.next();

        int fieldIndex = processedRow.fieldIndex(DUMMY_ENRICHED.getLabel());
        assertEquals(expectEnrichment, !processedRow.isNullAt(fieldIndex),
                () -> "ChargeCategory " + chargeCategory + " should " + (expectEnrichment ? "" : "not ")
                        + "trigger enrichment");
    }
}