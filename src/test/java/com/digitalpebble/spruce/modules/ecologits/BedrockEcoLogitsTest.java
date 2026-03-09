// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import com.digitalpebble.spruce.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class BedrockEcoLogitsTest {

    private BedrockEcoLogits module;
    private StructType schema;

    // Valid Map for simulate the PRODUCT column
    private static final Map<String, String> VALID_PRODUCT_MAP = Map.of("model", "anthropic.claude-v2");

    @BeforeEach
    void setUp() {
        module = new BedrockEcoLogits();
        schema = Utils.getSchema(module);
        module.init(new HashMap<>());
    }

    /**
     * Creates a {@link Row} matching the schema produced by {@link Utils#getSchema(EnrichmentModule)}
     *
     * <p>Schema order: {@code LINE_ITEM_PRODUCT_CODE}, {@code PRODUCT}, {@code USAGE_AMOUNT}, {@code PRICING_UNIT},
     * {@code LINE_ITEM_USAGE_TYPE}, {@code ENERGY_USED}, {@code EMBODIED_EMISSIONS}
     */
    static Row createRow(StructType schema, String productCode, Map<String, String> productMap,
                         Double usageAmount, String pricingUnit, String usageType) {
        Object[] values = new Object[7];
        values[0] = productCode;
        values[1] = productMap;
        values[2] = usageAmount;
        values[3] = pricingUnit;
        values[4] = usageType;
        values[5] = null;
        values[6] = null;
        return new GenericRowWithSchema(values, schema);
    }

    @Test
    void testColumnsNeeded() {
        Column[] needed = module.columnsNeeded();
        assertEquals(5, needed.length);
        assertEquals(CURColumn.LINE_ITEM_PRODUCT_CODE, needed[0]);
        assertEquals(CURColumn.PRODUCT, needed[1]);
        assertEquals(CURColumn.USAGE_AMOUNT, needed[2]);
        assertEquals(CURColumn.PRICING_UNIT, needed[3]);
        assertEquals(CURColumn.LINE_ITEM_USAGE_TYPE, needed[4]);
    }

    @Test
    void testColumnsAdded() {
        Column[] added = module.columnsAdded();
        assertEquals(2, added.length);
        assertEquals(SpruceColumn.ENERGY_USED, added[0]);
        assertEquals(SpruceColumn.EMBODIED_EMISSIONS, added[1]);
    }

    @ParameterizedTest
    @MethodSource("nullValueTestCases")
    void testProcessWithNullValues(String productCode, Map<String, String> productMap, Double usageAmount) {
        Row row = createRow(schema, productCode, productMap, usageAmount, "1K tokens", "input");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty(), "Should skip rows with invalid null values");
    }

    static Stream<Arguments> nullValueTestCases() {
        return Stream.of(
                Arguments.of(null, VALID_PRODUCT_MAP, 1.0),
                Arguments.of("AmazonBedrock", null, 1.0),
                Arguments.of("AmazonBedrock", VALID_PRODUCT_MAP, null)
        );
    }

    @ParameterizedTest
    @MethodSource("unsupportedValueTestCases")
    void testProcessWithUnsupportedValues(String productCode, Map<String, String> productMap, Double usageAmount) {
        Row row = createRow(schema, productCode, productMap, usageAmount, "1K tokens", "input");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty(), "Should skip unsupported products or unknown models");
    }

    static Stream<Arguments> unsupportedValueTestCases() {
        return Stream.of(
                Arguments.of("AmazonEC2", VALID_PRODUCT_MAP, 1.0),
                Arguments.of("AmazonS3", VALID_PRODUCT_MAP, 1.0),
                Arguments.of("AmazonBedrock", Map.of("model", "unknown.model-v99"), 1.0),
                Arguments.of("AmazonBedrock", VALID_PRODUCT_MAP, 0.0),
                Arguments.of("AmazonBedrock", VALID_PRODUCT_MAP, -5.0)
        );
    }

    @Test
    void testEnrichesInputTokens() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "EUN1-Claude-input-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
        assertEquals(0.00015, (Double) enriched.get(SpruceColumn.ENERGY_USED), 1e-9);
    }

    @Test
    void testEnrichesOutputTokens() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "EUN1-Claude-output-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
        assertEquals(0.00080, (Double) enriched.get(SpruceColumn.ENERGY_USED), 1e-9);
    }

    @Test
    void testEnrichesFallbackSplit() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "generic-tokens-usage");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
        assertEquals(0.000475, (Double) enriched.get(SpruceColumn.ENERGY_USED), 1e-9);
    }

    @Test
    void testEnrichesEmbodiedEmissions() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "input");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(SpruceColumn.EMBODIED_EMISSIONS));
        assertEquals(0.005, (Double) enriched.get(SpruceColumn.EMBODIED_EMISSIONS), 1e-9);
    }

    @Test
    void testAccumulatesEnergyWithExistingValue() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "input");
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(SpruceColumn.ENERGY_USED, 10.0);
        enriched.put(SpruceColumn.EMBODIED_EMISSIONS, 5.0);

        module.enrich(row, enriched);

        assertEquals(10.0 + 0.00015, (Double) enriched.get(SpruceColumn.ENERGY_USED), 1e-9);
        assertEquals(5.0 + 0.005, (Double) enriched.get(SpruceColumn.EMBODIED_EMISSIONS), 1e-9);
    }

    @Test
    void testScalesWithMillionsMultiplier() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 2.0, "1M tokens", "input");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertEquals(0.3, (Double) enriched.get(SpruceColumn.ENERGY_USED), 1e-9);
    }
}