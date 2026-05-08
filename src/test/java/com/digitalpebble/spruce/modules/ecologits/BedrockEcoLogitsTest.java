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

import static com.digitalpebble.spruce.SpruceColumn.EMBODIED_EMISSIONS;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.*;

public class BedrockEcoLogitsTest {

    private BedrockEcoLogits module;
    private StructType schema;

    private static final String TEST_MAPPING = "ecologits-test/mapping.csv";
    private static final String TEST_COEFFICIENTS = "ecologits-test/coefficients.csv";

    // Coefficients in test-coefficients.csv: 1e-3 kWh and 5e-4 kg (=0.5 g) per 1k output tokens.
    private static final double OUTPUT_ENERGY_PER_1K = 1.0e-3;
    private static final double OUTPUT_EMBODIED_G_PER_1K = 0.5;

    @BeforeEach
    void setUp() {
        module = new BedrockEcoLogits();
        schema = Utils.getSchema(module);
        EcoLogits impacts = new EcoLogits(TEST_MAPPING, TEST_COEFFICIENTS);
        impacts.load();
        module.setEcoLogits(impacts);
        module.init(new HashMap<>());
    }

    /**
     * Creates a {@link Row} matching the schema produced by {@link Utils#getSchema(EnrichmentModule)}
     *
     * <p>Schema order: {@code LINE_ITEM_PRODUCT_CODE}, {@code USAGE_AMOUNT}, {@code PRICING_UNIT},
     * {@code LINE_ITEM_USAGE_TYPE}, {@code ENERGY_USED}, {@code EMBODIED_EMISSIONS}
     */
    static Row createRow(StructType schema, String productCode, Double usageAmount,
                         String pricingUnit, String usageType) {
        Object[] values = new Object[6];
        values[0] = productCode;
        values[1] = usageAmount;
        values[2] = pricingUnit;
        values[3] = usageType;
        values[4] = null;
        values[5] = null;
        return new GenericRowWithSchema(values, schema);
    }

    @Test
    void testColumnsNeeded() {
        Column[] needed = module.columnsNeeded();
        assertEquals(4, needed.length);
        assertEquals(CURColumn.LINE_ITEM_PRODUCT_CODE, needed[0]);
        assertEquals(CURColumn.USAGE_AMOUNT, needed[1]);
        assertEquals(CURColumn.PRICING_UNIT, needed[2]);
        assertEquals(CURColumn.LINE_ITEM_USAGE_TYPE, needed[3]);
    }

    @Test
    void testColumnsAdded() {
        Column[] added = module.columnsAdded();
        assertEquals(2, added.length);
        assertEquals(ENERGY_USED, added[0]);
        assertEquals(EMBODIED_EMISSIONS, added[1]);
    }

    @ParameterizedTest
    @MethodSource("nullValueTestCases")
    void testProcessWithNullValues(String productCode, Double usageAmount, String usageType) {
        Row row = createRow(schema, productCode, usageAmount, "1K tokens", usageType);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty(), "Should skip rows with invalid null values");
    }

    static Stream<Arguments> nullValueTestCases() {
        return Stream.of(
                Arguments.of(null, 1.0, "USE1-Claude-input-tokens"),
                Arguments.of("AmazonBedrock", null, "USE1-Claude-input-tokens"),
                Arguments.of("AmazonBedrock", 1.0, null)
        );
    }

    @ParameterizedTest
    @MethodSource("unsupportedValueTestCases")
    void testProcessWithUnsupportedValues(String productCode, Double usageAmount, String usageType) {
        Row row = createRow(schema, productCode, usageAmount, "1K tokens", usageType);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty(), "Should skip unsupported products or unknown models");
    }

    static Stream<Arguments> unsupportedValueTestCases() {
        return Stream.of(
                Arguments.of("AmazonEC2", 1.0, "USE1-Claude-input-tokens"),
                Arguments.of("AmazonS3", 1.0, "USE1-Claude-input-tokens"),
                Arguments.of("AmazonBedrock", 1.0, "USE1-UnknownModel-input-tokens"),
                Arguments.of("AmazonBedrock", 0.0, "USE1-Claude-input-tokens"),
                Arguments.of("AmazonBedrock", -5.0, "USE1-Claude-input-tokens")
        );
    }

    @Test
    void testEnrichesOutputTokens() {
        Row row = createRow(schema, "AmazonBedrock", 1.0, "1K tokens", "EUN1-Claude-output-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(ENERGY_USED));
        assertEquals(OUTPUT_ENERGY_PER_1K, ENERGY_USED.getDouble(enriched), 1e-12);
    }

    @Test
    void testSkipsInputTokens() {
        // EcoLogits attributes ~all generation cost to output tokens; input rows are ignored.
        Row row = createRow(schema, "AmazonBedrock", 1.0, "1K tokens", "EUN1-Claude-input-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty());
    }

    @Test
    void testEnrichesEmbodiedEmissions() {
        Row row = createRow(schema, "AmazonBedrock", 1.0, "1K tokens", "EUN1-Claude-output-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(EMBODIED_EMISSIONS));
        assertEquals(OUTPUT_EMBODIED_G_PER_1K, EMBODIED_EMISSIONS.getDouble(enriched), 1e-12);
    }

    @Test
    void testOverwritesExistingEnergyValue() {
        Row row = createRow(schema, "AmazonBedrock", 1.0, "1K tokens", "EUN1-Claude-output-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 10.0);
        enriched.put(EMBODIED_EMISSIONS, 5.0);

        module.enrich(row, enriched);

        assertEquals(OUTPUT_ENERGY_PER_1K, ENERGY_USED.getDouble(enriched), 1e-12);
        assertEquals(OUTPUT_EMBODIED_G_PER_1K, EMBODIED_EMISSIONS.getDouble(enriched), 1e-12);
    }

    @Test
    void testScalesWithMillionsMultiplier() {
        Row row = createRow(schema, "AmazonBedrock", 2.0, "1M tokens", "EUN1-Claude-output-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        // 2.0 × 1_000_000 tokens = 2000 × 1k tokens
        double expected = 2000.0 * OUTPUT_ENERGY_PER_1K;
        assertEquals(expected, ENERGY_USED.getDouble(enriched), 1e-12);
    }

    @ParameterizedTest
    @MethodSource("parseUsageTypeCases")
    void testParseUsageType(String usageType, String expectedModelKey, String expectedTokenType) {
        String[] result = BedrockEcoLogits.parseUsageType(usageType);
        if (expectedModelKey == null) {
            assertNull(result);
        } else {
            assertNotNull(result);
            assertEquals(expectedModelKey, result[0]);
            assertEquals(expectedTokenType, result[1]);
        }
    }

    static Stream<Arguments> parseUsageTypeCases() {
        return Stream.of(
                Arguments.of("EUW2-Mistral7B-output-tokens", "Mistral7B", "output"),
                Arguments.of("EUW2-Mistral7B-input-tokens", "Mistral7B", "input"),
                Arguments.of("EU-Qwen3-32B-output-tokens-batch", "Qwen3-32B", "output"),
                Arguments.of("EU-Qwen3-32B-input-tokens-batch", "Qwen3-32B", "input"),
                Arguments.of("EUW2-Qwen3-VL-235B-A22B-input-tokens", "Qwen3-VL-235B-A22B", "input"),
                Arguments.of("EUN1-Claude-output-tokens", "Claude", "output"),
                Arguments.of(null, null, null),
                Arguments.of("nodash", null, null),
                Arguments.of("generic-tokens-usage", null, null)
        );
    }
}
