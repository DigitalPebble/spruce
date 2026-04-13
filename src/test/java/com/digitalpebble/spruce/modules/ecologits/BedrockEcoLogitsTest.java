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
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.digitalpebble.spruce.SpruceColumn.EMBODIED_EMISSIONS;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.*;

public class BedrockEcoLogitsTest {

    private BedrockEcoLogits module;
    private StructType schema;

    private static final String TEST_MAPPING = "ecologits-test/cur-model-mapping.csv";
    private static final String TEST_COEFFICIENTS = "ecologits-test/coefficients.csv";

    // The test fixture maps "anthropic.claude-v2" to test-provider/test-claude.
    private static final Map<String, String> VALID_PRODUCT_MAP = Map.of("model", "anthropic.claude-v2");

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

    private BedrockEcoLogits buildModule(Map<String, Object> params) {
        BedrockEcoLogits m = new BedrockEcoLogits();
        EcoLogits impacts = new EcoLogits(TEST_MAPPING, TEST_COEFFICIENTS);
        impacts.load();
        m.setEcoLogits(impacts);
        m.init(params);
        return m;
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
        values[1] = productMap != null ? JavaConverters.mapAsScalaMapConverter(productMap).asScala() : null;
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
        assertEquals(ENERGY_USED, added[0]);
        assertEquals(EMBODIED_EMISSIONS, added[1]);
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
    void testEnrichesOutputTokens() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "EUN1-Claude-output-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(ENERGY_USED));
        assertEquals(OUTPUT_ENERGY_PER_1K, ENERGY_USED.getDouble(enriched), 1e-12);
    }

    @Test
    void testEnrichesInputTokensDefaultRatioIsZero() {
        // EcoLogits attributes ~all generation cost to output tokens; default i2o ratio = 0.
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "EUN1-Claude-input-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertEquals(0.0, ENERGY_USED.getDouble(enriched), 1e-12);
        assertEquals(0.0, EMBODIED_EMISSIONS.getDouble(enriched), 1e-12);
    }

    @Test
    void testEnrichesInputTokensWithFullRatio() {
        // Operator-overridden i2o=1.0: input rows score the same as output rows.
        BedrockEcoLogits m = buildModule(Map.of("input_to_output_energy_ratio", 1.0));
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "EUN1-Claude-input-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        m.enrich(row, enriched);

        assertEquals(OUTPUT_ENERGY_PER_1K, ENERGY_USED.getDouble(enriched), 1e-12);
    }

    @Test
    void testEnrichesFallbackSplit() {
        // Ambiguous usage type, default ratios: 50% portion is "output", 50% is input × i2o(0) = 0.
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "generic-tokens-usage");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        double expected = OUTPUT_ENERGY_PER_1K * 0.5;
        assertNotNull(enriched.get(ENERGY_USED));
        assertEquals(expected, ENERGY_USED.getDouble(enriched), 1e-12);
    }

    @Test
    void testEnrichesEmbodiedEmissions() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "output");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(EMBODIED_EMISSIONS));
        assertEquals(OUTPUT_EMBODIED_G_PER_1K, EMBODIED_EMISSIONS.getDouble(enriched), 1e-12);
    }

    @Test
    void testOverwritesExistingEnergyValue() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 1.0, "1K tokens", "EUN1-Claude-output-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 10.0);
        enriched.put(EMBODIED_EMISSIONS, 5.0);

        module.enrich(row, enriched);

        assertEquals(OUTPUT_ENERGY_PER_1K, ENERGY_USED.getDouble(enriched), 1e-12);
        assertEquals(OUTPUT_EMBODIED_G_PER_1K, EMBODIED_EMISSIONS.getDouble(enriched), 1e-12);
    }

    @Test
    void testScalesWithMillionsMultiplier() {
        Row row = createRow(schema, "AmazonBedrock", VALID_PRODUCT_MAP, 2.0, "1M tokens", "EUN1-Claude-output-tokens");
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        // 2.0 × 1_000_000 tokens = 2000 × 1k tokens
        double expected = 2000.0 * OUTPUT_ENERGY_PER_1K;
        assertEquals(expected, ENERGY_USED.getDouble(enriched), 1e-12);
    }
}
