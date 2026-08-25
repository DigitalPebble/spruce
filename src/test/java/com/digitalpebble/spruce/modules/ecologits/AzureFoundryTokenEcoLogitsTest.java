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

public class AzureFoundryTokenEcoLogitsTest {

    private AzureFoundryTokenEcoLogits module;
    private StructType schema;

    private static final String TEST_MAPPING = "ecologits-test/mapping.csv";
    private static final String TEST_COEFFICIENTS = "ecologits-test/coefficients.csv";

    // Coefficients in test-coefficients.csv: 1e-3 kWh and 5e-4 kg (=0.5 g) per 1k output tokens.
    private static final double OUTPUT_ENERGY_PER_1K = 1.0e-3;
    private static final double OUTPUT_EMBODIED_G_PER_1K = 0.5;

    @BeforeEach
    void setUp() {
        module = new AzureFoundryTokenEcoLogits();
        schema = Utils.getSchema(module);
        EcoLogits impacts = new EcoLogits(TEST_MAPPING, TEST_COEFFICIENTS);
        impacts.load();
        module.setEcoLogits(impacts);
        module.init(new HashMap<>());
    }

    /**
     * Creates a {@link Row} matching the schema produced by {@link Utils#getSchema(EnrichmentModule)}
     * for either binding.
     *
     * <p>Schema order: meter category, meter name, quantity, {@code ENERGY_USED},
     * {@code EMBODIED_EMISSIONS}
     */
    static Row createRow(StructType schema, String meterCategory, String meterName, Double quantity) {
        Object[] values = new Object[5];
        values[0] = meterCategory;
        values[1] = meterName;
        values[2] = quantity;
        values[3] = null;
        values[4] = null;
        return new GenericRowWithSchema(values, schema);
    }

    @Test
    void testColumnsNeeded() {
        Column[] needed = module.columnsNeeded();
        assertEquals(3, needed.length);
        assertEquals(AzureColumn.METER_CATEGORY, needed[0]);
        assertEquals(AzureColumn.METER_NAME, needed[1]);
        assertEquals(AzureColumn.QUANTITY, needed[2]);
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
    void testProcessWithNullValues(String meterCategory, String meterName, Double quantity) {
        Row row = createRow(schema, meterCategory, meterName, quantity);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty(), "Should skip rows with invalid null values");
    }

    static Stream<Arguments> nullValueTestCases() {
        return Stream.of(
                Arguments.of(null, "GPT 5 outpt Glbl 1M Tokens", 1.0),
                Arguments.of("Azure OpenAI", null, 1.0),
                Arguments.of("Azure OpenAI", "GPT 5 outpt Glbl 1M Tokens", null)
        );
    }

    @ParameterizedTest
    @MethodSource("unsupportedValueTestCases")
    void testProcessWithUnsupportedValues(String meterCategory, String meterName, Double quantity) {
        Row row = createRow(schema, meterCategory, meterName, quantity);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty(), "Should skip unsupported categories or unknown models");
    }

    static Stream<Arguments> unsupportedValueTestCases() {
        return Stream.of(
                Arguments.of("Virtual Machines", "GPT 5 outpt Glbl 1M Tokens", 1.0),
                Arguments.of("Storage", "GPT 5 outpt Glbl 1M Tokens", 1.0),
                Arguments.of("Azure OpenAI", "UnknownModel outpt Glbl 1M Tokens", 1.0),
                Arguments.of("Azure OpenAI", "Code-Interpreter-global Session", 1.0),
                Arguments.of("Azure OpenAI", "GPT 5 outpt Glbl 1M Tokens", 0.0),
                Arguments.of("Azure OpenAI", "GPT 5 outpt Glbl 1M Tokens", -5.0)
        );
    }

    @Test
    void testEnrichesOutputTokens() {
        // Quantity is the consumed token count: 1,000,000 tokens = 1000 × 1k tokens.
        // UnitOfMeasure ("1M") only describes the pricing block and must not scale it.
        Row row = createRow(schema, "Azure OpenAI", "GPT 5 outpt Glbl 1M Tokens", 1_000_000.0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(ENERGY_USED));
        assertEquals(1000.0 * OUTPUT_ENERGY_PER_1K, ENERGY_USED.getDouble(enriched), 1e-12);
        assertEquals(1000.0 * OUTPUT_EMBODIED_G_PER_1K, EMBODIED_EMISSIONS.getDouble(enriched), 1e-9);
    }

    @Test
    void testAcceptsFoundryModelsCategory() {
        Row row = createRow(schema, "Foundry Models", "GPT 5 outpt Glbl 1M Tokens", 1.0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(ENERGY_USED));
    }

    @Test
    void testSkipsInputTokens() {
        // EcoLogits attributes ~all generation cost to output tokens; input rows are ignored.
        Row row = createRow(schema, "Azure OpenAI", "5 mini pp Inp Gl 1M Tokens", 1.0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty());
    }

    @Test
    void testQuantityIsTokenCountRegardlessOfPricingBlock() {
        // A meter priced per 1K tokens still reports the consumed token count in Quantity
        Row row = createRow(schema, "Azure OpenAI", "GPT 5 Outp regnl Tokens", 2_000.0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertEquals(2.0 * OUTPUT_ENERGY_PER_1K, ENERGY_USED.getDouble(enriched), 1e-12);
    }

    private AzureFoundryTokenEcoLogits focusModule() {
        AzureFoundryTokenEcoLogits focusModule = new AzureFoundryTokenEcoLogits();
        focusModule.bindReportFormat(ReportFormat.FOCUS);
        EcoLogits impacts = new EcoLogits(TEST_MAPPING, TEST_COEFFICIENTS);
        impacts.load();
        focusModule.setEcoLogits(impacts);
        focusModule.init(new HashMap<>());
        return focusModule;
    }

    @Test
    void testFOCUSBindingColumns() {
        AzureFoundryTokenEcoLogits focusModule = focusModule();
        assertEquals(3, focusModule.columnsNeeded().length);
        assertEquals(AzureFOCUSColumn.X_SKU_METER_CATEGORY, focusModule.columnsNeeded()[0]);
        assertEquals(AzureFOCUSColumn.X_SKU_METER_NAME, focusModule.columnsNeeded()[1]);
        assertEquals(FOCUSColumn.CONSUMED_QUANTITY, focusModule.columnsNeeded()[2]);
    }

    @Test
    void testNativeAndFOCUSAgreeOnSameInference() {
        // The same inference expressed in both report formats must yield the same impacts.
        // Microsoft's FOCUS conversion defines ConsumedQuantity = Quantity for usage rows, so
        // both columns hold the consumed token count (1,000,000 tokens here).
        Row nativeRow = createRow(schema, "Azure OpenAI", "GPT 5 outpt Glbl 1M Tokens", 1_000_000.0);
        Map<Column, Object> nativeEnriched = new HashMap<>();
        module.enrich(nativeRow, nativeEnriched);

        AzureFoundryTokenEcoLogits focusModule = focusModule();
        Row focusRow = createRow(Utils.getSchema(focusModule),
                "Azure OpenAI", "GPT 5 outpt Glbl 1M Tokens", 1_000_000.0);
        Map<Column, Object> focusEnriched = new HashMap<>();
        focusModule.enrich(focusRow, focusEnriched);

        assertNotNull(nativeEnriched.get(ENERGY_USED));
        assertNotNull(focusEnriched.get(ENERGY_USED));
        assertEquals(ENERGY_USED.getDouble(nativeEnriched),
                ENERGY_USED.getDouble(focusEnriched), 1e-12);
        assertEquals(EMBODIED_EMISSIONS.getDouble(nativeEnriched),
                EMBODIED_EMISSIONS.getDouble(focusEnriched), 1e-9);
    }

    @ParameterizedTest
    @MethodSource("parseMeterNameCases")
    void testParseMeterName(String meterName, String expectedLabel, String expectedDirection) {
        String[] result = AzureFoundryTokenEcoLogits.parseMeterName(meterName);
        if (expectedLabel == null) {
            assertNull(result);
        } else {
            assertNotNull(result, "Expected a match for: " + meterName);
            assertEquals(expectedLabel, result[0]);
            assertEquals(expectedDirection, result[1]);
        }
    }

    // Meter names taken from the Azure Retail Prices API for Azure OpenAI / Foundry Models.
    static Stream<Arguments> parseMeterNameCases() {
        return Stream.of(
                Arguments.of("GPT 5 outpt Glbl 1M Tokens", "gpt 5", "output"),
                Arguments.of("5.1 codex opt Gl 1M Tokens", "5.1 codex", "output"),
                Arguments.of("5 mini pp Inp Gl 1M Tokens", "5 mini", "input"),
                Arguments.of("gpt 4.1 Inp regnl Tokens", "gpt 4.1", "input"),
                Arguments.of("gpt-4o-rt-txt-1217 Outp glbl Tokens", "gpt-4o-rt-txt-1217", "output"),
                Arguments.of("5.6 terra ShortCo Cd Inp PP Gl 1M Tokens", "5.6 terra", "input"),
                Arguments.of("5.4 opt Dz 1M Tokens", "5.4", "output"),
                Arguments.of("o1 1217 Outp Data Zone Tokens", "o1 1217", "output"),
                Arguments.of("gpt rt aud 0828 cchd Inp glbl Tokens", "gpt rt aud 0828", "input"),
                Arguments.of("5.4 pro Batch inp Dz 1M Tokens", "5.4 pro", "input"),
                // no "Tokens" → not a token meter
                Arguments.of("Code-Interpreter-global Session", null, null),
                // no input/output marker
                Arguments.of("gpt image 1 generations", null, null),
                Arguments.of(null, null, null),
                Arguments.of("", null, null)
        );
    }

}
