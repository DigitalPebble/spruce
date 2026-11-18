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

import static com.digitalpebble.spruce.SpruceColumn.COVERAGE_GAP;
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
     * for either binding: meter category, meter name and quantity first, then
     * {@code UnitOfMeasure} when the binding reads it, then the added columns.
     */
    static Row createRow(StructType schema, String meterCategory, String meterName, String unitOfMeasure, Double quantity) {
        Object[] values = new Object[schema.size()];
        values[0] = meterCategory;
        values[1] = meterName;
        values[2] = quantity;
        scala.Option<Object> unitIndex = schema.getFieldIndex(AzureColumn.UNIT_OF_MEASURE.getLabel());
        if (unitIndex.isDefined()) {
            values[(Integer) unitIndex.get()] = unitOfMeasure;
        }
        return new GenericRowWithSchema(values, schema);
    }

    @Test
    void testColumnsNeeded() {
        Column[] needed = module.columnsNeeded();
        assertEquals(4, needed.length);
        assertEquals(AzureColumn.METER_CATEGORY, needed[0]);
        assertEquals(AzureColumn.METER_NAME, needed[1]);
        assertEquals(AzureColumn.QUANTITY, needed[2]);
        assertEquals(AzureColumn.UNIT_OF_MEASURE, needed[3]);
    }

    @Test
    void testColumnsAdded() {
        Column[] added = module.columnsAdded();
        assertEquals(3, added.length);
        assertEquals(ENERGY_USED, added[0]);
        assertEquals(EMBODIED_EMISSIONS, added[1]);
        assertEquals(com.digitalpebble.spruce.SpruceColumn.COVERAGE_GAP, added[2]);
    }

    @ParameterizedTest
    @MethodSource("nullValueTestCases")
    void testProcessWithNullValues(String meterCategory, String meterName, Double quantity) {
        Row row = createRow(schema, meterCategory, meterName, "1M", quantity);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(ENERGY_USED) || enriched.containsKey(EMBODIED_EMISSIONS), "Should skip rows with invalid null values");
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
        Row row = createRow(schema, meterCategory, meterName, "1M", quantity);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertFalse(enriched.containsKey(ENERGY_USED) || enriched.containsKey(EMBODIED_EMISSIONS), "Should skip unsupported categories or unknown models");
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
    void testNativeQuantityIsInUnitOfMeasureBlocks() {
        // Quantity 0.0047 with UnitOfMeasure "1M" is 4,700 output tokens
        Row row = createRow(schema, "Foundry Models", "5.4 mini Opt Gl 1M Tokens", "1M", 0.0047);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertEquals(4.7 * OUTPUT_ENERGY_PER_1K, ENERGY_USED.getDouble(enriched), 1e-12);
        assertEquals(4.7 * OUTPUT_EMBODIED_G_PER_1K, EMBODIED_EMISSIONS.getDouble(enriched), 1e-9);
    }

    @Test
    void testSkipsUnknownUnitOfMeasure() {
        Row row = createRow(schema, "Azure OpenAI", "GPT 5 outpt Glbl 1M Tokens", "1 Hour", 1.0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNull(enriched.get(ENERGY_USED));
        assertEquals("token_unit=1 Hour", COVERAGE_GAP.getString(enriched));
    }

    @Test
    void testSkipsMissingUnitOfMeasure() {
        Row row = createRow(schema, "Azure OpenAI", "GPT 5 outpt Glbl 1M Tokens", null, 1.0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNull(enriched.get(ENERGY_USED));
        assertEquals("token_unit=null", COVERAGE_GAP.getString(enriched));
    }

    @Test
    void testAcceptsFoundryModelsCategory() {
        Row row = createRow(schema, "Foundry Models", "GPT 5 outpt Glbl 1M Tokens", "1M", 1.0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertNotNull(enriched.get(ENERGY_USED));
    }

    @Test
    void testSkipsInputTokens() {
        // EcoLogits attributes ~all generation cost to output tokens; input rows are ignored.
        Row row = createRow(schema, "Azure OpenAI", "5 mini pp Inp Gl 1M Tokens", "1M", 1.0);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);

        assertTrue(enriched.isEmpty());
    }

    @Test
    void testNativeQuantityIn1KBlocks() {
        // Quantity 2 with UnitOfMeasure "1K" is 2,000 tokens
        Row row = createRow(schema, "Azure OpenAI", "GPT 5 Outp regnl Tokens", "1K", 2.0);
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
