// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class EcoLogitsTest {

    // CUR strings that are mapped to models present in coefficients.csv.
    private static final String KNOWN_CUR_MODEL = "Claude 3 Haiku";
    private static final String KNOWN_SMALL_MODEL = "Llama 3 8B Instruct";
    private static final String KNOWN_LARGE_MODEL = "Llama 3 70B Instruct";
    private static final String UNKNOWN_MODEL_ID = "non-existent-model-xyz";

    @Nested
    class LoadTests {

        @Test
        void testLoadDoesNotThrow() {
            EcoLogits ecoLogits = new EcoLogits();
            assertDoesNotThrow(ecoLogits::load);
        }

        @Test
        void testLoadPopulatesMap() {
            EcoLogits ecoLogits = new EcoLogits();
            ecoLogits.load();
            assertNotNull(ecoLogits.getImpacts(KNOWN_CUR_MODEL),
                    "Known CUR model should resolve after load()");
        }

        @Test
        void testLoadIsIdempotent() {
            EcoLogits ecoLogits = new EcoLogits();
            ecoLogits.load();
            assertDoesNotThrow(ecoLogits::load);
            assertNotNull(ecoLogits.getImpacts(KNOWN_CUR_MODEL));
        }

        @Test
        void testGetImpactsBeforeLoadReturnsNull() {
            EcoLogits ecoLogits = new EcoLogits();
            assertNull(ecoLogits.getImpacts(KNOWN_CUR_MODEL));
        }
    }

    @Nested
    class GetImpactsTests {

        private EcoLogits ecoLogits;

        @BeforeEach
        void setUp() {
            ecoLogits = new EcoLogits();
            ecoLogits.load();
        }

        @Test
        void testKnownModel() {
            assertNotNull(ecoLogits.getImpacts(KNOWN_CUR_MODEL));
        }

        @Test
        void testUnknownModelReturnsNull() {
            assertNull(ecoLogits.getImpacts(UNKNOWN_MODEL_ID));
        }

        @Test
        void testNullModelIdReturnsNull() {
            assertNull(ecoLogits.getImpacts(null));
        }

        @Test
        void testEmptyModelIdReturnsNull() {
            assertNull(ecoLogits.getImpacts(""));
        }

        @Test
        void testLookupIsCaseInsensitive() {
            assertNotNull(ecoLogits.getImpacts(KNOWN_CUR_MODEL.toLowerCase()));
            assertNotNull(ecoLogits.getImpacts(KNOWN_CUR_MODEL.toUpperCase()));
        }

        @Test
        void testConsistentAcrossCalls() {
            EcoLogits.ModelImpacts first = ecoLogits.getImpacts(KNOWN_CUR_MODEL);
            EcoLogits.ModelImpacts second = ecoLogits.getImpacts(KNOWN_CUR_MODEL);
            assertSame(first, second);
        }

        @ParameterizedTest
        @MethodSource("knownModelIdsProvider")
        void testMultipleKnownModels(String modelId) {
            assertNotNull(ecoLogits.getImpacts(modelId),
                    "Expected non-null ModelImpacts for model: " + modelId);
        }

        static Stream<String> knownModelIdsProvider() {
            // CUR strings whose (provider, model_name) mapping is covered by coefficients.csv.
            return Stream.of(
                    "Claude 3 Haiku",
                    "anthropic.claude-3-haiku-20240307-v1:0",
                    "Llama 3 8B Instruct",
                    "meta.llama3-8b-instruct-v1:0",
                    "Llama 3 70B Instruct",
                    "meta.llama3-70b-instruct-v1:0",
                    "Mistral 7B Instruct",
                    "mistral.mistral-7b-instruct-v0:2",
                    "Mixtral 8x7B Instruct",
                    "mistral.mixtral-8x7b-instruct-v0:1",
                    "Command R",
                    "cohere.command-r-v1:0",
                    "Mistral Large"
            );
        }
    }

    @Nested
    class ModelImpactsTests {

        private EcoLogits ecoLogits;

        @BeforeEach
        void setUp() {
            ecoLogits = new EcoLogits();
            ecoLogits.load();
        }

        @Test
        void testFieldsAreNonNegative() {
            EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(KNOWN_CUR_MODEL);
            assertNotNull(impacts);
            assertTrue(impacts.getEnergyKwhPer1kOutputTokens() >= 0);
            assertTrue(impacts.getGwpEmbodiedGPer1kOutputTokens() >= 0);
            assertTrue(impacts.getAdpeEmbodiedKgsbeqPer1kOutputTokens() >= 0);
        }

        @Test
        void testFieldsArePositive() {
            EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(KNOWN_CUR_MODEL);
            assertNotNull(impacts);
            assertTrue(impacts.getEnergyKwhPer1kOutputTokens() > 0);
            assertTrue(impacts.getGwpEmbodiedGPer1kOutputTokens() > 0);
            assertTrue(impacts.getAdpeEmbodiedKgsbeqPer1kOutputTokens() > 0);
        }

        @Test
        void testFieldsAreFinite() {
            EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(KNOWN_CUR_MODEL);
            assertNotNull(impacts);
            assertTrue(Double.isFinite(impacts.getEnergyKwhPer1kOutputTokens()));
            assertTrue(Double.isFinite(impacts.getGwpEmbodiedGPer1kOutputTokens()));
            assertTrue(Double.isFinite(impacts.getAdpeEmbodiedKgsbeqPer1kOutputTokens()));
        }

        @Test
        void testConstructorStoresValues() {
            EcoLogits.ModelImpacts impacts =
                    new EcoLogits.ModelImpacts(0.001234, 0.5678, 1.2e-9);
            assertEquals(0.001234, impacts.getEnergyKwhPer1kOutputTokens(), 1e-12);
            assertEquals(0.5678, impacts.getGwpEmbodiedGPer1kOutputTokens(), 1e-12);
            assertEquals(1.2e-9, impacts.getAdpeEmbodiedKgsbeqPer1kOutputTokens(), 1e-15);
        }

        @ParameterizedTest
        @ValueSource(doubles = {0.0, 1e-9, 0.5, 100.0})
        void testConstructorWithVaryingValues(double value) {
            EcoLogits.ModelImpacts impacts = new EcoLogits.ModelImpacts(value, value, value);
            assertEquals(value, impacts.getEnergyKwhPer1kOutputTokens(), 1e-12);
            assertEquals(value, impacts.getGwpEmbodiedGPer1kOutputTokens(), 1e-12);
            assertEquals(value, impacts.getAdpeEmbodiedKgsbeqPer1kOutputTokens(), 1e-12);
        }

        @Test
        void testLargerModelConsumesMoreEnergyThanSmallerModel() {
            EcoLogits.ModelImpacts small = ecoLogits.getImpacts(KNOWN_SMALL_MODEL);
            EcoLogits.ModelImpacts large = ecoLogits.getImpacts(KNOWN_LARGE_MODEL);
            assertNotNull(small);
            assertNotNull(large);
            assertTrue(large.getEnergyKwhPer1kOutputTokens() > small.getEnergyKwhPer1kOutputTokens(),
                    "Llama 3 70B should have higher per-token energy than Llama 3 8B");
        }
    }
}
