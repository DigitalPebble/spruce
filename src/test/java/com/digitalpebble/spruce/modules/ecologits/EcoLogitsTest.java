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

    // Model IDs from bedrock.json
    private static final String KNOWN_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0";
    private static final String KNOWN_MODEL_ID_SMALL = "meta.llama3-8b-instruct-v1:0";
    private static final String KNOWN_MODEL_ID_LARGE = "anthropic.claude-3-opus-20240229-v1:0";
    private static final String UNKNOWN_MODEL_ID = "non-existent-model-xyz";

    // load()
    @Nested
    class LoadTests {

        @Test
        void testLoadDoesNotThrow() {
            EcoLogits ecoLogits = new EcoLogits();
            assertDoesNotThrow(ecoLogits::load, "load() should not throw when the bundled resource is present");
        }

        @Test
        void testLoadPopulatesMap() {
            EcoLogits ecoLogits = new EcoLogits();
            ecoLogits.load();
            assertNotNull(ecoLogits.getImpacts(KNOWN_MODEL_ID), "At least one known model should be available after load()");
        }

        @Test
        void testLoadIsIdempotent() {
            EcoLogits ecoLogits = new EcoLogits();
            ecoLogits.load();
            assertDoesNotThrow(ecoLogits::load, "Calling load() twice should not throw");
            assertNotNull(ecoLogits.getImpacts(KNOWN_MODEL_ID), "Known model should still be resolvable after double load()");
        }

        @Test
        void testGetImpactsBeforeLoadReturnsNull() {
            EcoLogits ecoLogits = new EcoLogits();
            assertNull(ecoLogits.getImpacts(KNOWN_MODEL_ID), "getImpacts() should return null when load() has not been called");
        }

        // getImpacts(String modelId)
        @Nested
        class getImpactsTests {

            private EcoLogits ecoLogits;

            @BeforeEach
            void setUp() {
                ecoLogits = new EcoLogits();
                ecoLogits.load();
            }

            @Test
            void testGetImpactsWithKnownModel() {
                EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(KNOWN_MODEL_ID);
                assertNotNull(impacts, "Known model should return a non-null ModelImpacts");
            }

            @Test
            void testGetImpactsWithUnknownModelReturnsNull() {
                assertNull(ecoLogits.getImpacts(UNKNOWN_MODEL_ID),"Unknown model should return null");
            }

            @Test
            void testGetImpactsWithNullModelIdThrowsNPE() {
                // ConcurrentHashMap does not allow null keys: a NullPointerException is expected
                assertThrows(NullPointerException.class, () -> ecoLogits.getImpacts(null), "Null model ID should throw NullPointerException (ConcurrentHashMap constraint)");
            }

            @Test
            void testGetImpactsWithEmptyModelIdReturnsNull() {
                assertNull(ecoLogits.getImpacts(""), "Empty model ID should return null");
            }

            @Test
            void testGetImpactsIsConsistentAcrossCalls() {
                EcoLogits.ModelImpacts first = ecoLogits.getImpacts(KNOWN_MODEL_ID);
                EcoLogits.ModelImpacts second = ecoLogits.getImpacts(KNOWN_MODEL_ID);
                assertSame(first, second, "Repeated calls with the same model ID should return the same ModelImpacts instance");
            }

            @ParameterizedTest
            @MethodSource("knownModelIdsProvider")
            void testGetImpactsWithMultipleKnownModels(String modelId) {
                EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(modelId);
                assertNotNull(impacts, "Expected non-null ModelImpacts for model: " + modelId);
            }

            static Stream<String> knownModelIdsProvider() {
                // All the model IDs and commercial names present in bedrock.json
                return Stream.of(
                        "anthropic.claude-v2",
                        "Claude 2",
                        "anthropic.claude-v2:1",
                        "Claude 2.1",
                        "anthropic.claude-3-haiku-20240307-v1:0",
                        "Claude 3 Haiku",
                        "anthropic.claude-3-sonnet-20240229-v1:0",
                        "Claude 3 Sonnet",
                        "anthropic.claude-3-opus-20240229-v1:0",
                        "Claude 3 Opus",
                        "amazon.titan-text-express-v1",
                        "Titan Text G1 - Express",
                        "meta.llama3-8b-instruct-v1:0",
                        "Llama 3 8B Instruct",
                        "meta.llama3-70b-instruct-v1:0",
                        "Llama 3 70B Instruct",
                        "mistral.mistral-7b-instruct-v0:2",
                        "Mistral 7B Instruct",
                        "mistral.mixtral-8x7b-instruct-v0:1",
                        "Mixtral 8x7B Instruct",
                        "cohere.command-r-v1:0",
                        "Command R",
                        "ai21.j2-ultra-v1",
                        "Jurassic-2 Ultra"
                );
            }
        }

        // ModelImpacts (inner class)
        @Nested
        class ModelImpactsTests {

            private EcoLogits ecoLogits;

            @BeforeEach
            void setUp() {
                ecoLogits = new EcoLogits();
                ecoLogits.load();
            }

            @Test
            void testModelImpactsFieldsAreNonNegative() {
                EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(KNOWN_MODEL_ID);
                assertNotNull(impacts);

                assertTrue(impacts.getEnergyKwhPer1kInputTokens() >= 0, "Input-token energy coefficient should be non-negative");
                assertTrue(impacts.getEnergyKwhPer1kOutputTokens() >= 0, "Output-token energy coefficient should be non-negative");
                assertTrue(impacts.getEmbodiedCo2eGPer1kTokens() >= 0, "Embodied CO₂e coefficient should be non-negative");
            }

            @Test
            void testModelImpactsFieldsArePositive() {
                EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(KNOWN_MODEL_ID);
                assertNotNull(impacts);

                assertTrue(impacts.getEnergyKwhPer1kInputTokens() > 0, "Input-token energy coefficient should be positive for a real model");
                assertTrue(impacts.getEnergyKwhPer1kOutputTokens() > 0, "Output-token energy coefficient should be positive for a real model");
                assertTrue(impacts.getEmbodiedCo2eGPer1kTokens() > 0, "Embodied CO₂e coefficient should be positive for a real model");
            }

            @Test
            void testModelImpactsFieldsAreFinite() {
                EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(KNOWN_MODEL_ID);
                assertNotNull(impacts);

                assertTrue(Double.isFinite(impacts.getEnergyKwhPer1kInputTokens()), "Input-token energy must be a finite number");
                assertTrue(Double.isFinite(impacts.getEnergyKwhPer1kOutputTokens()), "Output-token energy must be a finite number");
                assertTrue(Double.isFinite(impacts.getEmbodiedCo2eGPer1kTokens()), "Embodied CO₂e must be a finite number");
            }

            @Test
            void testModelImpactsExactValuesMatchBedrock() {
                // Ground-truth values taken directly from bedrock.json for llama3-8b
                EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts("meta.llama3-8b-instruct-v1:0");
                assertNotNull(impacts);

                assertEquals(0.00002551, impacts.getEnergyKwhPer1kInputTokens(),  1e-10, "Input energy for meta.llama3-8b-instruct-v1:0 should match bedrock.json");
                assertEquals(0.00005101, impacts.getEnergyKwhPer1kOutputTokens(), 1e-10, "Output energy for meta.llama3-8b-instruct-v1:0 should match bedrock.json");
                assertEquals(0.00001971,   impacts.getEmbodiedCo2eGPer1kTokens(),   1e-10, "Embodied CO₂e for meta.llama3-8b-instruct-v1:0 should match bedrock.json");
            }

            @Test
            void testModelImpactsConstructorStoresValues() {
                double inputEnergy = 0.001234;
                double outputEnergy = 0.002345;
                double embodied = 0.5678;

                EcoLogits.ModelImpacts impacts = new EcoLogits.ModelImpacts(inputEnergy, outputEnergy, embodied);

                assertEquals(inputEnergy,  impacts.getEnergyKwhPer1kInputTokens(),  1e-12);
                assertEquals(outputEnergy, impacts.getEnergyKwhPer1kOutputTokens(), 1e-12);
                assertEquals(embodied,     impacts.getEmbodiedCo2eGPer1kTokens(),   1e-12);
            }

            @ParameterizedTest
            @ValueSource(doubles = {0.0, 1e-9, 0.5, 100.0})
            void testModelImpactsConstructorWithVaryingValues(double value) {
                EcoLogits.ModelImpacts impacts = new EcoLogits.ModelImpacts(value, value, value);
                assertEquals(value, impacts.getEnergyKwhPer1kInputTokens(), 1e-12);
                assertEquals(value, impacts.getEnergyKwhPer1kOutputTokens(), 1e-12);
                assertEquals(value, impacts.getEmbodiedCo2eGPer1kTokens(), 1e-12);
            }

            // Output energy > input energy (typical LLM characteristic)
            @Test
            void testOutputTokenEnergyIsGreaterThanOrEqualToInputTokenEnergy() {
                // Verified against bedrock.json: every model has output > input coefficient
                EcoLogits.ModelImpacts impacts = ecoLogits.getImpacts(KNOWN_MODEL_ID);
                assertNotNull(impacts);

                assertTrue(impacts.getEnergyKwhPer1kOutputTokens() > impacts.getEnergyKwhPer1kInputTokens(), "Output-token energy coefficient must be strictly greater than input-token coefficient");
            }

            @Test
            void testLargerModelConsumesMoreEnergyThanSmallerModel() {
                // claude-3-opus (large) vs llama3-8b (small) — both present in bedrock.json
                EcoLogits.ModelImpacts small = ecoLogits.getImpacts(KNOWN_MODEL_ID_SMALL);
                EcoLogits.ModelImpacts large = ecoLogits.getImpacts(KNOWN_MODEL_ID_LARGE);
                assertNotNull(small);
                assertNotNull(large);

                assertTrue(large.getEnergyKwhPer1kOutputTokens() > small.getEnergyKwhPer1kOutputTokens(), "A larger model should have a higher output-token energy coefficient");
                assertTrue(large.getEmbodiedCo2eGPer1kTokens() > small.getEmbodiedCo2eGPer1kTokens(), "A larger model should have a higher embodied CO₂e coefficient");
            }
        }
    }
}