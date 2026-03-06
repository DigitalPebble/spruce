// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import com.digitalpebble.spruce.Utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cloud-agnostic lookup for LLM inference energy and embodied emissions coefficients.
 * <p>
 * Loads a static JSON file (extracted from EcoLogits data) that maps model IDs to
 * energy-per-1K-token and embodied-emissions-per-1K-token values.
 */
public class EcoLogits implements Serializable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(EcoLogits.class);

    private static final String DEFAULT_RESOURCE = "ecologits/bedrock.json";

    private final Map<String, ModelImpacts> modelsMap = new HashMap<>();
    private final Set<String> unknownModels = ConcurrentHashMap.newKeySet();

    /**
     * Loads the model coefficients from the bundled JSON resource into the memory map.
     * This should typically be called once during the initialization phase.
     *
     * @throws RuntimeException if the resource file cannot be read, parsed, or found.
     */
    @SuppressWarnings("unchecked")
    public void load() {
        try {
            Map<String, Object> raw = Utils.loadJSONResources(DEFAULT_RESOURCE);
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                Map<String, Object> values = (Map<String, Object>) entry.getValue();
                double inputEnergy = ((Number) values.get("energy_kwh_per_1k_input_tokens")).doubleValue();
                double outputEnergy = ((Number) values.get("energy_kwh_per_1k_output_tokens")).doubleValue();
                double embodied = ((Number) values.get("embodied_co2e_g_per_1k_tokens")).doubleValue();
                modelsMap.put(entry.getKey(), new ModelImpacts(inputEnergy, outputEnergy, embodied));
            }
            LOG.info("Loaded {} LLM model coefficients from {}", modelsMap.size(), DEFAULT_RESOURCE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load LLM model coefficients from " + DEFAULT_RESOURCE, e);
        }
    }


    // Returns the environmental impacts for a given model ID.
    public ModelImpacts getImpacts(String modelId) {
        ModelImpacts impacts = modelsMap.get(modelId);
        if (impacts == null && unknownModels.add(modelId)) {
            LOG.warn("Unknown LLM model: {}", modelId);
        }
        return impacts;
    }

    /**
     * Holds the environmental impact coefficients for a specific LLM model.
     * The values are normalized to a block of 1,000 tokens, which is the standard
     * billing unit for cloud inference services like AWS Bedrock.
     */
    public static class ModelImpacts {
        private final double energyKwhPer1kInputTokens;
        private final double energyKwhPer1kOutputTokens;
        private final double embodiedCo2eGPer1kTokens;

        public ModelImpacts(double energyKwhPer1kInputTokens, double energyKwhPer1kOutputTokens, double embodiedCo2eGPer1kTokens) {
            this.energyKwhPer1kInputTokens = energyKwhPer1kInputTokens;
            this.energyKwhPer1kOutputTokens = energyKwhPer1kOutputTokens;
            this.embodiedCo2eGPer1kTokens = embodiedCo2eGPer1kTokens;
        }

        public double getEnergyKwhPer1kInputTokens() {
            return energyKwhPer1kInputTokens;
        }

        public double getEnergyKwhPer1kOutputTokens() {
            return energyKwhPer1kOutputTokens;
        }

        public double getEmbodiedCo2eGPer1kTokens() {
            return embodiedCo2eGPer1kTokens;
        }
    }
}