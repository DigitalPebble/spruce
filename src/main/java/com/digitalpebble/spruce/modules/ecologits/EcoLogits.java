// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import com.digitalpebble.spruce.Utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cloud-agnostic lookup for LLM inference impact coefficients sourced from the
 * EcoLogits public API ({@code https://api.ecologits.ai}).
 * <p>
 * Two CSV resources back this class:
 * <ul>
 *   <li>{@code ecologits/mapping.csv} — maps a provider-neutral model
 *       label to a canonical {@code (provider, model_name)} pair. The generator
 *       appends any missing models automatically.</li>
 *   <li>{@code ecologits/coefficients.csv} — per-{@code (provider, model_name)}
 *       per-1k-output-token coefficients (energy, embodied GWP, embodied ADPe).
 *       Regenerated from the API by
 *       {@link com.digitalpebble.spruce.tools.EcoLogitsCoefficientsGenerator}.</li>
 * </ul>
 * Coefficients only describe output tokens because the underlying EcoLogits
 * methodology attributes the energy of an LLM request to the autoregressive
 * generation phase. Translating CUR input-token usage into a comparable energy
 * figure is the responsibility of the calling enrichment module
 * (e.g. {@link BedrockEcoLogits}).
 */
public class EcoLogits implements Serializable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(EcoLogits.class);

    static final String DEFAULT_MAPPING_RESOURCE = "ecologits/mapping.csv";
    static final String DEFAULT_COEFFICIENTS_RESOURCE = "ecologits/coefficients.csv";

    private final String mappingResource;
    private final String coefficientsResource;

    private final Map<String, ProviderModelKey> mapping = new HashMap<>();
    private final Map<ProviderModelKey, ModelImpacts> coefficients = new HashMap<>();
    private final Set<String> unknownModels = ConcurrentHashMap.newKeySet();

    public EcoLogits() {
        this(DEFAULT_MAPPING_RESOURCE, DEFAULT_COEFFICIENTS_RESOURCE);
    }

    public EcoLogits(String mappingResource, String coefficientsResource) {
        this.mappingResource = mappingResource;
        this.coefficientsResource = coefficientsResource;
    }

    /**
     * Loads the mapping and coefficients CSV resources into memory.
     * Should be called once during module initialization.
     */
    public void load() {
        loadCoefficients();
        loadMapping();
        LOG.info("Loaded {} model label mappings and {} coefficient entries from {} / {}",
                mapping.size(), coefficients.size(),
                mappingResource, coefficientsResource);
    }

    private void loadCoefficients() {
        List<String[]> rows = Utils.loadCSV(coefficientsResource);
        for (String[] parts : rows) {
            if (parts.length < 5) continue;
            // Skip header row
            if ("provider".equalsIgnoreCase(parts[0].trim())) continue;
            String provider = parts[0].trim();
            String model = parts[1].trim();
            if (provider.isEmpty() || model.isEmpty()) continue;
            try {
                double energy = Double.parseDouble(parts[2].trim());
                double gwpEmbodiedKg = Double.parseDouble(parts[3].trim());
                double adpeEmbodied = Double.parseDouble(parts[4].trim());
                // Convert kgCO2eq -> gCO2eq once at load time so consumers get grams.
                double gwpEmbodiedG = gwpEmbodiedKg * 1_000.0;
                coefficients.put(new ProviderModelKey(provider, model),
                        new ModelImpacts(energy, gwpEmbodiedG, adpeEmbodied));
            } catch (NumberFormatException e) {
                LOG.warn("Skipping coefficients row with invalid number: {}/{}", provider, model);
            }
        }
    }

    private void loadMapping() {
        List<String[]> rows = Utils.loadCSV(mappingResource);
        for (String[] parts : rows) {
            if (parts.length < 3) continue;
            String label = parts[0].trim();
            if (label.isEmpty() || "label".equalsIgnoreCase(label)) continue;
            String provider = parts[1].trim();
            String model = parts[2].trim();
            if (provider.isEmpty() || model.isEmpty()) continue;
            mapping.put(label.toLowerCase(Locale.ROOT), new ProviderModelKey(provider, model));
        }
    }

    /**
     * Returns the impact coefficients for a model label, or {@code null} if
     * the label is not mapped or not covered by the API. Logs at most one warning
     * per distinct missing label.
     */
    public ModelImpacts getImpacts(String label) {
        if (label == null) return null;
        String lower = label.toLowerCase(Locale.ROOT);
        ProviderModelKey key = mapping.get(lower);
        if (key == null) {
            if (unknownModels.add(lower)) {
                LOG.warn("Unmapped LLM model label: {}", label);
            }
            return null;
        }
        ModelImpacts impacts = coefficients.get(key);
        if (impacts == null && unknownModels.add(lower)) {
            LOG.warn("Model label {} maps to {} but no coefficients found in {}",
                    label, key, coefficientsResource);
        }
        return impacts;
    }

    /**
     * Composite key identifying a model in the EcoLogits API.
     */
    public static final class ProviderModelKey implements Serializable {
        private final String provider;
        private final String modelName;

        public ProviderModelKey(String provider, String modelName) {
            this.provider = provider;
            this.modelName = modelName;
        }

        public String provider() { return provider; }
        public String modelName() { return modelName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ProviderModelKey other)) return false;
            return provider.equals(other.provider) && modelName.equals(other.modelName);
        }

        @Override
        public int hashCode() { return Objects.hash(provider, modelName); }

        @Override
        public String toString() { return provider + "/" + modelName; }
    }

    /**
     * Per-1k-output-token impact coefficients for an LLM. The GWP value is
     * stored in grams (converted from the API's native kg at load time).
     */
    public static final class ModelImpacts implements Serializable {
        private final double energyKwhPer1kOutputTokens;
        private final double gwpEmbodiedGPer1kOutputTokens;
        private final double adpeEmbodiedKgsbeqPer1kOutputTokens;

        public ModelImpacts(double energyKwhPer1kOutputTokens,
                            double gwpEmbodiedGPer1kOutputTokens,
                            double adpeEmbodiedKgsbeqPer1kOutputTokens) {
            this.energyKwhPer1kOutputTokens = energyKwhPer1kOutputTokens;
            this.gwpEmbodiedGPer1kOutputTokens = gwpEmbodiedGPer1kOutputTokens;
            this.adpeEmbodiedKgsbeqPer1kOutputTokens = adpeEmbodiedKgsbeqPer1kOutputTokens;
        }

        public double getEnergyKwhPer1kOutputTokens() { return energyKwhPer1kOutputTokens; }

        public double getGwpEmbodiedGPer1kOutputTokens() { return gwpEmbodiedGPer1kOutputTokens; }

        public double getAdpeEmbodiedKgsbeqPer1kOutputTokens() { return adpeEmbodiedKgsbeqPer1kOutputTokens; }
    }
}
