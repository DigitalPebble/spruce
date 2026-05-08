// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ember;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.CARBON_INTENSITY;

public abstract class AbstractEmberCarbonIntensity implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(AbstractEmberCarbonIntensity.class);

    private static final String DEFAULT_RESOURCE_LOCATION = "ember/ember_co2_intensity.csv";

    // keyed by "provider:region" e.g. "aws:us-east-1"
    private final Map<String, Double> intensities = new HashMap<>();

    /** Set via {@link #init(Map, Provider)} — left null on purpose so any call path that
     *  bypasses provider-aware init fails loudly rather than silently using AWS. */
    private Provider provider;

    @Override
    public void init(Map<String, Object> params, Provider provider) {
        this.provider = provider;
        init(params);
    }

    /** Returns the active cloud provider for lookups. */
    protected Provider getProvider() {
        return provider;
    }

    @Override
    public void init(Map<String, Object> params) {
        try {
            List<String> lines = Utils.loadLinesResources(DEFAULT_RESOURCE_LOCATION);
            lines.forEach(line -> {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    return;
                }
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    String key = parts[0].trim() + ":" + parts[1].trim();
                    double value = Double.parseDouble(parts[2].trim());
                    intensities.put(key, value);
                } else {
                    throw new RuntimeException("Invalid ember intensity line: " + line);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{CARBON_INTENSITY};
    }

    /**
     * Returns the carbon intensity in gCO2/kWh for the given provider and region,
     * or null if not found.
     */
    protected Double getIntensity(Provider provider, String region) {
        String key = provider.csvKey + ":" + region;
        Double value = intensities.get(key);
        if (value == null) {
            log.info("No Ember carbon intensity for {} region {}", provider, region);
        }
        return value;
    }
}
