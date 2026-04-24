// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ember;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Populate the CARBON_INTENSITY field using Ember's cloud region carbon intensity data.
 */
public class AverageCarbonIntensity implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(AverageCarbonIntensity.class);

    private static final String DEFAULT_RESOURCE_LOCATION = "ember/ember_co2_intensity.csv";

    // keyed by "provider:region" e.g. "aws:us-east-1"
    private final Map<String, Double> intensities = new HashMap<>();

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

    private static String providerCode(Provider provider) {
        return switch (provider) {
            case AWS -> "aws";
            case GOOGLE -> "gcp";
            case AZURE -> "azure";
        };
    }

    /**
     * Returns the carbon intensity in gCO2/kWh for the given provider and region,
     * or null if not found.
     */
    protected Double getIntensity(Provider provider, String region) {
        String key = providerCode(provider) + ":" + region;
        Double value = intensities.get(key);
        if (value == null) {
            log.info("No Ember carbon intensity for {} region {}", provider, region);
        }
        return value;
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED, REGION};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{CARBON_INTENSITY};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (!enrichedValues.containsKey(ENERGY_USED)) {
            return;
        }

        String locationCode = REGION.getString(enrichedValues);
        if (locationCode == null) {
            return;
        }

        Double coeff = getIntensity(Provider.AWS, locationCode);
        if (coeff == null) {
            return;
        }
        enrichedValues.put(CARBON_INTENSITY, coeff);
    }
}
