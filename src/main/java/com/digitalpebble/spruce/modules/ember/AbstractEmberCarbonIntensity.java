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
import java.util.NavigableMap;
import java.util.TreeMap;

import static com.digitalpebble.spruce.SpruceColumn.CARBON_INTENSITY;

public abstract class AbstractEmberCarbonIntensity implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(AbstractEmberCarbonIntensity.class);

    private static final String DEFAULT_RESOURCE_LOCATION = "ember/ember_co2_intensity.csv";

    // keyed by "provider:region" e.g. "aws:us-east-1", then by year
    private final Map<String, NavigableMap<Integer, Double>> intensities = new HashMap<>();

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
                if (parts.length == 4) {
                    String key = parts[0].trim() + ":" + parts[1].trim();
                    int year = Integer.parseInt(parts[2].trim());
                    double value = Double.parseDouble(parts[3].trim());
                    intensities.computeIfAbsent(key, k -> new TreeMap<>()).put(year, value);
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
     * Returns the carbon intensity in gCO2/kWh for the given provider and region in that year,
     * see {@link #forYear} for the years the file does not cover; null if the region is not
     * found.
     */
    protected Double getIntensity(Provider provider, String region, Integer year) {
        NavigableMap<Integer, Double> byYear = intensities.get(provider.csvKey + ":" + region);
        if (byYear == null) {
            log.info("No Ember carbon intensity for {} region {}", provider, region);
            return null;
        }
        return forYear(byYear, year);
    }

    /**
     * Returns the figure published for that year, else the closest earlier year, else the first
     * year published; the latest year when the year is null, i.e. the row has no usable date.
     */
    static Double forYear(NavigableMap<Integer, Double> byYear, Integer year) {
        Map.Entry<Integer, Double> entry = year == null ? byYear.lastEntry() : byYear.floorEntry(year);
        if (entry == null) {
            entry = byYear.firstEntry();
        }
        return entry == null ? null : entry.getValue();
    }
}
