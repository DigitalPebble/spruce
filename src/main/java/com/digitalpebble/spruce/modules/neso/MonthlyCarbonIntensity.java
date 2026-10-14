// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.neso;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.UsageDate;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.time.Month;
import java.time.YearMonth;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Populates the CARBON_INTENSITY field with the average of the regional carbon intensity
 * published by the UK Carbon Intensity API for the month the usage was incurred in. Only the
 * cloud regions located in Great Britain are covered; the module leaves the other regions
 * untouched, so it goes after the Ember module in the chain and overrides its yearly figure
 * where it has a monthly one.
 */
public class MonthlyCarbonIntensity implements EnrichmentModule {

    private static final String DEFAULT_RESOURCE_LOCATION = "carbonintensity-uk/monthly_co2_intensity.csv";

    // keyed by "provider:region" e.g. "aws:eu-west-2", then by calendar month and year
    private final Map<String, Map<Month, NavigableMap<Integer, Double>>> intensities = new HashMap<>();

    /** Set via {@link #init(Map, Provider)}, left null so that a lookup without it fails loudly. */
    private Provider provider;

    @Override
    public void init(Map<String, Object> params, Provider provider) {
        this.provider = provider;
        init(params);
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
                if (parts.length != 4) {
                    throw new RuntimeException("Invalid monthly intensity line: " + line);
                }
                String key = parts[0].trim() + ":" + parts[1].trim();
                YearMonth month = YearMonth.parse(parts[2].trim());
                double value = Double.parseDouble(parts[3].trim());
                intensities.computeIfAbsent(key, k -> new EnumMap<>(Month.class))
                        .computeIfAbsent(month.getMonth(), m -> new TreeMap<>())
                        .put(month.getYear(), value);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED, REGION};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{CARBON_INTENSITY};
    }

    /**
     * Returns the carbon intensity in gCO2/kWh for the given provider and region in that month,
     * or null if the region is not covered. Months outside the period of the file get the
     * figure of the same calendar month in the closest earlier year, else in the first year
     * published, so that the seasonal variation is kept.
     */
    Double getIntensity(Provider provider, String region, YearMonth month) {
        Map<Month, NavigableMap<Integer, Double>> byMonth = intensities.get(provider.csvKey + ":" + region);
        if (byMonth == null) {
            return null;
        }
        NavigableMap<Integer, Double> byYear = byMonth.get(month.getMonth());
        if (byYear == null) {
            return null;
        }
        Map.Entry<Integer, Double> entry = byYear.floorEntry(month.getYear());
        if (entry == null) {
            entry = byYear.firstEntry();
        }
        return entry.getValue();
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (!enrichedValues.containsKey(ENERGY_USED)) {
            return;
        }

        String region = REGION.getString(enrichedValues);
        if (region == null) {
            return;
        }

        // without a date, keep the yearly figure set by the previous module
        YearMonth month = UsageDate.yearMonth(row);
        if (month == null) {
            return;
        }

        Double coeff = getIntensity(provider, region, month);
        if (coeff != null) {
            enrichedValues.put(CARBON_INTENSITY, coeff);
        }
    }
}
