// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ember;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.UsageDate;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.YearMonth;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Populate the CARBON_INTENSITY field with Ember's figure for the month the usage was incurred
 * in, falling back to the yearly figure of {@link AverageCarbonIntensity} when the month is not
 * covered: rows without a usable date, regions Ember has no monthly data for, and months Ember
 * has not published yet. The country series lag by a few months, the per-state series for the
 * US and India by more. The yearly figure comes from a separate Ember dataset and can differ
 * noticeably from the monthly one for the same region.
 */
public class MonthlyCarbonIntensity extends AverageCarbonIntensity {

    private static final Logger log = LoggerFactory.getLogger(MonthlyCarbonIntensity.class);

    private static final String MONTHLY_RESOURCE_LOCATION = "ember/ember_co2_intensity_monthly.csv";

    // keyed by "provider:region:yyyy-MM" e.g. "aws:us-east-1:2025-12"
    private final Map<String, Double> monthlyIntensities = new HashMap<>();

    @Override
    public void init(Map<String, Object> params) {
        super.init(params);
        try {
            List<String> lines = Utils.loadLinesResources(MONTHLY_RESOURCE_LOCATION);
            lines.forEach(line -> {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    return;
                }
                String[] parts = line.split(",");
                if (parts.length != 4) {
                    throw new RuntimeException("Invalid ember monthly intensity line: " + line);
                }
                String key = parts[0].trim() + ":" + parts[1].trim() + ":" + parts[2].trim();
                monthlyIntensities.put(key, Double.parseDouble(parts[3].trim()));
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (!enrichedValues.containsKey(ENERGY_USED)) {
            return;
        }
        String region = REGION.getString(enrichedValues);
        YearMonth month = UsageDate.yearMonth(row);
        if (region != null && month != null) {
            Double value = monthlyIntensities.get(getProvider().csvKey + ":" + region + ":" + month);
            if (value != null) {
                enrichedValues.put(CARBON_INTENSITY, value);
                return;
            }
            log.debug("No Ember monthly carbon intensity for {} region {} in {}, using the yearly figure",
                    getProvider(), region, month);
        }
        super.enrich(row, enrichedValues);
    }
}
