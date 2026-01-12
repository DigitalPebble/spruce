// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Enrichment module that applies a Power Usage Effectiveness (PUE) factor.
 * <p>
 * It attempts to determine the PUE based on the region code ({@link com.digitalpebble.spruce.CURColumn#PRODUCT_REGION_CODE})
 * by looking up values in a CSV resource file ({@code aws-pue.csv}).
 * <p>
 * The lookup logic follows this priority:
 * <ol>
 * <li>Exact region match (e.g., "us-east-1")</li>
 * <li>Regex pattern match (e.g., "us-.*")</li>
 * <li>Default global average fallback</li>
 * </ol>
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology/#power-usage-effectiveness">CCF methodology</a>
 **/
public class PUE implements EnrichmentModule {

    // UPDATE: Maintainer requested 1.15 as default
    private static final double DEFAULT_PUE_VALUE = 1.15;
    private static final String CSV_RESOURCE_PATH = "aws-pue.csv";

    private static final Map<String, Double> EXACT_MATCHES = new HashMap<>();
    private static final Map<Pattern, Double> REGEX_MATCHES = new HashMap<>();

    static {
        // Use the new reusable method in Utils
        Utils.loadCSVToMaps(CSV_RESOURCE_PATH, EXACT_MATCHES, REGEX_MATCHES);
    }

    @Override
    public Column[] columnsNeeded() {
        // UPDATE: Using the new SpruceColumn.REGION
        return new Column[]{ENERGY_USED, REGION};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{PUE};
    }

    @Override
    public Row process(Row row) {
        if (ENERGY_USED.isNullAt(row)) {
            return row;
        }

        double energyUsed = ENERGY_USED.getDouble(row);
        if (energyUsed <= 0) return row;

        String region = null;
        if (!REGION.isNullAt(row)) {
            region = REGION.getString(row);
        }

        double pueToApply = getPueForRegion(region);

        return EnrichmentModule.withUpdatedValue(row, PUE, pueToApply);
    }

    private double getPueForRegion(String region) {
        if (region == null || region.isEmpty()) {
            return DEFAULT_PUE_VALUE;
        }

        if (EXACT_MATCHES.containsKey(region)) {
            return EXACT_MATCHES.get(region);
        }

        for (Map.Entry<Pattern, Double> entry : REGEX_MATCHES.entrySet()) {
            if (entry.getKey().matcher(region).matches()) {
                return entry.getValue();
            }
        }

        return DEFAULT_PUE_VALUE;
    }
}