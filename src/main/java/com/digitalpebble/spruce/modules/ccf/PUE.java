// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static com.digitalpebble.spruce.SpruceColumn.PUE;

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

    private static final double DEFAULT_PUE_VALUE = 1.135;
    private static final String CSV_RESOURCE_PATH = "/aws-pue.csv";

    // Static caches for PUE data to avoid reloading the CSV for every instance
    private static final Map<String, Double> EXACT_MATCHES = new HashMap<>();
    private static final Map<Pattern, Double> REGEX_MATCHES = new HashMap<>();

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED, PRODUCT_REGION_CODE};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{PUE};
    }

    @Override
    public Row process(Row row) {
        // apply only to rows corresponding for which energy usage
        // exists and has been estimated
        if (ENERGY_USED.isNullAt(row)) {
            return row;
        }
        double energyUsed = ENERGY_USED.getDouble(row);
        if (energyUsed <= 0) return row;

        String region = null;
        if (!PRODUCT_REGION_CODE.isNullAt(row)) {
            region = PRODUCT_REGION_CODE.getString(row);
        }

        double pueToApply = getPueForRegion(region);

        return EnrichmentModule.withUpdatedValue(row, PUE, pueToApply);
    }

    /**
     * Resolves the PUE value for a given region string.
     *
     * @param region The region code (e.g. "us-east-1")
     * @return The specific PUE if found, otherwise the default global average.
     */
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
