// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static com.digitalpebble.spruce.SpruceColumn.PUE;
import static com.digitalpebble.spruce.SpruceColumn.REGION;

/**
 * Enrichment module that applies a Power Usage Effectiveness (PUE) factor.
 * <p>
 * It attempts to determine the PUE based on the region code ({@link com.digitalpebble.spruce.SpruceColumn#REGION})
 * by looking up values in a CSV resource file ({@code aws-pue.csv}).
 * <p>
 * The lookup logic follows this priority:
 * <ol>
 * <li>Exact region match (e.g., "us-east-1")</li>
 * <li>Regex pattern match (e.g., "us-.*")</li>
 * <li>Default configured value (fallback to 1.15)</li>
 * </ol>
 **/
public class PUE implements EnrichmentModule {

    private double defaultPueValue = 1.15;
    private static final String CSV_RESOURCE_PATH = "aws-pue.csv";

    private static final Map<String, Double> EXACT_MATCHES = new HashMap<>();
    private static final Map<Pattern, Double> REGEX_MATCHES = new HashMap<>();

    static {
        List<String[]> rows = Utils.loadCSV(CSV_RESOURCE_PATH);

        for (String[] parts : rows) {
            if (parts.length >= 2) {
                String key = parts[0].trim();
                try {
                    double value = Double.parseDouble(parts[1].trim());
                    // Treat as Regex if it contains wildcards
                    if (key.contains("*") || key.contains(".")) {
                        REGEX_MATCHES.put(Pattern.compile(key), value);
                    } else {
                        EXACT_MATCHES.put(key, value);
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format in PUE CSV for key: " + key);
                }
            }
        }
    }

    @Override
    public void init(Map<String, Object> params) {
        if (params != null && params.containsKey("default")) {
            Object val = params.get("default");
            if (val instanceof Number) {
                this.defaultPueValue = ((Number) val).doubleValue();
            } else if (val instanceof String) {
                try {
                    this.defaultPueValue = Double.parseDouble((String) val);
                } catch (NumberFormatException e) {
                    // ignore and keep fallback
                }
            }
        }
    }

    @Override
    public Column[] columnsNeeded() {
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
            return defaultPueValue;
        }

        if (EXACT_MATCHES.containsKey(region)) {
            return EXACT_MATCHES.get(region);
        }

        for (Map.Entry<Pattern, Double> entry : REGEX_MATCHES.entrySet()) {
            if (entry.getKey().matcher(region).matches()) {
                return entry.getValue();
            }
        }

        return defaultPueValue;
    }
}