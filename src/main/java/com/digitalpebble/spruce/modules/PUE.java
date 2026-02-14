// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
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
 * <li>Regex pattern match (e.g., "us-.+")</li>
 * <li>Default configured value (fallback to 1.15)</li>
 * </ol>
 **/
public class PUE implements EnrichmentModule {

    private double defaultPueValue = 1.15;
    private static final String CSV_RESOURCE_PATH = "aws-pue.csv";

    private final Map<String, Double> exactMatches = new HashMap<>();
    private final Map<Pattern, Double> regexMatches = new HashMap<>();

    @Override
    public void init(Map<String, Object> params) {
        List<String[]> rows = Utils.loadCSV(CSV_RESOURCE_PATH);

        for (String[] parts : rows) {
            if (parts.length >= 3) {
                String key = parts[1].trim();
                try {
                    double value = Double.parseDouble(parts[2].trim());

                    // Only treat as regex if it contains regex metacharacters
                    if (key.contains(".") || key.contains("+") || key.contains("*")) {
                        regexMatches.put(Pattern.compile(key), value);
                    } else {
                        exactMatches.put(key, value);
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format in PUE CSV for key: " + key);
                }
            }
        }

        if (params != null && params.containsKey("default")) {
            Object val = params.get("default");
            if (val instanceof Number) {
                this.defaultPueValue = ((Number) val).doubleValue();
            } else if (val instanceof String) {
                try {
                    this.defaultPueValue = Double.parseDouble((String) val);
                } catch (NumberFormatException e) {
                    // ignore
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
        return new Column[]{SpruceColumn.PUE};
    }

    @Override
    public void enrich(Row inputRow, Map<Column, Object> enrichedValues) {
        Object energyObj = enrichedValues.get(ENERGY_USED);
        if (energyObj == null) return;

        double energyUsed = (Double) energyObj;
        if (energyUsed <= 0) return;

        String region = (String) enrichedValues.get(REGION);

        double pueToApply = getPueForRegion(region);

        enrichedValues.put(SpruceColumn.PUE, pueToApply);
    }

    private double getPueForRegion(String region) {
        if (region == null || region.isEmpty()) {
            return defaultPueValue;
        }

        if (exactMatches.containsKey(region)) {
            return exactMatches.get(region);
        }

        for (Map.Entry<Pattern, Double> entry : regexMatches.entrySet()) {
            if (entry.getKey().matcher(region).matches()) {
                return entry.getValue();
            }
        }

        return defaultPueValue;
    }
}
