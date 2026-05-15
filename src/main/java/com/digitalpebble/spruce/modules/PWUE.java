// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.digitalpebble.spruce.SpruceColumn.REGION;

/**
 * Enrichment module that loads and stores Power Usage Effectiveness (PUE) 
 * and Water Usage Effectiveness (WUE) factors from a CSV resource file.
 * <p>
 * This module centralizes the loading of PUE and WUE values that were previously
 * loaded separately by the PUE and Water modules. The values are stored in
 * columns for use by downstream modules.
 * <p>
 * The lookup logic follows this priority:
 * <ol>
 * <li>Exact region match (e.g., "us-east-1")</li>
 * <li>Regex pattern match (e.g., "us-.+")</li>
 * <li>Default configured value (fallback to 1.15 for PUE, null for WUE)</li>
 * </ol>
 **/
public class PWUE implements EnrichmentModule {

    private double defaultPueValue = 1.15;
    private static final String DEFAULT_CSV_RESOURCE_PATH = "aws-pue-wue.csv";

    // PUE lookup maps
    private final Map<String, Double> pueExactMatches = new HashMap<>();
    private final Map<Pattern, Double> pueRegexMatches = new HashMap<>();

    // WUE lookup maps
    private final Map<String, Double> wueExactMatches = new HashMap<>();
    private final Map<Pattern, Double> wueRegexMatches = new HashMap<>();

    @Override
    public void init(Map<String, Object> params) {
        init(params, Provider.AWS);
    }

    @Override
    public void init(Map<String, Object> params, Provider provider) {
        String csvResourcePath = DEFAULT_CSV_RESOURCE_PATH;

        if (provider != null) {
            switch (provider) {
                case AZURE:
                    csvResourcePath = "azure-pue-wue.csv";
                    break;
                case AWS:
                default:
                    csvResourcePath = "aws-pue-wue.csv";
                    break;
            }
        }

        List<String[]> rows = Utils.loadCSV(csvResourcePath);

        for (String[] parts : rows) {
            // We need at least 3 columns: Geography, RegionID, PUE (WUE is optional)
            if (parts.length >= 3) {
                String key = parts[1].trim();
                String pueStr = parts[2].trim();
                String wueStr = parts.length >= 4 ? parts[3].trim() : "";

                // Process PUE value
                if (!pueStr.isEmpty()) {
                    try {
                        double pueValue = Double.parseDouble(pueStr);
                        
                        // Only treat as regex if it contains regex metacharacters
                        if (key.contains(".") || key.contains("+") || key.contains("*")) {
                            pueRegexMatches.put(Pattern.compile(key), pueValue);
                        } else {
                            pueExactMatches.put(key, pueValue);
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid PUE format in CSV for key: " + key);
                    }
                }

                // Process WUE value
                if (!wueStr.isEmpty()) {
                    try {
                        double wueValue = Double.parseDouble(wueStr);
                        
                        // Only treat as regex if it contains regex metacharacters
                        if (key.contains(".") || key.contains("+") || key.contains("*")) {
                            wueRegexMatches.put(Pattern.compile(key), wueValue);
                        } else {
                            wueExactMatches.put(key, wueValue);
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid WUE format in CSV for key: " + key);
                    }
                }
            }
        }

        if (params.containsKey("default")) {
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
        return new Column[]{REGION};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{SpruceColumn.PUE, SpruceColumn.WUE};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String region = REGION.getString(enrichedValues);

        // Get and store PUE value
        double pueValue = getPueForRegion(region);
        enrichedValues.put(SpruceColumn.PUE, pueValue);

        // Get and store WUE value
        Double wueValue = getWueForRegion(region);
        if (wueValue != null) {
            enrichedValues.put(SpruceColumn.WUE, wueValue);
        }
    }

    private double getPueForRegion(String region) {
        if (region == null || region.isEmpty()) {
            return defaultPueValue;
        }

        if (pueExactMatches.containsKey(region)) {
            return pueExactMatches.get(region);
        }

        for (Map.Entry<Pattern, Double> entry : pueRegexMatches.entrySet()) {
            if (entry.getKey().matcher(region).matches()) {
                return entry.getValue();
            }
        }

        return defaultPueValue;
    }

    private Double getWueForRegion(String region) {
        if (region == null || region.isEmpty()) {
            return null;
        }

        if (wueExactMatches.containsKey(region)) {
            return wueExactMatches.get(region);
        }

        for (Map.Entry<Pattern, Double> entry : wueRegexMatches.entrySet()) {
            if (entry.getKey().matcher(region).matches()) {
                return entry.getValue();
            }
        }

        return null;
    }
}