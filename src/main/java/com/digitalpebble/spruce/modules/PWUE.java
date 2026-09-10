// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.UsageDate;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
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
 * Providers publish these factors per year and they move noticeably from one year to the
 * next, so the figures are keyed by region <em>and</em> year rather than applied as a blanket
 * value. The year comes from the usage date of the line item; when the report carries no
 * usable date the most recent figures are used.
 * <p>
 * The lookup logic follows this priority:
 * <ol>
 * <li>Exact region match (e.g., "us-east-1")</li>
 * <li>Regex pattern match on the region id (e.g., "eu-.+"), i.e. the geography average</li>
 * <li>The provider-wide "GLOBAL" entry, where the CSV has one</li>
 * <li>Default configured value (fallback to 1.15 for PUE, null for WUE)</li>
 * </ol>
 * Within a tier, the entry for the usage year is used; if that year is not covered, the
 * closest year available for that region is used instead.
 **/
public class PWUE implements EnrichmentModule {

    private double defaultPueValue = 1.15;
    private static final String DEFAULT_CSV_RESOURCE_PATH = "aws-pue-wue.csv";

    /** RegionID marking the provider-wide average, used when no region entry matches. */
    private static final String GLOBAL_KEY = "GLOBAL";

    /** Year given to entries whose CSV row leaves the year empty, i.e. figures that are not
     *  broken down by year. Below any real year, so it is only picked when nothing else is. */
    private static final int UNDATED = 0;

    /** Year used for rows with no usable usage date: yields the most recent figures. */
    private static final int LATEST = Integer.MAX_VALUE;

    /** PUE and WUE by year for one CSV key — a region id, or a regex over region ids. */
    private static class Factors implements Serializable {
        /** null when the key is an exact region id rather than a pattern. */
        final Pattern pattern;
        final NavigableMap<Integer, Double> pue = new TreeMap<>();
        final NavigableMap<Integer, Double> wue = new TreeMap<>();

        Factors(Pattern pattern) {
            this.pattern = pattern;
        }
    }

    private final Map<String, Factors> exactMatches = new HashMap<>();
    /** A list rather than a map so the patterns are tried in the order the CSV lists them. */
    private final List<Factors> regexMatches = new ArrayList<>();
    private Factors globalMatch;

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
            // We need at least 4 columns: Geography, RegionID, Year, PUE (WUE is optional)
            if (parts.length < 4) {
                continue;
            }
            String key = parts[1].trim();
            String yearStr = parts[2].trim();
            String pueStr = parts[3].trim();
            String wueStr = parts.length >= 5 ? parts[4].trim() : "";

            int year = UNDATED;
            if (!yearStr.isEmpty()) {
                try {
                    year = Integer.parseInt(yearStr);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid year format in CSV for key: " + key);
                    continue;
                }
            }

            Factors factors = factorsFor(key);
            store(factors.pue, key, year, pueStr, "PUE");
            store(factors.wue, key, year, wueStr, "WUE");
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

    /** Returns the entry holding the figures for this CSV key, creating it on first sight. */
    private Factors factorsFor(String key) {
        if (GLOBAL_KEY.equals(key)) {
            if (globalMatch == null) {
                globalMatch = new Factors(null);
            }
            return globalMatch;
        }
        // Only treat as regex if it contains regex metacharacters
        if (key.contains(".") || key.contains("+") || key.contains("*")) {
            for (Factors existing : regexMatches) {
                if (existing.pattern.pattern().equals(key)) {
                    return existing;
                }
            }
            Factors factors = new Factors(Pattern.compile(key));
            regexMatches.add(factors);
            return factors;
        }
        return exactMatches.computeIfAbsent(key, k -> new Factors(null));
    }

    private static void store(NavigableMap<Integer, Double> byYear, String key, int year,
                              String value, String label) {
        if (value.isEmpty()) {
            return;
        }
        try {
            byYear.put(year, Double.parseDouble(value));
        } catch (NumberFormatException e) {
            System.err.println("Invalid " + label + " format in CSV for key: " + key);
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
        int year = usageYear(row);

        // Get and store PUE value
        Double pueValue = lookup(region, year, false);
        enrichedValues.put(SpruceColumn.PUE, pueValue != null ? pueValue : defaultPueValue);

        // Get and store WUE value
        Double wueValue = lookup(region, year, true);
        if (wueValue != null) {
            enrichedValues.put(SpruceColumn.WUE, wueValue);
        }
    }

    /** Returns the year the line item was incurred in, or {@link #LATEST} if it has no date. */
    private static int usageYear(Row row) {
        Integer year = UsageDate.year(row);
        return year != null ? year : LATEST;
    }

    private Double lookup(String region, int year, boolean water) {
        if (region != null && !region.isEmpty()) {
            Double value = valueFor(exactMatches.get(region), year, water);
            if (value != null) {
                return value;
            }
            for (Factors factors : regexMatches) {
                if (factors.pattern.matcher(region).matches()) {
                    value = valueFor(factors, year, water);
                    if (value != null) {
                        return value;
                    }
                }
            }
        }
        return valueFor(globalMatch, year, water);
    }

    /** Returns the figure published for that year, or the closest year available. */
    private static Double valueFor(Factors factors, int year, boolean water) {
        if (factors == null) {
            return null;
        }
        NavigableMap<Integer, Double> byYear = water ? factors.wue : factors.pue;
        Map.Entry<Integer, Double> entry = byYear.floorEntry(year);
        if (entry == null) {
            entry = byYear.firstEntry();
        }
        return entry == null ? null : entry.getValue();
    }
}
