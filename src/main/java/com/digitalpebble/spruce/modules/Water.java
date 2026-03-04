// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.Utils;
import com.digitalpebble.spruce.modules.realtimecloud.RegionMappings;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Enrichment module that estimates water consumption from cooling and energy generation.
 * <p>
 * It produces two fields:
 * <ul>
 * <li>{@link com.digitalpebble.spruce.SpruceColumn#WATER_COOLING} – water used for data centre
 *     cooling, computed as {@code energy_kwh * PUE * WUE} (litres). The WUE (Water Usage
 *     Effectiveness) is looked up per AWS region from {@code aws-pue-wue.csv}.</li>
 * <li>{@link com.digitalpebble.spruce.SpruceColumn#WATER_ENERGY} – water consumed during
 *     electricity generation, computed as {@code energy_kwh * PUE * WCF} (litres). The WCF
 *     (Water Consumption Factor) is looked up per Electricity Maps zone from
 *     {@code em-locations-wcf.csv}.</li>
 * </ul>
 */
public class Water implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(Water.class);

    private static final String WUE_CSV = "aws-pue-wue.csv";
    private static final String WCF_CSV = "em-locations-wcf.csv";

    // WUE lookup by AWS region (exact and regex, same logic as PUE)
    private final Map<String, Double> wueExactMatches = new HashMap<>();
    private final Map<Pattern, Double> wueRegexMatches = new HashMap<>();

    // WCF lookup by Electricity Maps zone ID
    private final Map<String, Double> wcfByZone = new HashMap<>();

    @Override
    public void init(Map<String, Object> params) {
        // Load WUE values from column index 3 of the PUE-WUE CSV
        List<String[]> pueWueRows = Utils.loadCSV(WUE_CSV);
        for (String[] parts : pueWueRows) {
            if (parts.length >= 4) {
                String key = parts[1].trim();
                String wueStr = parts[3].trim();
                if (wueStr.isEmpty()) continue;
                try {
                    double wue = Double.parseDouble(wueStr);
                    if (key.contains(".") || key.contains("+") || key.contains("*")) {
                        wueRegexMatches.put(Pattern.compile(key), wue);
                    } else {
                        wueExactMatches.put(key, wue);
                    }
                } catch (NumberFormatException e) {
                    log.warn("Invalid WUE value in {} for key: {}", WUE_CSV, key);
                }
            }
        }

        // Load WCF values from em-locations-wcf.csv
        List<String[]> wcfRows = Utils.loadCSV(WCF_CSV);
        for (String[] parts : wcfRows) {
            if (parts.length >= 2) {
                String zoneId = parts[0].trim();
                String wcfStr = parts[1].trim();
                if (wcfStr.isEmpty()) continue;
                try {
                    double wcf = Double.parseDouble(wcfStr);
                    wcfByZone.put(zoneId, wcf);
                } catch (NumberFormatException e) {
                    log.warn("Invalid WCF value in {} for zone: {}", WCF_CSV, zoneId);
                }
            }
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED, PUE, REGION};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{WATER_COOLING, WATER_ENERGY};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        Double energyUsed = ENERGY_USED.getDouble(enrichedValues);
        if (energyUsed == null || energyUsed <= 0) return;

        String region = REGION.getString(enrichedValues);
        if (region == null || region.isEmpty()) return;

        Double pueVal = PUE.getDouble(enrichedValues);
        double pue = pueVal != null ? pueVal : 1.0;

        double totalEnergy = energyUsed * pue;

        // Water from cooling (WUE)
        Double wue = getWueForRegion(region);
        if (wue != null) {
            enrichedValues.put(WATER_COOLING, totalEnergy * wue);
        }

        // Water from energy generation (WCF)
        String emZone = RegionMappings.getEMRegion(Provider.AWS, region);
        if (emZone != null) {
            Double wcf = wcfByZone.get(emZone);
            if (wcf != null) {
                enrichedValues.put(WATER_ENERGY, totalEnergy * wcf);
            }
        }
    }

    private Double getWueForRegion(String region) {
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
