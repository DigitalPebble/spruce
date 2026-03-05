// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides water statistics per Electricity Maps zone ID.
 * <ul>
 *   <li>Water Consumption Factor (WCF) in l/kWh — loaded from {@code em-locations-wcf.csv}</li>
 *   <li>Water Stress Category (0–4) — loaded from {@code em-water-stress.csv},
 *       derived from Aqueduct 4.0 baseline water stress</li>
 * </ul>
 */
public class WaterStats {

    private static final String WCF_CSV = "em-locations-wcf.csv";
    private static final String WATER_STRESS_CSV = "em-water-stress.csv";

    private static final Map<String, Double> wcfByZone = new HashMap<>();
    private static final Map<String, Integer> waterStressByZone = new HashMap<>();

    static {
        // Load WCF values
        List<String[]> wcfRows = Utils.loadCSV(WCF_CSV);
        for (String[] parts : wcfRows) {
            if (parts.length >= 2) {
                String zoneId = parts[0].trim();
                String wcfStr = parts[1].trim();
                if (!wcfStr.isEmpty()) {
                    try {
                        wcfByZone.put(zoneId, Double.parseDouble(wcfStr));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }

        // Load water stress categories
        List<String[]> stressRows = Utils.loadCSV(WATER_STRESS_CSV);
        for (String[] parts : stressRows) {
            if (parts.length >= 2) {
                String zoneId = parts[0].trim();
                String catStr = parts[1].trim();
                if (!catStr.isEmpty()) {
                    try {
                        waterStressByZone.put(zoneId, Integer.parseInt(catStr));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }
    }

    /**
     * Returns the Water Consumption Factor (l/kWh) for the given zone, or null if not available.
     */
    public static Double getWCF(String zoneId) {
        return wcfByZone.get(zoneId);
    }

    /**
     * Returns the water stress category (0–4) for the given zone, or null if not available.
     */
    public static Integer getWaterStressCategory(String zoneId) {
        return waterStressByZone.get(zoneId);
    }
}
