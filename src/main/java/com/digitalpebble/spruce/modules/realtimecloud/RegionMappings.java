// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.realtimecloud;

import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.Utils;

import java.io.IOException;
import java.util.List;

/**
 * Maps regions to ElectricityMaps region IDs for different cloud providers.
 * The mappings are loaded from a CSV file located in the resources directory.
 * Source: https://github.com/Green-Software-Foundation/real-time-cloud
 */
public class RegionMappings {

    private static final java.util.Map<Provider, java.util.Map<String, String>> mappings = new java.util.HashMap<>();

    // load the resources
    static {
        try {
            List<String> region_mappings = Utils.loadLinesResources("gsf_realtime_cloud/region-mappings.csv");
            // regions mappings consists of comma separated Provider, region, EM region ID
            region_mappings.forEach(line -> {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    return; // Skip comments and empty lines
                }
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    Provider provider = Provider.fromString(parts[0].trim());
                    String region = parts[1].trim();
                    String emRegionId = parts[2].trim();
                    mappings.computeIfAbsent(provider, k -> new java.util.HashMap<>()).put(region, emRegionId);
                } else {
                    throw new RuntimeException("Invalid region mapping line: " + line);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the ElectricityMaps region ID for a given cloud provider and region.
     *
     * @param p      The cloud provider (e.g., AWS, GOOGLE, AZURE).
     * @param region The region name (e.g., "us-east-1").
     * @return The corresponding ElectricityMaps region ID or null if the region or provider is not supported.
     */
    public static String getEMRegion(Provider p, String region) {
        java.util.Map<String, String> regionMap = mappings.get(p);
        if (regionMap == null) {
            return null;
        }
        return regionMap.get(region);
    }

}
