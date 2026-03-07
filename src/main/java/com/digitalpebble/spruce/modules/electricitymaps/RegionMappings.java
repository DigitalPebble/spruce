// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.electricitymaps;

import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.Utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Maps regions to ElectricityMaps zone keys for different cloud providers.
 * The mappings are loaded from electricitymaps/em-datacenters.json in the resources directory.
 */
public class RegionMappings {

    private static final java.util.Map<Provider, java.util.Map<String, String>> mappings = new java.util.HashMap<>();

    // load the resources
    static {
        try {
            List<Map<String, Object>> datacenters = Utils.loadJSONArrayResources("electricitymaps/em-datacenters.json");
            for (Map<String, Object> entry : datacenters) {
                String providerCode = (String) entry.get("provider");
                String region = (String) entry.get("region");
                String zoneKey = (String) entry.get("zoneKey");
                if (providerCode == null || region == null || zoneKey == null) {
                    continue;
                }
                Provider provider = providerFromCode(providerCode);
                if (provider == null) {
                    continue;
                }
                mappings.computeIfAbsent(provider, k -> new java.util.HashMap<>()).put(region, zoneKey);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Provider providerFromCode(String code) {
        return switch (code.toLowerCase()) {
            case "aws" -> Provider.AWS;
            case "gcp" -> Provider.GOOGLE;
            case "azure" -> Provider.AZURE;
            default -> null;
        };
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
