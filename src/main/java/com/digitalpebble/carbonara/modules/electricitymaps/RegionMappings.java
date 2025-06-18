/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara.modules.electricitymaps;

import com.digitalpebble.carbonara.Provider;
import com.digitalpebble.carbonara.Utils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 * Maps regions to ElectricityMaps region IDs for different cloud providers.
 * The mappings are loaded from a CSV file located in the resources directory.
 */
public class RegionMappings {

    private static final java.util.Map<Provider, java.util.Map<String, String>> mappings = new java.util.HashMap<>();

    // load the resources
    static {
        try {
            List<String> region_mappings = Utils.loadLinesResources("electricitymaps/region-mappings.csv");
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
     * @return The corresponding ElectricityMaps region ID.
     * @throws RuntimeException if the region is not supported.
     */
    public static String getEMRegion(Provider p, String region) {

        java.util.Map<String, String> regionMap = mappings.get(p);

        String regionKey = regionMap.get(region);
        if (regionKey == null) {
            throw new RuntimeException("Unsupported region for provider " + p + ": " + region);
        }
        return regionKey;
    }

}
