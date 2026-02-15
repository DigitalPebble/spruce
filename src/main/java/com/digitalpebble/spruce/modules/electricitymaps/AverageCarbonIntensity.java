// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.electricitymaps;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.Utils;
import com.digitalpebble.spruce.modules.realtimecloud.RegionMappings;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Populate the CARBON_INTENSITY field using ElecticityMaps' 2024 datasets
 * for rows where energy usage has been estimated.
 **/
public class AverageCarbonIntensity implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(AverageCarbonIntensity.class);

    private final static String DEFAULT_RESOURCE_LOCATION = "electricitymaps/averages_2024.csv";
    private final Map<String, Double> average_intensities = new HashMap<>();

    public void init(Map<String, Object> params) {
        // load the averages for each EM IDs
        try {
            List<String> averages = Utils.loadLinesResources(DEFAULT_RESOURCE_LOCATION);
            // averages consists of comma separated EM region ID, average carbon intensity
            averages.forEach(line -> {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    return; // Skip comments and empty lines
                }
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String emRegionId = parts[0].trim();
                    double average = Double.parseDouble(parts[1].trim());
                    average_intensities.put(emRegionId, average);
                } else {
                    throw new RuntimeException("Invalid average mapping line: " + line);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED, REGION};
    }

    /**
     * Get the average intensity for the given region ID
     * in gCO2perKWH
     * or null if the region does not exist
     */
    protected Double getAverageIntensity(Provider provider, String regionId) {
        String emRegionId = RegionMappings.getEMRegion(provider, regionId);
        if (emRegionId == null) {
            log.info("Region unknown {} for {}", regionId, provider);
            return null;
        }
        return average_intensities.get(emRegionId);
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{CARBON_INTENSITY};
    }

    @Override
    public void enrich(Row inputRow, Map<Column, Object> enrichedValues) {
        if (!enrichedValues.containsKey(ENERGY_USED)) {
            return;
        }

        String locationCode = REGION.getString(enrichedValues);
        //  no location found - skip
        if (locationCode == null) {
            return;
        }

        // get intensity for the location
        Double coeff = getAverageIntensity(Provider.AWS, locationCode);
        if (coeff == null) {
            // if the coefficient is 0 it means that the region is not supported
            return;
        }
        enrichedValues.put(CARBON_INTENSITY, coeff);
    }
}
