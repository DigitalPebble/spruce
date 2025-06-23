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

import com.digitalpebble.carbonara.Column;
import com.digitalpebble.carbonara.EnrichmentModule;
import com.digitalpebble.carbonara.Provider;
import com.digitalpebble.carbonara.Utils;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.carbonara.Column.*;

/**
 * Populate the CARBON_INTENSITY and OPERATIONAL_EMISSIONS using ElecticityMaps' 2024 datasets
 **/
public class AverageCarbonIntensity implements EnrichmentModule {

    private final Map<String, Double> average_intensities = new HashMap<>();

    private final static String DEFAULT_RESOURCE_LOCATION = "electricitymaps/averages_2024.csv";

    public void init(Map<String, String> params) {
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

    /**
      Get the average intensity for the given region ID
      in gCO2perKWH
     */
    protected Double getAverageIntensity(Provider provider, String regionId) {
        String emRegionId = RegionMappings.getEMRegion(provider, regionId);
        return average_intensities.get(emRegionId);
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{CARBON_INTENSITY, OPERATIONAL_EMISSIONS};
    }

    @Override
    public Row process(Row row) {
        int index = row.fieldIndex(ENERGY_USED.toString());
        if (row.isNullAt(index)){
            return row;
        }
        double energyUsed = row.getDouble(index);

        // get the location
        // in most cases you have a product_region_code but can be product_to_region_code or product_from_region_code
        // when the traffic is between two regions or to/from the outside
        int locationIndex = -1;
        String[] regionFields = {"product_region_code", "product_from_region_code", "product_to_region_code"};
        for (String field : regionFields) {
            locationIndex = row.fieldIndex(field);
            if (!row.isNullAt(locationIndex)) {
                break;
            }
        }
        if (locationIndex == -1 || row.isNullAt(locationIndex)) {
            return row;
        }

        // get intensity for the location
        try {
            String locationCode = row.getString(locationIndex);
            final double coeff = getAverageIntensity(Provider.AWS, locationCode);
            if (coeff == 0.0d) {
                // if the coefficient is 0 it means that the region is not supported
                return row;
            }
            // compute the usage emissions
            double emissions = energyUsed * coeff;
            Map<Column, Object>  additions = new HashMap<>();
            additions.put(CARBON_INTENSITY, coeff);
            additions.put(OPERATIONAL_EMISSIONS, emissions);
            return EnrichmentModule.withUpdatedValues(row, additions);
        }
        catch (Exception exception) {
            // if the region is not supported, we cannot compute the carbon intensity
            return row;
        }
    }
}
