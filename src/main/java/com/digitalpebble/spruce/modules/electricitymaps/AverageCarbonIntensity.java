// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.electricitymaps;

import com.digitalpebble.spruce.*;

import static com.digitalpebble.spruce.CURColumn.PRODUCT_FROM_REGION_CODE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_TO_REGION_CODE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_REGION_CODE;
import static com.digitalpebble.spruce.CarbonaraColumn.*;

import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Populate the CARBON_INTENSITY and OPERATIONAL_EMISSIONS using ElecticityMaps' 2024 datasets
 **/
public class AverageCarbonIntensity implements EnrichmentModule {

    private final Map<String, Double> average_intensities = new HashMap<>();

    private final static String DEFAULT_RESOURCE_LOCATION = "electricitymaps/averages_2024.csv";

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
        return new Column[]{ENERGY_USED, PRODUCT_REGION_CODE, PRODUCT_FROM_REGION_CODE, PRODUCT_TO_REGION_CODE};
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
        if (ENERGY_USED.isNullAt(row)) {
            return row;
        }
        double energyUsed = ENERGY_USED.getDouble(row);

        String locationCode = null;
        // get the location
        // in most cases you have a product_region_code but can be product_to_region_code or product_from_region_code
        // when the traffic is between two regions or to/from the outside
        final Column[] location_columns = new Column[]{PRODUCT_REGION_CODE, PRODUCT_FROM_REGION_CODE, PRODUCT_TO_REGION_CODE};
        for (Column c : location_columns) {
            locationCode = c.getString(row);
            if (locationCode != null) {
                break;
            }
        }

        //  no location found - skip
        if (locationCode == null) {
            return row;
        }

        // get intensity for the location
        try {
            final double coeff = getAverageIntensity(Provider.AWS, locationCode);
            if (coeff == 0.0d) {
                // if the coefficient is 0 it means that the region is not supported
                return row;
            }
            // compute the usage emissions
            double emissions = energyUsed * coeff;
            Map<Column, Object> additions = new HashMap<>();
            additions.put(CARBON_INTENSITY, coeff);
            additions.put(OPERATIONAL_EMISSIONS, emissions);
            return EnrichmentModule.withUpdatedValues(row, additions);
        } catch (Exception exception) {
            // if the region is not supported, we cannot compute the carbon intensity
            return row;
        }
    }
}
