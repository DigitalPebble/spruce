// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.carbonara.modules.boavizta;

import com.digitalpebble.carbonara.*;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.spark.sql.Row;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.Map;

import static com.digitalpebble.carbonara.CURColumn.PRODUCT_SERVICE_CODE;
import static com.digitalpebble.carbonara.CarbonaraColumn.ENERGY_USED;

/**
 * Adds power usage estimates for Cloud instances
 *  Launch API locally with
 *  docker run -p 5000:5000 ghcr.io/boavizta/boaviztapi:latest
 ***/
public class BoaviztAPI implements EnrichmentModule {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BoaviztAPI.class);

    // store the default load values in a cache
    // to save a trip to the API
    private static Cache<Object, @Nullable Object> cache;

    private final String host = "http://localhost:5000";

    private BoaviztAPIClient client;

    @Override
    public void init(Map<String, String> params) {
        // TODO get the host for the API from the config
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public Row process(Row row) {

        if (cache == null) {
            cache = Caffeine.newBuilder()
                    .maximumSize(100)
                    .build();
        }

        if (client == null){
            client = new BoaviztAPIClient(host);
        }

        // TODO handle non-default CPU loads
        String service_code = PRODUCT_SERVICE_CODE.getString(row);
        if (service_code == null || !service_code.equals("AmazonEC2")) {
            return row;
        }

        String operation = CURColumn.LINE_ITEM_OPERATION.getString(row);
        if (operation == null || !operation.startsWith("RunInstances")) {
            return row;
        }

        String product_code = CURColumn.LINE_ITEM_PRODUCT_CODE.getString(row);
        if (product_code == null || !product_code.equals("AmazonEC2")) {
            return row;
        }

        String instanceType = CURColumn.PRODUCT_INSTANCE_TYPE.getString(row);
        if (instanceType == null) {
            return row;
        }

        double[] useAndEmbodiedEnergy = (double[]) cache.getIfPresent(instanceType);

        if (useAndEmbodiedEnergy == null) {
            try {
                useAndEmbodiedEnergy = client.getEnergyEstimates(Provider.AWS, instanceType);
                cache.put(instanceType, useAndEmbodiedEnergy);
            } catch (IOException e) {
                LOG.trace("Exception caught when retrieving estimates for {}", instanceType, e);
                return row;
            }
        }

        return EnrichmentModule.withUpdatedValue(row, CarbonaraColumn.ENERGY_USED, useAndEmbodiedEnergy[0]);
    }
}
