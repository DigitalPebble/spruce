// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.spark.sql.Row;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;

/**
 * Adds power usage estimates for instance types used in the EC2, ElasticSearch and RDS services
 * Launch API locally with
 * docker run -p 5000:5000 ghcr.io/boavizta/boaviztapi:latest
 ***/
public class BoaviztAPI implements EnrichmentModule {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BoaviztAPI.class);

    // store the default load values in a cache
    // to save a trip to the API
    private static Cache<String, @Nullable BoaviztaResult> cache;

    private static Set<String> unknownInstanceTypes;

    private String address = "http://localhost:5000";

    private BoaviztAPIClient client;

    @Override
    public void init(Map<String, Object> params) {
        String a = (String) params.get("address");
        if (a != null) {
            address = a;
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{PRODUCT_INSTANCE_TYPE, PRODUCT_SERVICE_CODE, LINE_ITEM_OPERATION, LINE_ITEM_PRODUCT_CODE};
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

        if (client == null) {
            client = new BoaviztAPIClient(address);
        }

        if (unknownInstanceTypes == null) {
            unknownInstanceTypes = new HashSet<>();
        }

        // TODO handle non-default CPU loads

        String instanceType = PRODUCT_INSTANCE_TYPE.getString(row);
        if (instanceType == null) {
            return row;
        }

        final String service_code = PRODUCT_SERVICE_CODE.getString(row);
        final String operation = LINE_ITEM_OPERATION.getString(row);
        final String product_code = LINE_ITEM_PRODUCT_CODE.getString(row);

        if (operation == null || product_code == null) {
            return row;
        }

        // conditions for EC2 instances
        if (product_code.equals("AmazonEC2") && operation.startsWith("RunInstances") && "AmazonEC2".equals(service_code)) {
            LOG.debug("EC2 instance {}", instanceType);
        }
        // conditions for search service
        else if (product_code.equals("AmazonES") && operation.equals("ESDomain")) {
            LOG.debug("Search instance {}", instanceType);
            // remove the '.search' suffix
            if (instanceType.endsWith(".search")) {
                instanceType = instanceType.replace(".search", "");
            }
        } else if (product_code.equals("AmazonRDS") && operation.startsWith("CreateDBInstance")) {
            LOG.debug("RDS instance {}", instanceType);
            // remove the 'db.' prefix
            if (instanceType.startsWith("db.")) {
                instanceType = instanceType.substring(3);
            }
        } else {
            return row;
        }

        // don't look for instance types that are known to be unknown
        if (unknownInstanceTypes.contains(instanceType)) {
            return row;
        }

        BoaviztaResult useAndEmbodiedEnergy = cache.getIfPresent(instanceType);

        if (useAndEmbodiedEnergy == null) {
            try {
                useAndEmbodiedEnergy = client.getEnergyAndEmbodiedEmissionsEstimates(Provider.AWS, instanceType);
                cache.put(instanceType, useAndEmbodiedEnergy);
            } catch (InstanceTypeUknown e1) {
                LOG.info("Unknown instance type {}", instanceType);
                unknownInstanceTypes.add(instanceType);
                return row;
            } catch (IOException e) {
                LOG.error("Exception caught when retrieving estimates for {}", instanceType, e);
                return row;
            }
        }

        return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, useAndEmbodiedEnergy.getFinalEnergyKWh());
    }
}
