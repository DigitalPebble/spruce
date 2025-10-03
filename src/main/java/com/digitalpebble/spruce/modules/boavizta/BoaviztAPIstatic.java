// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.*;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Adds power usage estimates for instance types used in the EC2, ElasticSearch and RDS services
 * Relies on static data in the resources directory, so that it is not necessary to run the API
 ***/
public class BoaviztAPIstatic implements EnrichmentModule {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BoaviztAPIstatic.class);

    private final static String DEFAULT_RESOURCE_LOCATION = "boavizta/instanceTypes.csv";

    private static Map<String, Impacts> impactsMap;

    private static Set<String> unknownInstanceTypes;

    @Override
    public void init(Map<String, Object> params) {

        if (unknownInstanceTypes == null) {
            unknownInstanceTypes = new HashSet<>();
        }

        if (impactsMap == null) {
            impactsMap = new HashMap<>();
        }

        try {
            List<String> estimates = Utils.loadLinesResources(DEFAULT_RESOURCE_LOCATION);
            // estimates consists of comma separated instance type, usage energy, embodied emissions
            estimates.forEach(line -> {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    return; // Skip comments and empty lines
                }
                String[] parts = line.split(",");
                if (parts.length == 4) {
                    String instanceType = parts[0].trim();
                    double energyUsed = Double.parseDouble(parts[1].trim());
                    double embodied = Double.parseDouble(parts[2].trim());
                    double adp = Double.parseDouble(parts[3].trim());
                    impactsMap.put(instanceType, new Impacts(energyUsed, embodied, adp));
                } else {
                    throw new RuntimeException("Invalid estimates mapping line: " + line);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{PRODUCT_INSTANCE_TYPE, PRODUCT_SERVICE_CODE, LINE_ITEM_OPERATION, LINE_ITEM_PRODUCT_CODE};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED, EMBODIED_EMISSIONS,EMBODIED_ADP};
    }

    @Override
    public Row process(Row row) {

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

        final Impacts impacts = impactsMap.get(instanceType);

        if (impacts == null) {
            LOG.info("Unknown instance type {}", instanceType);
            unknownInstanceTypes.add(instanceType);
            return row;
        }

        Map<Column, Object> kv = new HashMap<>();
        kv.put(ENERGY_USED, impacts.getFinalEnergyKWh());
        kv.put(EMBODIED_EMISSIONS, impacts.getEmbeddedEmissionsGramsCO2eq());
        kv.put(EMBODIED_ADP, impacts.getAbioticDepletionPotentialGramsSbeq());
        return EnrichmentModule.withUpdatedValues(row, kv);
    }
}
