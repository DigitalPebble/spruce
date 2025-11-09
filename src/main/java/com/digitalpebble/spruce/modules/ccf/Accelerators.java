// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.*;
import static com.digitalpebble.spruce.Utils.loadJSONResources;

public class Accelerators implements EnrichmentModule {

    private static final Logger LOG = LoggerFactory.getLogger(Accelerators.class);

    public int gpu_utilisation_percent = 50;
    public Map<String, Map> gpu_instance_types;
    public Map<String, Map> gpu_info;

    public void init(Map<String, Object> params) {
        // specify a gpu utilisation
        Integer val = (Integer) params.get("gpu_utilisation_percent");
        if (val != null) {
            gpu_utilisation_percent = val;
        }
        LOG.info("gpu_utilisation_percent: {}", gpu_utilisation_percent);

        try {
            Map<String, Object> map = loadJSONResources("ccf/accelerators.json");
            gpu_instance_types = (Map<String, Map>) map.get("GPU_INSTANCES_TYPES");
            gpu_info = (Map<String, Map>) map.get("GPU_INFO");
        } catch (
                IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{PRODUCT_INSTANCE_TYPE, LINE_ITEM_OPERATION, LINE_ITEM_PRODUCT_CODE, USAGE_AMOUNT};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public Row process(Row row) {
        // limit to EC2 instances

        String instanceType = PRODUCT_INSTANCE_TYPE.getString(row);
        if (instanceType == null) {
            return row;
        }

        final String operation = LINE_ITEM_OPERATION.getString(row);
        final String product_code = LINE_ITEM_PRODUCT_CODE.getString(row);

        if (operation == null || product_code == null) {
            return row;
        }

        // conditions for EC2 instances
        if (product_code.equals("AmazonEC2") && operation.startsWith("RunInstances")) {
            LOG.debug("EC2 instance {}", instanceType);
        }
        else {
            return row;
        }

        // check that they have a GPU
        // and how many of them
        Map instanceTypeInfo = gpu_instance_types.get(instanceType);
        if (instanceTypeInfo == null) {
            // check product instance family
            // if GPU then log if we have no info about it
            String fam = PRODUCT_INSTANCE_FAMILY.getString(row, true);
            if ("GPU instance".equals(fam)) {
                LOG.debug("Lacking info for instance type with GPU {}", instanceType);
            }
            return row;
        }

        String gpu = instanceTypeInfo.get("type").toString();
        int quantity = (Integer)instanceTypeInfo.get("quantity");

        // get the min max range for the GPU
        int minWatts = (Integer) gpu_info.get(gpu).get("min");
        int maxWatts = (Integer) gpu_info.get(gpu).get("max");

        // work out the estimated usage
        // minWatts + (gpu_utilisation_percent / 100) * (maxWatts - minWatts)
        double energy_used = minWatts + ((double) gpu_utilisation_percent / 100) * (maxWatts - minWatts);

        double amount = USAGE_AMOUNT.getDouble(row);

        // watts to kw
        energy_used = (amount * energy_used * quantity / 1000);

        // add it to an existing value or create it
        return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, energy_used, true);
    }
}
