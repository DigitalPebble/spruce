// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.Utils.loadJSONResources;

public class Accelerators implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(Networking.class);

    public int gpu_utilisation_percent = 50;
    public Map<String, Map> gpu_instance_types;
    public Map<String, Map> gpu_info;

    public void init(Map<String, Object> params) {
        // specify a gpu utilisation
        Integer val = (Integer) params.get("gpu_utilisation_percent");
        if (val != null) {
            gpu_utilisation_percent = val;
        }
        log.info("gpu_utilisation_percent: {}", gpu_utilisation_percent);

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
        return new Column[0];
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[0];
    }

    @Override
    public Row process(Row row) {
        // limit to EC2 instances

        // check that they have a GPU
        // and how many of them

        // get the min max range for the GPU

        // work out the estimated usage
        // minWatts + (gpu_utilisation_percent / 100) * (maxWatts - minWatts)

        // add it to an existing value or create it

        return null;
    }
}
