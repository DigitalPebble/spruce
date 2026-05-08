// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

import com.digitalpebble.spruce.Utils;
import com.digitalpebble.spruce.modules.boavizta.Impacts;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AWS-specific Boavizta module backed by a bundled static CSV, so SPRUCE can run without a live
 * BoaviztAPI instance. The CUR extraction lives in {@link AbstractBoaviztaAws}; this class only
 * provides the lookup.
 */
public class BoaviztAPIstatic extends AbstractBoaviztaAws {

    private static final String DEFAULT_RESOURCE_LOCATION = "boavizta/instanceTypes.csv";

    private static Map<String, Impacts> impactsMap;

    @Override
    public void init(Map<String, Object> params) {
        synchronized (BoaviztAPIstatic.class) {
            if (impactsMap == null) {
                impactsMap = new HashMap<>();
                try {
                    List<String> estimates = Utils.loadLinesResources(DEFAULT_RESOURCE_LOCATION);
                    // estimates consists of comma separated instance type, usage energy, embodied emissions, adp
                    estimates.forEach(line -> {
                        if (line.startsWith("#") || line.trim().isEmpty()) {
                            return;
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
        }
    }

    @Override
    protected Impacts lookupImpacts(String instanceType) {
        return impactsMap.get(instanceType);
    }
}
