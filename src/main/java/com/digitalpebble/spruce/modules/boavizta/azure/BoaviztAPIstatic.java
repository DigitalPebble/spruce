// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.azure;

import com.digitalpebble.spruce.modules.boavizta.Impacts;

import java.util.Map;

/**
 * Azure-specific Boavizta module backed by a bundled static CSV, so SPRUCE can run without a live
 * BoaviztAPI instance. The extraction lives in {@link AbstractBoaviztaAzure}; this class only
 * provides the lookup.
 */
public class BoaviztAPIstatic extends AbstractBoaviztaAzure {

    private static final String DEFAULT_RESOURCE_LOCATION = "boavizta/azure_vms.csv";

    @Override
    public void init(Map<String, Object> params) {
        populateCacheFromStaticFile(DEFAULT_RESOURCE_LOCATION);
    }

    @Override
    protected Impacts lookupImpacts(String instanceType) {
        // should have been found in cache already
        return null;
    }
}
