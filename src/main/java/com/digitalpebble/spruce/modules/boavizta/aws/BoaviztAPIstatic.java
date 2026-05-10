// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

import com.digitalpebble.spruce.modules.boavizta.Impacts;

import java.util.Map;

/**
 * AWS-specific Boavizta module backed by a bundled static CSV, so SPRUCE can run without a live
 * BoaviztAPI instance. The CUR extraction lives in {@link AbstractBoaviztaAws}; this class only
 * provides the lookup.
 */
public class BoaviztAPIstatic extends AbstractBoaviztaAws {

    private static final String DEFAULT_RESOURCE_LOCATION = "boavizta/instanceTypes.csv";

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
