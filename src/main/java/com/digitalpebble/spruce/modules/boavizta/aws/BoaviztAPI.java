// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

import com.digitalpebble.spruce.modules.boavizta.BoaviztAPIClient;
import com.digitalpebble.spruce.modules.boavizta.Impacts;
import com.digitalpebble.spruce.modules.boavizta.InstanceTypeUknown;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * AWS-specific Boavizta module backed by a live BoaviztAPI instance. The CUR extraction lives in
 * {@link AbstractBoaviztaAws}; this class only provides the lookup, with a small Caffeine cache
 * to avoid repeated round-trips for the same instance type.
 *
 * <p>Launch the API locally with:
 * <pre>docker run -p 5000:5000 ghcr.io/boavizta/boaviztapi:latest</pre>
 */
public class BoaviztAPI extends AbstractBoaviztaAws {

    private static final Logger LOG = LoggerFactory.getLogger(BoaviztAPI.class);

    private static Cache<String, @Nullable Impacts> cache;

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
    protected Impacts lookupImpacts(String instanceType) {
        if (cache == null) {
            cache = Caffeine.newBuilder().maximumSize(100).build();
        }
        if (client == null) {
            client = new BoaviztAPIClient(address);
        }

        Impacts impacts = cache.getIfPresent(instanceType);
        if (impacts != null) {
            return impacts;
        }

        try {
            impacts = client.getImpacts(provider, instanceType);
            cache.put(instanceType, impacts);
            return impacts;
        } catch (InstanceTypeUknown e) {
            return null;
        } catch (IOException e) {
            LOG.error("Exception caught when retrieving estimates for {}", instanceType, e);
            return null;
        }
    }
}
