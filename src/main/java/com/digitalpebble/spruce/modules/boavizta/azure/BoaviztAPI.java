// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.azure;

import com.digitalpebble.spruce.modules.boavizta.BoaviztAPIClient;
import com.digitalpebble.spruce.modules.boavizta.Impacts;
import com.digitalpebble.spruce.modules.boavizta.InstanceTypeUknown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Azure-specific Boavizta module backed by a live BoaviztAPI instance. The extraction lives in
 * {@link AbstractBoaviztaAzure}; this class only provides the call to the API.
 *
 * <p>Launch the API locally with:
 * <pre>docker run -p 5000:5000 ghcr.io/boavizta/boaviztapi:latest</pre>
 */
public class BoaviztAPI extends AbstractBoaviztaAzure {

    private static final Logger LOG = LoggerFactory.getLogger(BoaviztAPI.class);

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
        if (client == null) {
            client = new BoaviztAPIClient(address);
        }
        try {
            return client.getImpacts(provider, instanceType);
        } catch (InstanceTypeUknown e) {
            return null;
        } catch (IOException e) {
            LOG.error("Exception caught when retrieving estimates for {}", instanceType, e);
            return null;
        }
    }
}
