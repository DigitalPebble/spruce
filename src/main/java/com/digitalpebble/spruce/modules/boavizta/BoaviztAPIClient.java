// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Provider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.NonNull;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Map;

/** Get energy consumption estimates from the BoaviztAPI **/
public class BoaviztAPIClient {

    private static final String URLPattern = "%s/v1/cloud/instance?provider=aws&instance_type=%s&verbose=false&duration=1&criteria=pe";
    private static final double MJtoKWh = 0.277778;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final OkHttpClient client = new OkHttpClient();

    private final String host;

    public BoaviztAPIClient(@NonNull String host) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null, empty, or whitespace only");
        }
        this.host = host;
    }

    public double[] getEnergyEstimates(@NonNull Provider p, @NonNull String instanceType) throws IOException {
        if (p == null) {
            throw new IllegalArgumentException("Provider cannot be null");
        }
        if (instanceType == null || instanceType.trim().isEmpty()) {
            throw new IllegalArgumentException("Instance type cannot be null, empty, or whitespace only");
        }

        final String url = String.format(URLPattern, host, instanceType);

        Request request = new Request.Builder()
                .url(url)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                if (response.message().equals("Not Found")) {
                    throw new InstanceTypeUknown(instanceType);
                }
                throw new IOException("Unexpected code " + response.code());
            }

            Map<String, Object> result = objectMapper.readValue(response.body().byteStream(), new TypeReference<Map<String, Object>>() {
            });
            result = (Map<String, Object>) result.get("impacts");
            result = (Map<String, Object>) result.get("pe");
            Map<String, Object> use = (Map<String, Object>) result.get("use");
            Double use_value_mj = (Double) use.get("value");

            Map<String, Object> embedded = (Map<String, Object>) result.get("embedded");
            Double embedded_value_mj = (Double) embedded.get("value");

            return new double[]{use_value_mj * MJtoKWh, (embedded_value_mj * MJtoKWh)};
        }
    }
}
