// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Provider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.util.Map;


/**
 * Client for interacting with the BoaviztAPI to retrieve environmental impacts
 * estimates for cloud instances.
 */
public class BoaviztAPIClient {

    private static final String URLPattern = "%s/v1/cloud/instance?provider=aws&instance_type=%s&verbose=false&duration=1&criteria=pe&criteria=gwp&criteria=adp";
    // Conversion factor from megajoules (MJ) to kilowatt-hours (kWh)
    private static final double MJtoKWh = 0.277778;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final OkHttpClient client = new OkHttpClient();

    private final String address;

    // The default location in BoaviztAPI is EEE
    // https://github.com/Boavizta/boaviztapi/blob/13a87c74603b79859f644f6704605fc3cfdfe0cd/boaviztapi/data/factors.yml#L3428C16-L3428C22
    private final double PE_FACTOR_DEFAULT_LOCATION = 12.873d;

    /**
     * Constructs a new BoaviztAPIClient.
     *
     * @param address The base address of the BoaviztAPI. Must not be null, empty, or whitespace only.
     * @throws IllegalArgumentException if the address is null, empty, or contains only whitespace.
     */
    public BoaviztAPIClient(@NonNull String address) {
        if (address == null || address.trim().isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null, empty, or whitespace only");
        }
        this.address = address;
    }

    public Impacts getImpacts(@NonNull Provider p, @NonNull String instanceType) throws IOException {
        if (p == null) {
            throw new IllegalArgumentException("Provider cannot be null");
        }
        if (instanceType == null || instanceType.trim().isEmpty()) {
            throw new IllegalArgumentException("Instance type cannot be null, empty, or whitespace only");
        }

        final String url = String.format(URLPattern, address, instanceType);

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

            Double use_value_mj = extractValue(result, "pe", true);
            final double final_energy_kwh = use_value_mj * MJtoKWh / PE_FACTOR_DEFAULT_LOCATION;

            Double embedded_value_kgCO2eq = extractValue(result, "gwp", false);
            Double embedded_adp_value_kgCO2eq = extractValue(result, "adp", false);

            return new Impacts(final_energy_kwh, embedded_value_kgCO2eq * 1000, embedded_adp_value_kgCO2eq * 1000);
        }
    }

    private static double extractValue(Map<String, Object> impacts, String impactName, boolean use) {
        Map<String, Object> temp = (Map<String, Object>) impacts.get(impactName);
        String label = use ? "use" : "embedded";
        temp = (Map<String, Object>) temp.get(label);
        return (Double) temp.get("value");
    }
}

class Impacts {
    private final double finalEnergyKWh;
    private final double embeddedEmissionsGramsCO2eq;
    private final double abioticDepletionPotentialGramsSbeq;

    public Impacts(double finalEnergyKWh, double embeddedEmissionsGramsCO2eq, double abioticDepletionPotentialKgSbeq) {
        this.finalEnergyKWh = finalEnergyKWh;
        this.embeddedEmissionsGramsCO2eq = embeddedEmissionsGramsCO2eq;
        this.abioticDepletionPotentialGramsSbeq = abioticDepletionPotentialKgSbeq;
    }

    public double getFinalEnergyKWh() {
        return finalEnergyKWh;
    }

    public double getEmbeddedEmissionsGramsCO2eq() {
        return embeddedEmissionsGramsCO2eq;
    }

    public double getAbioticDepletionPotentialGramsSbeq() {
        return abioticDepletionPotentialGramsSbeq;
    }
}