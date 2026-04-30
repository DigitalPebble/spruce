// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Standalone build-time tool that regenerates
 * {@code src/main/resources/ecologits/coefficients.csv} from the EcoLogits public API.
 * <p>
 * Walks {@code /v1beta/providers} → {@code /v1beta/models/{provider}} → {@code /v1beta/estimations}
 * for every model the API exposes (regardless of whether SPRUCE has a CUR mapping for it
 * yet — the mapping file is curated separately by the operator).
 * <p>
 * Usage: {@code mvn exec:java -Dexec.mainClass=com.digitalpebble.spruce.modules.ecologits.EcoLogitsCoefficientsGenerator}
 * <p>
 * Optional system properties:
 * <ul>
 *   <li>{@code -Decologits.api.base=https://api.ecologits.ai}</li>
 *   <li>{@code -Decologits.output=src/main/resources/ecologits/coefficients.csv}</li>
 * </ul>
 */
public class EcoLogitsCoefficientsGenerator {

    private static final String DEFAULT_BASE_URL = "https://api.ecologits.ai";
    private static final String DEFAULT_OUTPUT = "src/main/resources/ecologits/coefficients.csv";
    private static final String DEFAULT_MAPPING = "src/main/resources/ecologits/mapping.csv";
    private static final int OUTPUT_TOKENS = 1000;
    private static final double FALLBACK_LATENCY = 0.0001;
    private static final MediaType JSON = MediaType.parse("application/json");

    private final String baseUrl;
    private final OkHttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    public EcoLogitsCoefficientsGenerator(String baseUrl, OkHttpClient client) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.client = client;
    }

    public static void main(String[] args) throws IOException {
        String baseUrl = System.getProperty("ecologits.api.base", DEFAULT_BASE_URL);
        String output = System.getProperty("ecologits.output", DEFAULT_OUTPUT);
        String mappingPath = System.getProperty("ecologits.mapping", DEFAULT_MAPPING);

        OkHttpClient client = new OkHttpClient.Builder().build();
        EcoLogitsCoefficientsGenerator generator = new EcoLogitsCoefficientsGenerator(baseUrl, client);

        List<CoefficientRow> rows = generator.generate();
        generator.writeCsv(Paths.get(output), rows);
        System.out.printf("Wrote %d rows to %s%n", rows.size(), output);

        int missing = generator.reportMissingFromMapping(Paths.get(mappingPath), rows);
        if (missing > 0) {
            System.out.printf("%d model(s) above have coefficients but no entry in %s — add them manually if needed.%n",
                    missing, mappingPath);
        }
    }

    /**
     * Walks the API and returns one CoefficientRow per supported model.
     */
    public List<CoefficientRow> generate() throws IOException {
        List<String> providers = listProviders();
        System.out.printf("Discovered %d providers%n", providers.size());

        List<CoefficientRow> rows = new ArrayList<>();
        for (String provider : providers) {
            List<ModelEntry> models = listModels(provider);
            System.out.printf("Provider %s: %d models%n", provider, models.size());
            for (ModelEntry entry : models) {
                if (entry.skip) {
                    System.out.printf("  skip %s/%s (warnings: %s)%n", provider, entry.name, entry.warnings);
                    continue;
                }
                try {
                    CoefficientRow row = estimate(provider, entry);
                    if (row != null) rows.add(row);
                } catch (IOException e) {
                    System.err.printf("  estimate failed for %s/%s: %s%n", provider, entry.name, e.getMessage());
                }
            }
        }

        rows.sort(Comparator.comparing((CoefficientRow r) -> r.provider).thenComparing(r -> r.modelName));
        return rows;
    }

    private List<String> listProviders() throws IOException {
        JsonNode node = getJson(baseUrl + "/v1beta/providers").path("providers");
        List<String> out = new ArrayList<>();
        for (JsonNode n : node) out.add(n.asText());
        return out;
    }

    private List<ModelEntry> listModels(String provider) throws IOException {
        JsonNode node = getJson(baseUrl + "/v1beta/models/" + provider).path("models");
        List<ModelEntry> out = new ArrayList<>();
        for (JsonNode m : node) {
            ModelEntry entry = new ModelEntry();
            entry.name = m.path("name").asText();

            JsonNode deployment = m.path("deployment");
            if (deployment.isObject()) {
                if (deployment.hasNonNull("tps")) entry.tps = deployment.get("tps").asDouble();
                if (deployment.hasNonNull("ttft")) entry.ttft = deployment.get("ttft").asDouble();
            }

            JsonNode warnings = m.path("warnings");
            if (warnings.isArray()) {
                for (JsonNode w : warnings) {
                    entry.warnings.add(w.path("code").asText());
                }
            }
            // Warnings like "model-arch-not-released" just mean lower precision; don't skip.
            out.add(entry);
        }
        return out;
    }

    private CoefficientRow estimate(String provider, ModelEntry entry) throws IOException {
        // request_latency = ttft + output_token_count / tps  → matches the API's natural
        // generation time so it does not stretch GPU runtime past the model's true speed.
        // Falls back to a tiny value (clamped by the API to its internal default) if either
        // tps or ttft is missing.
        double latency;
        if (entry.tps != null && entry.tps > 0 && entry.ttft != null) {
            latency = entry.ttft + (double) OUTPUT_TOKENS / entry.tps;
        } else {
            latency = FALLBACK_LATENCY;
            System.err.printf("  WARN %s/%s missing tps/ttft - falling back to latency=%s%n",
                    provider, entry.name, latency);
        }

        String body = String.format(
                "{\"provider\":\"%s\",\"model_name\":\"%s\",\"output_token_count\":%d,\"request_latency\":%s}",
                provider, escapeJson(entry.name), OUTPUT_TOKENS, latency);

        Request req = new Request.Builder()
                .url(baseUrl + "/v1beta/estimations")
                .post(RequestBody.create(body, JSON))
                .build();

        try (Response resp = client.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                System.err.printf("  HTTP %d for %s/%s%n", resp.code(), provider, entry.name);
                return null;
            }
            JsonNode root = mapper.readTree(resp.body().byteStream());
            JsonNode impacts = root.path("impacts");

            double energy = readMax(impacts.path("energy"));
            double gwpEmbodiedKg = readMax(impacts.path("embodied").path("gwp"));
            double adpeEmbodied = readMax(impacts.path("embodied").path("adpe"));

            CoefficientRow row = new CoefficientRow();
            row.provider = provider;
            row.modelName = entry.name;
            // The API returns totals for OUTPUT_TOKENS (=1000), so the per-1k value
            // is the response value itself.
            row.energyKwhPer1k = energy;
            row.gwpEmbodiedKgPer1k = gwpEmbodiedKg;
            row.adpeEmbodiedPer1k = adpeEmbodied;
            return row;
        }
    }

    private double readMax(JsonNode metric) {
        JsonNode value = metric.path("value");
        if (value.isMissingNode()) return 0.0;
        if (value.has("max")) return value.get("max").asDouble();
        if (value.isNumber()) return value.asDouble();
        return 0.0;
    }

    private JsonNode getJson(String url) throws IOException {
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = client.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                throw new IOException("HTTP " + resp.code() + " from " + url);
            }
            return mapper.readTree(resp.body().byteStream());
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    public void writeCsv(Path path, List<CoefficientRow> rows) throws IOException {
        Files.createDirectories(path.getParent());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(path))) {
            w.println("# Per-1k-output-token impact coefficients sourced from the EcoLogits public API.");
            w.println("# Regenerated by com.digitalpebble.spruce.modules.ecologits.EcoLogitsCoefficientsGenerator");
            w.println("# source_api_version=v1beta");
            w.println("# generated_at=" + Instant.now());
            w.println("provider,model_name,energy_kwh,gwp_embodied_kgco2eq,adpe_embodied_kgsbeq");
            for (CoefficientRow r : rows) {
                w.printf("%s,%s,%s,%s,%s%n",
                        r.provider, r.modelName,
                        formatScientific(r.energyKwhPer1k),
                        formatScientific(r.gwpEmbodiedKgPer1k),
                        formatScientific(r.adpeEmbodiedPer1k));
            }
        }
    }

    /**
     * Reads the existing mapping file and prints to stdout any (provider, model_name) pairs
     * from the coefficients that have no mapping entry yet.
     *
     * @return the number of unmapped models found
     */
    public int reportMissingFromMapping(Path mappingPath, List<CoefficientRow> coefficients) throws IOException {
        Set<String> mapped = new HashSet<>();
        if (Files.exists(mappingPath)) {
            for (String line : Files.readAllLines(mappingPath)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
                String[] parts = trimmed.split(",", -1);
                if (parts.length >= 3) {
                    String provider = parts[1].trim();
                    String model = parts[2].trim();
                    if (!provider.isEmpty() && !model.isEmpty()) {
                        mapped.add(provider + "/" + model);
                    }
                }
            }
        }

        int count = 0;
        for (CoefficientRow row : coefficients) {
            if (!mapped.contains(row.provider + "/" + row.modelName)) {
                System.out.printf("NOT MAPPED: %s/%s%n",
                        row.provider, row.modelName);
                count++;
            }
        }
        return count;
    }

    private static String formatScientific(double v) {
        if (v == 0.0) return "0";
        return String.format("%.6e", v);
    }

    public static class ModelEntry {
        String name;
        Double tps;
        Double ttft;
        boolean skip;
        final List<String> warnings = new ArrayList<>();
    }

    public static class CoefficientRow {
        public String provider;
        public String modelName;
        public double energyKwhPer1k;
        public double gwpEmbodiedKgPer1k;
        public double adpeEmbodiedPer1k;
    }
}
