// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** List of modules and their configuration defined as JSON **/
public class Config implements Serializable {

    private final List<com.digitalpebble.spruce.EnrichmentModule> enrichmentModules = new java.util.ArrayList<>();
    private final List<Map<String, Object>> configs = new java.util.ArrayList<>();
    /** Must be set before {@link #configureModules()} is called — null on purpose so we
     *  fail fast rather than silently defaulting to AWS for non-AWS workflows. */
    private Provider provider;

    /**  Returns the list of enrichment modules defined in the configuration **/
    public List<com.digitalpebble.spruce.EnrichmentModule> getModules() {
        return enrichmentModules;
    }

    /** Returns the cloud provider this configuration applies to, or null if not set. */
    public Provider getProvider() {
        return provider;
    }

    /** Sets the cloud provider. Required before {@link #configureModules()}. */
    public void setProvider(Provider provider) {
        this.provider = provider;
    }

    /**
     * Initializes each enrichment module with its corresponding configuration.
     * Iterates through the list of enrichment modules and calls their init method
     * with the associated configuration map and the active provider. Return the modules.
     *
     * @throws IllegalStateException if the provider has not been set.
     */
    public List<com.digitalpebble.spruce.EnrichmentModule> configureModules() {
        if (provider == null) {
            throw new IllegalStateException("Provider must be set before configureModules()");
        }
        for (int i = 0; i < enrichmentModules.size(); i++) {
            enrichmentModules.get(i).init(configs.get(i), provider);
        }
        return enrichmentModules;
    }

    public static Config loadDefault() throws java.io.IOException {
        return loadDefault(Provider.AWS);
    }

    /**
     * Loads the default config bundled for the given provider. The resource file is resolved
     * as {@code default-config-<provider>.json} where {@code <provider>} is the lowercased
     * enum name (e.g. {@code default-config-aws.json}).
     */
    public static Config loadDefault(Provider provider) throws java.io.IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        final String resourceFileName = "default-config-" + provider.name().toLowerCase() + ".json";
        try (InputStream inputStream = Config.class
                .getClassLoader()
                .getResourceAsStream(resourceFileName)) {

            if (inputStream == null) {
                throw new IOException("Resource file not found: " + resourceFileName);
            }

            Map<String, Object> startNode = objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
            Config conf = process(startNode);
            conf.provider = provider;
            return conf;
        }
    }



    /** Loads a Config instance from a JSON file, defaulting to {@link Provider#AWS}. */
    public static Config fromJsonFile(java.nio.file.Path path) throws java.io.IOException {
        return fromJsonFile(path, Provider.AWS);
    }

    /**
     * Loads a Config instance from a JSON file and tags it with the given provider so that
     * provider-aware modules (e.g. Water, AverageCarbonIntensity) can pick the correct
     * region-keyed lookups.
     */
    public static Config fromJsonFile(java.nio.file.Path path, Provider provider) throws java.io.IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        try (var reader = java.nio.file.Files.newBufferedReader(path)) {
            Map<String, Object> startNode = mapper.readValue(
                    reader,
                    new TypeReference<Map<String, Object>>() {
                    }
            );
            Config conf = process(startNode);
            conf.provider = provider;
            return conf;
        }
    }

    private static Config process(Map<String, Object> startNode){
        List<Map<String, Object>> modulesList = (List<Map<String, Object>>) startNode.get("modules");

        Config conf = new Config();

        for (Map<String, Object> moduleMap : modulesList) {
            String className = (String) moduleMap.get("className");
            Map<String, Object> config = (Map<String, Object>) moduleMap.getOrDefault("config", java.util.Collections.emptyMap());
            try {
                Class<?> clazz = Class.forName(className);
                if (!com.digitalpebble.spruce.EnrichmentModule.class.isAssignableFrom(clazz)) {
                    throw new IllegalArgumentException("Class " + className + " is not an instance of EnrichmentModule");
                }
                com.digitalpebble.spruce.EnrichmentModule instance =
                        (com.digitalpebble.spruce.EnrichmentModule) clazz.getDeclaredConstructor().newInstance();
                conf.enrichmentModules.add(instance);
                conf.configs.add(config);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Class not found: " + className, e);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Failed to instantiate: " + className, e);
            }
        }

        return conf;
    }
}

