// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;
import java.util.Map;

/** List of modules and their configuration defined as JSON **/
public class Config {

    private final List<com.digitalpebble.spruce.EnrichmentModule> enrichmentModules = new java.util.ArrayList<>();
    private final List<Map<String, Object>> configs = new java.util.ArrayList<>();

    /**  Returns the list of enrichment modules defined in the configuration **/
    public List<com.digitalpebble.spruce.EnrichmentModule> getModules() {
        return enrichmentModules;
    }

    /**
     * Initializes each enrichment module with its corresponding configuration.
     * Iterates through the list of enrichment modules and calls their init method
     * with the associated configuration map.
     */
    public void configureModules() {
        for (int i = 0; i < enrichmentModules.size(); i++) {
            enrichmentModules.get(i).init(configs.get(i));
        }
    }

    /**
     * Loads a Config instance from a JSON file
     */
    public static Config fromJsonFile(java.nio.file.Path path) throws java.io.IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        // Read the JSON as a List of Maps
        Map<String, Object> startNode = mapper.readValue(
                java.nio.file.Files.newBufferedReader(path),
                new TypeReference<Map<String, Object>>() {
                }
        );

        List<Map<String, Object>> modulesList = (List<Map<String, Object>>) startNode.get("modules");

        Config conf = new Config();

        for (Map<String, Object> moduleMap : modulesList) {
            String className = (String) moduleMap.get("className");
            Map<String, Object> config = (Map<String, Object>) moduleMap.get("config");
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

