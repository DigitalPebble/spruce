package com.digitalpebble.spruce;

import java.util.List;
import java.util.Map;

public class Config {

    private List<com.digitalpebble.spruce.EnrichmentModule> enrichmentModules = new java.util.ArrayList<>();
    private List<Map<String, Object>> configs = new java.util.ArrayList<>();

    public List<com.digitalpebble.spruce.EnrichmentModule> getModules(){
        return enrichmentModules;
    }

    public  void configureModules() {
        for (int i = 0; i < enrichmentModules.size(); i++) {
            enrichmentModules.get(i).init(configs.get(i));
        }
    }

    public static Config fromJsonFile(java.nio.file.Path path) throws java.io.IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        // Read the JSON as a List of Maps
        List<Map<String, Object>> modulesList = mapper.readValue(
                java.nio.file.Files.newBufferedReader(path),
                new com.fasterxml.jackson.core.type.TypeReference<List<Map<String, Object>>>() {}
        );

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

