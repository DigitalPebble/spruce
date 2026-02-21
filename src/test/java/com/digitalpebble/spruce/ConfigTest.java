// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;


import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ConfigTest {

    static class DummyModule implements EnrichmentModule {
        boolean inited = false;
        Map<String, Object> configPassed = null;

        @Override
        public void init(Map<String, Object> config) {
            inited = true;
            configPassed = config;
        }

        @Override
        public Column[] columnsNeeded() {
            return new Column[0];
        }

        @Override
        public Column[] columnsAdded() {
            return new Column[0];
        }

        @Override
        public void enrich(Row row, Map<Column, Object> enrichedValues) {
        }
    }

    @Test
    void testGetModulesReturnsAddedModules() {
        Config config = new Config();
        DummyModule module = new DummyModule();
        config.getModules().add(module);
        assertTrue(config.getModules().contains(module));
    }

    @Test
    void testConfigureModulesCallsInit() {
        Config config = new Config();
        DummyModule module = new DummyModule();
        config.getModules().add(module);

        // Use reflection to add config to private field
        try {
            var configsField = Config.class.getDeclaredField("configs");
            configsField.setAccessible(true);
            List<Map<String, Object>> configs = (List<Map<String, Object>>) configsField.get(config);
            Map<String, Object> dummyConfig = new HashMap<>();
            dummyConfig.put("foo", "bar");
            configs.add(dummyConfig);
        } catch (Exception e) {
            fail(e);
        }

        config.configureModules();
        assertTrue(module.inited);
        assertEquals("bar", module.configPassed.get("foo"));
    }

    @Test
    void testFromJsonFileLoadsModules() throws Exception {
        String json = """
        {
          "modules": [
            {
              "className": "com.digitalpebble.spruce.ConfigTest$DummyModule",
              "config": { "key": "value" }
            }
          ]
        }
        """;
        Path tempFile = Files.createTempFile("config", ".json");
        Files.writeString(tempFile, json);

        Config conf = Config.fromJsonFile(tempFile);
        assertEquals(1, conf.getModules().size());
        assertTrue(conf.getModules().get(0) instanceof DummyModule);

        // Clean up
        Files.deleteIfExists(tempFile);
    }

    @Test
    void testFromJsonFileThrowsOnInvalidClass() throws Exception {
        String json = """
        {
          "modules": [
            {
              "className": "java.lang.String",
              "config": {}
            }
          ]
        }
        """;
        Path tempFile = Files.createTempFile("config", ".json");
        Files.writeString(tempFile, json);

        Exception ex = assertThrows(IllegalArgumentException.class, () -> {
            Config.fromJsonFile(tempFile);
        });
        assertTrue(ex.getMessage().contains("is not an instance of EnrichmentModule"));

        Files.deleteIfExists(tempFile);
    }

    @Test
        void testLoadDefaultConfig() throws Exception {
            // Assumes default-config.json is present in test resources and valid
            Config conf = Config.loadDefault();
            assertNotNull(conf);
            assertNotNull(conf.getModules());
            assertFalse(conf.getModules().isEmpty());
        }
}