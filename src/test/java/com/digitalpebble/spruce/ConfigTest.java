// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
        Provider providerPassed = null;

        @Override
        public void init(Map<String, Object> config) {
            inited = true;
            configPassed = config;
        }

        @Override
        public void init(Map<String, Object> config, Provider provider) {
            providerPassed = provider;
            init(config);
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
        config.setProvider(Provider.AWS);
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
    void testConfigureModulesPropagatesProvider() {
        Config config = new Config();
        config.setProvider(Provider.AZURE);
        DummyModule module = new DummyModule();
        config.getModules().add(module);

        try {
            var configsField = Config.class.getDeclaredField("configs");
            configsField.setAccessible(true);
            List<Map<String, Object>> configs = (List<Map<String, Object>>) configsField.get(config);
            configs.add(new HashMap<>());
        } catch (Exception e) {
            fail(e);
        }

        config.configureModules();
        assertEquals(Provider.AZURE, module.providerPassed);
    }

    @Test
    void testConfigureModulesFailsWhenProviderUnset() {
        Config config = new Config();
        config.getModules().add(new DummyModule());

        try {
            var configsField = Config.class.getDeclaredField("configs");
            configsField.setAccessible(true);
            List<Map<String, Object>> configs = (List<Map<String, Object>>) configsField.get(config);
            configs.add(new HashMap<>());
        } catch (Exception e) {
            fail(e);
        }

        IllegalStateException ex = assertThrows(IllegalStateException.class, config::configureModules);
        assertTrue(ex.getMessage().contains("Provider"));
    }

    @Test
    void testFromJsonFileTagsProvider() throws Exception {
        String json = """
                {
                  "modules": [
                    { "className": "com.digitalpebble.spruce.ConfigTest$DummyModule" }
                  ]
                }
                """;
        Path tempFile = Files.createTempFile("config", ".json");
        Files.writeString(tempFile, json);

        Config conf = Config.fromJsonFile(tempFile, Provider.AZURE);
        assertEquals(Provider.AZURE, conf.getProvider());

        Files.deleteIfExists(tempFile);
    }

    @Test
    void testLoadDefaultTagsProvider() throws Exception {
        Config conf = Config.loadDefault(Provider.AWS);
        assertEquals(Provider.AWS, conf.getProvider());
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
        Config conf = Config.loadDefault();
        assertNotNull(conf);
        assertNotNull(conf.getModules());
        assertFalse(conf.getModules().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"AWS", "AZURE"})
    void testLoadDefaultConfigForProvider(String providerName) throws Exception {
        Provider provider = Provider.valueOf(providerName);
        Config conf = Config.loadDefault(provider);
        assertNotNull(conf);
        assertNotNull(conf.getModules());
        assertFalse(conf.getModules().isEmpty());
    }

    @Test
    void testAzureDefaultConfigIncludesStorage() throws Exception {
        Config conf = Config.loadDefault(Provider.AZURE);
        assertTrue(conf.getModules().stream()
                .anyMatch(module -> module instanceof com.digitalpebble.spruce.modules.ccf.azure.Storage));
    }

    @Test
    void testLoadDefaultTagsReportFormat() throws Exception {
        Config conf = Config.loadDefault(Provider.AZURE);
        assertEquals(ReportFormat.NATIVE, conf.getReportFormat());

        conf = Config.loadDefault(Provider.AZURE, ReportFormat.FOCUS);
        assertEquals(ReportFormat.FOCUS, conf.getReportFormat());
        assertEquals(Provider.AZURE, conf.getProvider());
        assertFalse(conf.getModules().isEmpty());
    }

    @Test
    void testAzureFocusDefaultConfigBindsFocusColumns() throws Exception {
        Config conf = Config.loadDefault(Provider.AZURE, ReportFormat.FOCUS);
        assertTrue(conf.getModules().stream()
                .anyMatch(module -> module instanceof com.digitalpebble.spruce.modules.focus.RegionExtraction));
        // the FOCUS input already carries the FinOps columns, no bridging module needed
        assertFalse(conf.getModules().stream()
                .anyMatch(module -> module instanceof com.digitalpebble.spruce.modules.azure.FOCUSColumns));
        // loading the config binds the format-dependent input columns before validation
        var storage = conf.getModules().stream()
                .filter(module -> module instanceof com.digitalpebble.spruce.modules.ccf.azure.Storage)
                .findFirst().orElseThrow();
        assertTrue(java.util.Arrays.asList(storage.columnsNeeded())
                .contains(AzureFOCUSColumn.X_SKU_METER_CATEGORY));
    }
}
