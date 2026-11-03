// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers the timing added to {@link Config#configureModules(long[])}. */
public class ConfigInitTimingTest {

    /** Module whose init() takes a measurable amount of time, as the resource loading ones do. */
    public static class SlowInit implements EnrichmentModule {
        @Override
        public void init(Map<String, Object> params) {
            long until = System.nanoTime() + 200_000;
            while (System.nanoTime() < until) {
                // spin
            }
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

    private static Config twoSlowModules() throws Exception {
        String json = """
                {"modules": [
                  {"className": "com.digitalpebble.spruce.ConfigInitTimingTest$SlowInit"},
                  {"className": "com.digitalpebble.spruce.ConfigInitTimingTest$SlowInit"}
                ]}
                """;
        Path file = Files.createTempFile("spruce-config", ".json");
        try {
            Files.writeString(file, json);
            return Config.fromJsonFile(file, Provider.AWS);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testRecordsTimePerModule() throws Exception {
        Config config = twoSlowModules();
        long[] initNanos = new long[2];

        List<EnrichmentModule> modules = config.configureModules(initNanos);

        assertEquals(2, modules.size());
        assertTrue(initNanos[0] > 0, "first module's init should be timed");
        assertTrue(initNanos[1] > 0, "second module's init should be timed");
    }

    @Test
    public void testNullArraySkipsTheMeasurement() throws Exception {
        Config config = twoSlowModules();
        assertEquals(2, config.configureModules(null).size());
        // the no-argument overload is the same thing, and is what a run without -m uses
        assertEquals(2, twoSlowModules().configureModules().size());
    }

    @Test
    public void testWrongSizedArrayIsRejected() throws Exception {
        Config config = twoSlowModules();
        assertThrows(IllegalArgumentException.class, () -> config.configureModules(new long[1]));
    }
}
