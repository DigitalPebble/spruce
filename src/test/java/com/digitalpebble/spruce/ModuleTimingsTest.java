// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ModuleTimingsTest {

    private static final List<String> TWO_MODULES = List.of("RegionExtraction", "PWUE");

    @Test
    public void testEnrichmentTableShowsPerRowCostAndShareOfTotal() {
        String[] lines = ModuleTimings.format(TWO_MODULES,
                new long[]{250_000_000L, 750_000_000L},
                new long[]{40_000_000L, 700_000_000L},
                new long[]{0L, 0L},
                new long[]{0L, 0L},
                1_000_000L, 8L, 0L).split("\\R");

        assertTrue(lines[0].contains("1,000,000 rows enriched over 8 tasks"), lines[0]);
        assertTrue(lines[1].contains("1,000 ms summed over all tasks"), lines[1]);
        // 250 ms over a million rows is 0.25 us/row and a quarter of the total
        assertTrue(lines[2].contains("250 ms"), lines[2]);
        assertTrue(lines[2].contains("0.25 us/row"), lines[2]);
        assertTrue(lines[2].contains("25.0 %"), lines[2]);
        assertTrue(lines[3].contains("75.0 %"), lines[3]);
        // a module whose total is spread evenly over tasks has a small slowest task
        assertTrue(lines[2].contains("40 ms slowest task"), lines[2]);
        // one whose total is nearly all in a single task has a slowest task close to it
        assertTrue(lines[3].contains("700 ms slowest task"), lines[3]);
    }

    @Test
    public void testConfigurationTableIsPerInitialisationNotPerRow() {
        String[] lines = ModuleTimings.format(TWO_MODULES,
                new long[]{0L, 0L},
                new long[]{0L, 0L},
                new long[]{200_000_000L, 600_000_000L},
                new long[]{25_000_000L, 590_000_000L},
                1_000L, 8L, 8L).split("\\R");

        // 800 ms over 8 initialisations is 100 ms each
        assertTrue(lines[0].contains("8 module initialisations"), lines[0]);
        String header = lines[4];
        assertTrue(header.contains("800 ms summed over 8 initialisations"), header);
        assertTrue(header.contains("100.00 ms each"), header);
        // 200 ms over 8 initialisations is 25 ms each, a quarter of the total
        assertTrue(lines[5].contains("25.00 ms/init"), lines[5]);
        assertTrue(lines[5].contains("25.0 %"), lines[5]);
        assertTrue(lines[6].contains("75.00 ms/init"), lines[6]);
        // 590 of the 600 ms went into one initialisation, so it is paid once not per task
        assertTrue(lines[6].contains("590 ms slowest init"), lines[6]);
        assertTrue(lines[5].contains("25 ms slowest init"), lines[5]);
    }

    @Test
    public void testReportWithoutRowsOrInitialisationsDoesNotDivideByZero() {
        String report = ModuleTimings.format(List.of("PWUE"),
                new long[]{0L}, new long[]{0L}, new long[]{0L}, new long[]{0L}, 0L, 0L, 0L);

        assertTrue(report.contains("0 rows enriched"), report);
        assertTrue(report.contains("0.00 us/row"), report);
        assertTrue(report.contains("0.00 ms/init"), report);
        assertTrue(report.contains("0.00 ms each"), report);
        assertTrue(report.contains("0 ms slowest task"), report);
        assertTrue(report.contains("0 ms slowest init"), report);
    }

    @Test
    public void testOneLinePerModuleInEachTable() {
        String[] lines = ModuleTimings.format(TWO_MODULES,
                new long[]{1L, 1L}, new long[]{1L, 1L},
                new long[]{1L, 1L}, new long[]{1L, 1L}, 1L, 1L, 1L).split("\\R");

        // a header, two modules, a header, two modules
        assertEquals(1 + 1 + 2 + 1 + 2, lines.length);
    }
}
