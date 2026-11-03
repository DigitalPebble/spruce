// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ModuleTimingsTest {

    @Test
    public void testReportShowsMillisPerModuleAndShareOfTotal() {
        String report = ModuleTimings.format(
                List.of("RegionExtraction", "PUE"),
                new long[]{250_000_000L, 750_000_000L},
                1_000_000L);

        String[] lines = report.split("\\R");
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("1,000,000 rows enriched"), lines[0]);
        assertTrue(lines[0].contains("1,000 ms"), lines[0]);
        // 250 ms over a million rows is 0.25 us/row and a quarter of the total
        assertTrue(lines[1].contains("250 ms"), lines[1]);
        assertTrue(lines[1].contains("0.25 us/row"), lines[1]);
        assertTrue(lines[1].contains("25.0 %"), lines[1]);
        assertTrue(lines[2].contains("75.0 %"), lines[2]);
    }

    @Test
    public void testReportWithoutRowsDoesNotDivideByZero() {
        String report = ModuleTimings.format(List.of("PUE"), new long[]{0L}, 0L);

        assertTrue(report.contains("0 rows enriched"), report);
        assertTrue(report.contains("0.00 us/row"), report);
        assertTrue(report.contains("0.0 %"), report);
    }
}
