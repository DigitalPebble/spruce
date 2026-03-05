// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WaterStatsTest {

    // --- WCF tests ---

    @Test
    void wcfForKnownZone() {
        assertEquals(5.72, WaterStats.getWCF("AT"), 0.01);
    }

    @Test
    void wcfNullForZoneWithoutValue() {
        assertNull(WaterStats.getWCF("AE"));
    }

    @Test
    void wcfNullForUnknownZone() {
        assertNull(WaterStats.getWCF("UNKNOWN"));
    }

    // --- Water stress tests (country-level zones) ---

    @Test
    void waterStressForCountryZone() {
        assertEquals(4, WaterStats.getWaterStressCategory("AE"));
    }

    @Test
    void waterStressLowForSwitzerland() {
        assertEquals(0, WaterStats.getWaterStressCategory("CH"));
    }

    // --- Water stress tests (regional sub-zones) ---

    @Test
    void waterStressForAustralianState() {
        // AU-NSW = New South Wales = cat 3
        assertEquals(3, WaterStats.getWaterStressCategory("AU-NSW"));
    }

    @Test
    void waterStressForUSState() {
        // US-CAL-CISO = California = cat 4
        assertEquals(4, WaterStats.getWaterStressCategory("US-CAL-CISO"));
    }

    @Test
    void waterStressForUSFlorida() {
        // US-FLA-FPL = Florida = cat 3
        assertEquals(3, WaterStats.getWaterStressCategory("US-FLA-FPL"));
    }

    @Test
    void waterStressForIndianGrid() {
        // IN-NO = Northern India (NCT of Delhi) = cat 4
        assertEquals(4, WaterStats.getWaterStressCategory("IN-NO"));
    }

    @Test
    void waterStressForJapanTokyo() {
        // JP-TK = Tokyo = cat 2
        assertEquals(2, WaterStats.getWaterStressCategory("JP-TK"));
    }

    // --- Water stress tests (territories) ---

    @Test
    void waterStressForHongKong() {
        // HK -> China = cat 2
        assertEquals(2, WaterStats.getWaterStressCategory("HK"));
    }

    // --- No data / unknown ---

    @Test
    void waterStressNullForNoDataZone() {
        // SG has no water stress data in Aqueduct (-9999)
        assertNull(WaterStats.getWaterStressCategory("SG"));
    }

    @Test
    void waterStressNullForUnknownZone() {
        assertNull(WaterStats.getWaterStressCategory("UNKNOWN"));
    }
}
