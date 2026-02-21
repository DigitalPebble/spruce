// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.realtimecloud;

import com.digitalpebble.spruce.Provider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RegionMappingsTest {

    @Test
    void testGetEMRegion_ValidMapping() {
        // Replace with actual values from your region-mappings.csv
        String emRegion = RegionMappings.getEMRegion(Provider.AWS, "us-east-1");
        assertNotNull(emRegion);
        assertEquals("US-MIDA-PJM", emRegion);
    }

    @Test
    void testGetEMRegion_UnsupportedRegion() {
        assertNull(RegionMappings.getEMRegion(Provider.AWS, "non-existent-region"));
    }
}