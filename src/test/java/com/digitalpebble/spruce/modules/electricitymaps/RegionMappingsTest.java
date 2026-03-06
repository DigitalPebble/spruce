// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.electricitymaps;

import com.digitalpebble.spruce.Provider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RegionMappingsTest {

    @Test
    void awsRegionReturnsCorrectZoneKey() {
        assertEquals("US-MIDA-PJM", RegionMappings.getEMRegion(Provider.AWS, "us-east-1"));
    }

    @Test
    void gcpRegionReturnsCorrectZoneKey() {
        assertEquals("TW", RegionMappings.getEMRegion(Provider.GOOGLE, "asia-east1"));
    }

    @Test
    void azureRegionReturnsCorrectZoneKey() {
        assertEquals("AU-NSW", RegionMappings.getEMRegion(Provider.AZURE, "australiacentral"));
    }

    @Test
    void unknownRegionReturnsNull() {
        assertNull(RegionMappings.getEMRegion(Provider.AWS, "non-existent-region"));
    }

    @Test
    void regionDoesNotResolveUnderWrongProvider() {
        // asia-east1 is a valid GCP region but should not resolve under AWS
        assertNull(RegionMappings.getEMRegion(Provider.AWS, "asia-east1"));
    }
}