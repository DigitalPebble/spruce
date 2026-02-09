// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.electricitymaps;

import com.digitalpebble.spruce.Provider;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AverageCarbonIntensityTest {

    @Test
    void testGetAverageIntensity() {
        AverageCarbonIntensity aci = new AverageCarbonIntensity();
        aci.init(Map.of());
        String regionId = "us-east-1";
        Double result = aci.getAverageIntensity(Provider.AWS, regionId);
        assertEquals(400.33, result);
    }

    @Test
    void testUnknownRegion() {
        AverageCarbonIntensity aci = new AverageCarbonIntensity();
        aci.init(Map.of());
        String regionId = "us-blablabla-1";
        Double result = aci.getAverageIntensity(Provider.AWS, regionId);
        assertNull(result);
    }
}
