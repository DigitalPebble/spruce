/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara.modules.electricitymaps;

import com.digitalpebble.carbonara.Provider;
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
        Exception exception = assertThrows(RuntimeException.class, () ->
                RegionMappings.getEMRegion(Provider.AWS, "non-existent-region"));
        assertTrue(exception.getMessage().contains("Unsupported region"));
    }



}