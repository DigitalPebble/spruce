// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class UtilsVersionTest {

    @Test
    void versionIsSetByMaven() {
        String version = Utils.getSpruceVersion();
        assertNotNull(version);
        assertNotEquals("unknown", version);
        // the placeholder must have been replaced by resource filtering
        assertFalse(version.contains("${"), version);
    }
}
