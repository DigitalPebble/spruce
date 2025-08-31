// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Example test class demonstrating how to use the refactored AbstractBoaviztaTest
 * with a static container that starts once for all tests.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractBoaviztaTestExample extends AbstractBoaviztaTest {

    @Test
    void testContainerIsRunning() {
        // Verify the container is accessible
        assertNotNull(getContainer());
        assertTrue(getContainer().isRunning());
        
        // Get container details
        String host = getContainerHost();
        int port = getContainerPort();
        String url = getContainerUrl();
        
        System.out.println("Container running at: " + url);
        System.out.println("Host: " + host + ", Port: " + port);
    }

    @Test
    void testContainerReuse() {
        // This test should use the same container instance
        assertNotNull(getContainer());
        assertTrue(getContainer().isRunning());
        
        // Both tests should see the same container
        System.out.println("Container ID: " + getContainer().getContainerId());
    }
}
