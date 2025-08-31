// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractBoaviztaTest {

    private static final String BOAVIZTAPI_VERSION = "1.3.11";

    // Static container instance - shared across all test classes
    private static GenericContainer boaviztapiContainer;

    @BeforeAll
    static void startContainer() {
        if (boaviztapiContainer == null) {
            boaviztapiContainer = new GenericContainer(
                DockerImageName.parse("ghcr.io/boavizta/boaviztapi:" + BOAVIZTAPI_VERSION))
                .withExposedPorts(5000);
            
            boaviztapiContainer.start();
            
            // Wait for container to be ready
            boaviztapiContainer.waitingFor(
                org.testcontainers.containers.wait.strategy.Wait.forLogMessage(".*Application startup complete.*", 1)
            );
        }
    }

    @AfterAll
    static void stopContainer() {
        if (boaviztapiContainer != null) {
            boaviztapiContainer.stop();
            boaviztapiContainer = null;
        }
    }

    // Getter method for test classes that need the container
    protected static GenericContainer getContainer() {
        return boaviztapiContainer;
    }

    // Get the container's host and port for test configuration
    protected static String getContainerHost() {
        return boaviztapiContainer.getHost();
    }

    protected static int getContainerPort() {
        return boaviztapiContainer.getMappedPort(5000);
    }

    // Get the full URL for the container
    protected static String getContainerUrl() {
        return "http://" + getContainerHost() + ":" + getContainerPort();
    }
}