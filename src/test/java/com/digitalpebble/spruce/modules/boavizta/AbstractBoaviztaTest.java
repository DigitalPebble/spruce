// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractBoaviztaTest {

    private static final String BOAVIZTAPI_VERSION = "1.3.11";

    protected GenericContainer boaviztapiContainer =
            new GenericContainer(
                            DockerImageName.parse(
                                    "ghcr.io/boavizta/boaviztapi:"+ BOAVIZTAPI_VERSION))
                    .withExposedPorts(5000);

    @BeforeEach
    void init() {
        boaviztapiContainer.start();
    }

    @AfterEach
    void close() {
        boaviztapiContainer.close();
    }

}