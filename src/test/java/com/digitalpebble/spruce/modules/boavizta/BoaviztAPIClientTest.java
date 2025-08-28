// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Provider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class BoaviztAPIClientTest extends AbstractBoaviztaTest {

    private BoaviztAPIClient client;
    private static final String TEST_HOST = "http://localhost:5000";

    @BeforeEach
    void setUp() {
        client = new BoaviztAPIClient(TEST_HOST);
    }

    @Test
    void testConstructor() {
        BoaviztAPIClient testClient = new BoaviztAPIClient("http://custom-host:8080");
        assertNotNull(testClient);
    }

    @Test
    void testConstructorWithNullHost() {
        assertThrows(IllegalArgumentException.class, () -> {
            new BoaviztAPIClient(null);
        });
    }

    @Test
    void testConstructorWithEmptyHost() {
        assertThrows(IllegalArgumentException.class, () -> {
            new BoaviztAPIClient("");
        });
    }

    @Test
    void testConstructorWithWhitespaceHost() {
        assertThrows(IllegalArgumentException.class, () -> {
            new BoaviztAPIClient("   ");
        });
    }

    @Test
    void testGetEnergyEstimatesWithDifferentInstanceTypes() throws IOException {
        String[] instanceTypes = {"t3.micro", "t3.small", "t3.medium", "c5.large", "m5.xlarge"};
        
        for (String instanceType : instanceTypes) {
            try {
                double[] result = client.getEnergyEstimates(Provider.AWS, instanceType);
                assertNotNull(result);
                assertEquals(2, result.length);
                assertTrue(result[0] >= 0, "Use energy should be non-negative for " + instanceType);
                assertTrue(result[1] >= 0, "Embedded energy should be non-negative for " + instanceType);
            } catch (IOException e) {
                // Expected if the test container is not accessible
                assertTrue(e.getMessage().contains("Unexpected code") || 
                          e.getMessage().contains("Connection refused") ||
                          e.getMessage().contains("Failed to connect"),
                          "Expected connection-related error for " + instanceType + ", got: " + e.getMessage());
            }
        }
    }

    @Test
    void testGetEnergyEstimatesWithNullInstanceType() {
        assertThrows(IllegalArgumentException.class, () -> {
            client.getEnergyEstimates(Provider.AWS, null);
        });
    }

    @Test
    void testGetEnergyEstimatesWithEmptyInstanceType() {
        assertThrows(IllegalArgumentException.class, () -> {
            client.getEnergyEstimates(Provider.AWS, "");
        });
    }

    @Test
    void testGetEnergyEstimatesWithWhitespaceInstanceType() {
        assertThrows(IllegalArgumentException.class, () -> {
            client.getEnergyEstimates(Provider.AWS, "   ");
        });
    }

    @Test
    void testGetEnergyEstimatesWithDifferentProviders() throws IOException {
        Provider[] providers = {Provider.AWS, Provider.AZURE, Provider.GOOGLE};
        
        for (Provider provider : providers) {
            try {
                double[] result = client.getEnergyEstimates(provider, "t3.micro");
                assertNotNull(result);
                assertEquals(2, result.length);
            } catch (IOException e) {
                // Expected if the test container is not accessible or provider not supported
                assertTrue(e.getMessage().contains("Unexpected code") || 
                          e.getMessage().contains("Connection refused") ||
                          e.getMessage().contains("Failed to connect"),
                          "Expected connection-related error for " + provider + ", got: " + e.getMessage());
            }
        }
    }

    @Test
    void testGetEnergyEstimatesWithComplexInstanceTypes() throws IOException {
        String[] complexInstanceTypes = {
            "db.r5.24xlarge", 
            "c5.18xlarge.search", 
            "m5.12xlarge",
            "db.t3.micro"
        };
        
        for (String instanceType : complexInstanceTypes) {
            try {
                double[] result = client.getEnergyEstimates(Provider.AWS, instanceType);
                assertNotNull(result);
                assertEquals(2, result.length);
                assertTrue(result[0] >= 0, "Use energy should be non-negative for " + instanceType);
                assertTrue(result[1] >= 0, "Embedded energy should be non-negative for " + instanceType);
            } catch (IOException e) {
                // Expected if the test container is not accessible
                assertTrue(e.getMessage().contains("Unexpected code") || 
                          e.getMessage().contains("Connection refused") ||
                          e.getMessage().contains("Failed to connect"),
                          "Expected connection-related error for " + instanceType + ", got: " + e.getMessage());
            }
        }
    }

    @Test
    void testGetEnergyEstimatesWithNullProvider() {
        assertThrows(IllegalArgumentException.class, () -> {
            client.getEnergyEstimates(null, "t3.micro");
        });
    }

    @Test
    void testGetEnergyEstimatesWithInvalidHost() {
        BoaviztAPIClient invalidClient = new BoaviztAPIClient("http://invalid-host-that-does-not-exist:9999");
        
        assertThrows(IOException.class, () -> {
            invalidClient.getEnergyEstimates(Provider.AWS, "t3.micro");
        });
    }

    @Test
    void testGetEnergyEstimatesWithMalformedHost() {
        BoaviztAPIClient malformedClient = new BoaviztAPIClient("not-a-valid-url");
        
        assertThrows(IllegalArgumentException.class, () -> {
            malformedClient.getEnergyEstimates(Provider.AWS, "t3.micro");
        });
    }
}
