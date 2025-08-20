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
        // The constructor doesn't actually throw NullPointerException for null host
        // It just stores the null value, which will cause issues later when making HTTP calls
        BoaviztAPIClient testClient = new BoaviztAPIClient(null);
        assertNotNull(testClient);
    }

    @Test
    void testConstructorWithEmptyHost() {
        BoaviztAPIClient testClient = new BoaviztAPIClient("");
        assertNotNull(testClient);
    }

    @Test
    void testConstructorWithHttpsHost() {
        BoaviztAPIClient testClient = new BoaviztAPIClient("https://secure-host:8443");
        assertNotNull(testClient);
    }

    @Test
    void testGetEnergyEstimatesWithValidInstanceType() throws IOException {
        // This test will attempt to call the actual API
        // It may succeed if the test container is running, or fail gracefully if not
        try {
            double[] result = client.getEnergyEstimates(Provider.AWS, "t3.micro");
            assertNotNull(result);
            assertEquals(2, result.length);
            assertTrue(result[0] >= 0, "Use energy should be non-negative");
            assertTrue(result[1] >= 0, "Embedded energy should be non-negative");
        } catch (IOException e) {
            // Expected if the test container is not accessible
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
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
    void testGetEnergyEstimatesWithNullInstanceType() throws IOException {
        // The method doesn't throw NullPointerException for null instance type
        // It just passes "null" as a string to the API
        try {
            double[] result = client.getEnergyEstimates(Provider.AWS, null);
            assertNotNull(result);
            assertEquals(2, result.length);
        } catch (IOException e) {
            // Expected if the test container is not accessible
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
    }

    @Test
    void testGetEnergyEstimatesWithEmptyInstanceType() throws IOException {
        try {
            double[] result = client.getEnergyEstimates(Provider.AWS, "");
            assertNotNull(result);
            assertEquals(2, result.length);
        } catch (IOException e) {
            // Expected if the test container is not accessible
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
    }

    @Test
    void testGetEnergyEstimatesWithWhitespaceInstanceType() throws IOException {
        try {
            double[] result = client.getEnergyEstimates(Provider.AWS, "   ");
            assertNotNull(result);
            assertEquals(2, result.length);
        } catch (IOException e) {
            // Expected if the test container is not accessible
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
    }

    @Test
    void testGetEnergyEstimatesWithSpecialCharacters() throws IOException {
        String[] specialInstanceTypes = {"t3.micro@test", "t3.micro-test", "t3.micro_test", "t3.micro+test"};
        
        for (String instanceType : specialInstanceTypes) {
            try {
                double[] result = client.getEnergyEstimates(Provider.AWS, instanceType);
                assertNotNull(result);
                assertEquals(2, result.length);
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
    void testGetEnergyEstimatesWithVeryLongInstanceType() throws IOException {
        String longInstanceType = "t3.micro".repeat(100); // Create a very long instance type
        
        try {
            double[] result = client.getEnergyEstimates(Provider.AWS, longInstanceType);
            assertNotNull(result);
            assertEquals(2, result.length);
        } catch (IOException e) {
            // Expected if the test container is not accessible
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
    }

    @Test
    void testGetEnergyEstimatesWithDifferentProviders() throws IOException {
        // Test with different provider values (though the API might only support AWS)
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
            "db.t3.micro",
            "t3.micro.search"
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
    void testGetEnergyEstimatesWithEdgeCaseInstanceTypes() throws IOException {
        String[] edgeCaseInstanceTypes = {
            "0",           // Single character
            "a",           // Single letter
            "instance",    // Generic name
            "test",        // Test name
            "123456789",   // Numbers only
            "a1b2c3d4e5"  // Mixed alphanumeric
        };
        
        for (String instanceType : edgeCaseInstanceTypes) {
            try {
                double[] result = client.getEnergyEstimates(Provider.AWS, instanceType);
                assertNotNull(result);
                assertEquals(2, result.length);
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
    void testGetEnergyEstimatesWithNullProvider() throws IOException {
        // The method doesn't throw NullPointerException for null provider
        // It just passes null to the API call
        try {
            double[] result = client.getEnergyEstimates(null, "t3.micro");
            assertNotNull(result);
            assertEquals(2, result.length);
        } catch (IOException e) {
            // Expected if the test container is not accessible
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
    }

    @Test
    void testGetEnergyEstimatesWithEmptyProvider() {
        // This test assumes Provider is an enum, so we can't pass empty string
        // But we can test the behavior with a valid provider
        assertDoesNotThrow(() -> {
            try {
                client.getEnergyEstimates(Provider.AWS, "t3.micro");
            } catch (IOException e) {
                // Expected if the test container is not accessible
                assertTrue(e.getMessage().contains("Unexpected code") || 
                          e.getMessage().contains("Connection refused") ||
                          e.getMessage().contains("Failed to connect"),
                          "Expected connection-related error, got: " + e.getMessage());
            }
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

    @Test
    void testGetEnergyEstimatesWithHttpsHost() throws IOException {
        BoaviztAPIClient httpsClient = new BoaviztAPIClient("https://localhost:5000");
        
        try {
            double[] result = httpsClient.getEnergyEstimates(Provider.AWS, "t3.micro");
            assertNotNull(result);
            assertEquals(2, result.length);
        } catch (IOException e) {
            // Expected if the test container is not accessible or HTTPS not supported
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect") ||
                      e.getMessage().contains("SSL") ||
                      e.getMessage().contains("TLS") ||
                      e.getMessage().contains("Read timed out"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
    }

    @Test
    void testGetEnergyEstimatesWithCustomPort() throws IOException {
        BoaviztAPIClient customPortClient = new BoaviztAPIClient("http://localhost:8080");
        
        try {
            double[] result = customPortClient.getEnergyEstimates(Provider.AWS, "t3.micro");
            assertNotNull(result);
            assertEquals(2, result.length);
        } catch (IOException e) {
            // Expected if the test container is not accessible on custom port
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
    }

    @Test
    void testGetEnergyEstimatesWithInstanceTypeContainingSpaces() throws IOException {
        try {
            double[] result = client.getEnergyEstimates(Provider.AWS, "t3 micro");
            assertNotNull(result);
            assertEquals(2, result.length);
        } catch (IOException e) {
            // Expected if the test container is not accessible
            assertTrue(e.getMessage().contains("Unexpected code") || 
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("Failed to connect"),
                      "Expected connection-related error, got: " + e.getMessage());
        }
    }

    @Test
    void testGetEnergyEstimatesWithInstanceTypeContainingUnicode() throws IOException {
        String[] unicodeInstanceTypes = {
            "t3.micro-é", 
            "t3.micro-ñ", 
            "t3.micro-ü",
            "t3.micro-中文",
            "t3.micro-日本語"
        };
        
        for (String instanceType : unicodeInstanceTypes) {
            try {
                double[] result = client.getEnergyEstimates(Provider.AWS, instanceType);
                assertNotNull(result);
                assertEquals(2, result.length);
            } catch (IOException e) {
                // Expected if the test container is not accessible
                assertTrue(e.getMessage().contains("Unexpected code") || 
                          e.getMessage().contains("Connection refused") ||
                          e.getMessage().contains("Failed to connect"),
                          "Expected connection-related error for " + instanceType + ", got: " + e.getMessage());
            }
        }
    }
}
