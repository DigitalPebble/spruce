// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Provider;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class BoaviztAPIClientTest {

    private MockWebServer mockWebServer;
    private BoaviztAPIClient client;
    private static final String TEST_HOST = "http://localhost:5000";

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start(5000);
        client = new BoaviztAPIClient(TEST_HOST);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void testConstructor() {
        assertDoesNotThrow(() -> {
            new BoaviztAPIClient(TEST_HOST);
        });
    }

    @ParameterizedTest
    @MethodSource("invalidHostProvider")
    void testConstructorWithInvalidHosts(String invalidHost) {
        assertThrows(IllegalArgumentException.class, () -> {
            new BoaviztAPIClient(invalidHost);
        }, "Constructor should throw IllegalArgumentException for invalid host: " + invalidHost);
    }

    static Stream<String> invalidHostProvider() {
        return Stream.of(null, "", "   ", "\t", "\n");
    }

    @Test
    void testGetEnergyEstimatesWithDifferentInstanceTypes() throws IOException {
        String[] instanceTypes = {"t3.micro", "t3.small", "t3.medium", "c5.large", "m5.xlarge"};
        
        for (String instanceType : instanceTypes) {
            // Mock the API response for this instance type
            String mockResponse = createMockResponse(instanceType);
            mockWebServer.enqueue(new MockResponse()
                .setBody(mockResponse)
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));
            
            double[] result = client.getEnergyEstimates(Provider.AWS, instanceType);
            assertNotNull(result);
            assertEquals(2, result.length);
            assertTrue(result[0] >= 0, "Use energy should be non-negative for " + instanceType);
            assertTrue(result[1] >= 0, "Embedded energy should be non-negative for " + instanceType);
        }
    }

    private String createMockResponse(String instanceType) {
        // Create a realistic mock response based on the BoaviztAPI format
        return String.format("""
            {
                "impacts": {
                    "pe": {
                        "use": {
                            "value": 15.5,
                            "unit": "MJ"
                        },
                        "embedded": {
                            "value": 120.0,
                            "unit": "MJ"
                        }
                    }
                }
            }
            """);
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
            // Mock the API response
            String mockResponse = createMockResponse("t3.micro");
            mockWebServer.enqueue(new MockResponse()
                .setBody(mockResponse)
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));
            
            double[] result = client.getEnergyEstimates(provider, "t3.micro");
            assertNotNull(result);
            assertEquals(2, result.length);
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
            // Mock the API response
            String mockResponse = createMockResponse(instanceType);
            mockWebServer.enqueue(new MockResponse()
                .setBody(mockResponse)
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json"));
            
            double[] result = client.getEnergyEstimates(Provider.AWS, instanceType);
            assertNotNull(result);
            assertEquals(2, result.length);
            assertTrue(result[0] >= 0, "Use energy should be non-negative for " + instanceType);
            assertTrue(result[1] >= 0, "Embedded energy should be non-negative for " + instanceType);
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
        BoaviztAPIClient invalidClient = new BoaviztAPIClient("not-a-valid-url");
        
        assertThrows(IllegalArgumentException.class, () -> {
            invalidClient.getEnergyEstimates(Provider.AWS, "t3.micro");
        });
    }
}
