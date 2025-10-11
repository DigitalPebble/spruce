// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Provider;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class BoaviztAPIClientTest {

    private static final String TEST_HOST = "http://localhost:5000";

    @Nested
    class ValidationTests {
        private BoaviztAPIClient client;

        @BeforeEach
        void setUp() {
            client = new BoaviztAPIClient(TEST_HOST);
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
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithNullInstanceType() {
            assertThrows(IllegalArgumentException.class, () -> {
                client.getImpacts(Provider.AWS, null);
            });
        }

        @Test
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithEmptyInstanceType() {
            assertThrows(IllegalArgumentException.class, () -> {
                client.getImpacts(Provider.AWS, "");
            });
        }

        @Test
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithWhitespaceInstanceType() {
            assertThrows(IllegalArgumentException.class, () -> {
                client.getImpacts(Provider.AWS, "   ");
            });
        }

        @Test
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithNullProvider() {
            assertThrows(IllegalArgumentException.class, () -> {
                client.getImpacts(null, "t3.micro");
            });
        }

        @Test
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithMalformedHost() {
            BoaviztAPIClient invalidClient = new BoaviztAPIClient("not-a-valid-url");

            assertThrows(IllegalArgumentException.class, () -> {
                invalidClient.getImpacts(Provider.AWS, "t3.micro");
            });
        }
    }

    @Nested
    class NetworkTests {
        private MockWebServer mockWebServer;
        private BoaviztAPIClient client;

        @BeforeEach
        void setUp() throws IOException {
            mockWebServer = new MockWebServer();
            mockWebServer.start(0);
            final String address = "http://localhost:" + mockWebServer.getPort();
            client = new BoaviztAPIClient(address);
        }

        @AfterEach
        void tearDown() throws IOException {
            mockWebServer.shutdown();
        }

        @Test
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithDifferentInstanceTypes() throws IOException {
            String[] instanceTypes = {"t3.micro", "t3.small", "t3.medium", "c5.large", "m5.xlarge"};

            for (String instanceType : instanceTypes) {
                // Mock the API response for this instance type
                String mockResponse = createMockResponse(instanceType);
                mockWebServer.enqueue(new MockResponse()
                        .setBody(mockResponse)
                        .setResponseCode(200)
                        .addHeader("Content-Type", "application/json"));

                Impacts result = client.getImpacts(Provider.AWS, instanceType);
                assertNotNull(result);
                assertTrue(result.getFinalEnergyKWh() >= 0, "Use energy should be non-negative for " + instanceType);
                assertTrue(result.getEmbeddedEmissionsGramsCO2eq() >= 0, "Embedded emissions should be non-negative for " + instanceType);
            }
        }

        @Test
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithDifferentProviders() throws IOException {
            Provider[] providers = {Provider.AWS, Provider.AZURE, Provider.GOOGLE};

            for (Provider provider : providers) {
                // Mock the API response
                String mockResponse = createMockResponse("t3.micro");
                mockWebServer.enqueue(new MockResponse()
                        .setBody(mockResponse)
                        .setResponseCode(200)
                        .addHeader("Content-Type", "application/json"));

                Impacts result = client.getImpacts(provider, "t3.micro");
                assertNotNull(result);
            }
        }

        @Test
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithComplexInstanceTypes() throws IOException {
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

                Impacts result = client.getImpacts(Provider.AWS, instanceType);
                assertNotNull(result);
                assertTrue(result.getFinalEnergyKWh() >= 0, "Use energy should be non-negative for " + instanceType);
                assertTrue(result.getEmbeddedEmissionsGramsCO2eq() >= 0, "Embedded emissions should be non-negative for " + instanceType);
            }
        }

        @Test
        void testgetEnergyAndEmbodiedEmissionsEstimatesWithInvalidHost() {
            BoaviztAPIClient invalidClient = new BoaviztAPIClient("http://invalid-host-that-does-not-exist:9999");

            assertThrows(IOException.class, () -> {
                invalidClient.getImpacts(Provider.AWS, "t3.micro");
            });
        }

        private String createMockResponse(String instanceType) {
            // Create a realistic mock response based on the BoaviztAPI format
            return """
                    {
                        "impacts": {
                            "gwp": {
                                  "unit": "kgCO2eq",
                                  "embedded": {
                                    "value": 0.0086
                                  }
                            },
                            "pe": {
                                "use": {
                                    "value": 15.5,
                                    "unit": "MJ"
                                },
                                "embedded": {
                                    "value": 120.0,
                                    "unit": "MJ"
                                }
                            },
                            "adp": {
                                "use": {
                                    "value": 7e-10,
                                    "unit": "kgSbeq"
                                },
                                "embedded": {
                                    "value": 4.7e-8,
                                    "unit": "kgSbeq"
                                }
                            }
                        }
                    }
                    """;
        }
    }
}
