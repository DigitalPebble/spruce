// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class BoaviztAPITest {

    private static final String TEST_HOST = "http://localhost:5000";

    @Nested
    class ValidationTests {
        private BoaviztAPI api;
        private StructType schema;

        @BeforeEach
        void setUp() {
            api = new BoaviztAPI();
            schema = Utils.getSchema(api);
        }

        @Test
        void testColumnsNeeded() {
            Column[] needed = api.columnsNeeded();
            assertEquals(5, needed.length);
            assertEquals(CURColumn.PRODUCT_INSTANCE_TYPE, needed[0]);
            assertEquals(CURColumn.PRODUCT_SERVICE_CODE, needed[1]);
            assertEquals(CURColumn.LINE_ITEM_OPERATION, needed[2]);
            assertEquals(CURColumn.LINE_ITEM_PRODUCT_CODE, needed[3]);
            assertEquals(CURColumn.USAGE_AMOUNT, needed[4]);
        }

        @Test
        void testColumnsAdded() {
            Column[] added = api.columnsAdded();
            assertEquals(3, added.length);
            assertEquals(SpruceColumn.ENERGY_USED, added[0]);
            assertEquals(SpruceColumn.EMBODIED_EMISSIONS, added[1]);
            assertEquals(SpruceColumn.EMBODIED_ADP, added[2]);
        }

        @Test
        void testInitWithDefaultAddress() {
            assertDoesNotThrow(() -> {
                Map<String, Object> params = new HashMap<>();
                api.init(params);
                // Should use default address "http://localhost:5000"
            });
        }

        @Test
        void testInitWithCustomAddress() {
            assertDoesNotThrow(() -> {
                Map<String, Object> params = new HashMap<>();
                params.put("address", "http://custom-host:8080");
                api.init(params);
                // The address should be set to the custom value
            });
        }

        @ParameterizedTest
        @MethodSource("nullValueTestCases")
        void testProcessWithNullValues(String instanceType, String serviceCode, String operation, String productCode) {
            Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();
            api.enrich(row, enriched);

            // Should not add any enrichment
            assertTrue(enriched.isEmpty());
        }

        static Stream<Arguments> nullValueTestCases() {
            return Stream.of(
                    Arguments.of("AWSDataTransfer", null, null, null),
                    Arguments.of("t3.micro", "AmazonEC2", null, "AmazonEC2"),
                    Arguments.of("t3.micro", "AmazonEC2", "RunInstances", null),
                    Arguments.of("t3.micro", null, "RunInstances", "AmazonEC2"),
                    Arguments.of(null, null, null, null)
            );
        }

        @ParameterizedTest
        @MethodSource("emptyValueTestCases")
        void testProcessWithEmptyValues(String instanceType, String serviceCode, String operation, String productCode) {
            Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();

            if (instanceType == null || instanceType.trim().isEmpty()) {
                try {
                    api.enrich(row, enriched);
                    assertTrue(enriched.isEmpty(), "Should not enrich for null/empty instance types");
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains("Instance type cannot be null, empty, or whitespace only"));
                }
            } else {
                api.enrich(row, enriched);
                assertTrue(enriched.isEmpty(), "Should not enrich for other empty value cases");
            }
        }

        static Stream<Arguments> emptyValueTestCases() {
            return Stream.of(
                    Arguments.of("", "AmazonEC2", "RunInstances", "AmazonEC2"),
                    Arguments.of("   ", "AmazonEC2", "RunInstances", "AmazonEC2"),
                    Arguments.of("", "", "", ""),
                    Arguments.of("   ", "   ", "   ", "   ")
            );
        }

        @ParameterizedTest
        @MethodSource("unsupportedValueTestCases")
        void testProcessWithUnsupportedValues(String instanceType, String serviceCode, String operation, String productCode) {
            Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();
            api.enrich(row, enriched);

            // Should not add any enrichment for unsupported services/operations
            assertTrue(enriched.isEmpty());
        }

        static Stream<Arguments> unsupportedValueTestCases() {
            return Stream.of(
                    Arguments.of("t3.micro", "AmazonS3", "GetObject", "AmazonS3"),
                    Arguments.of("t3.micro", "AmazonEC2", "StopInstances", "AmazonEC2"),
                    Arguments.of("t3.micro", "amazonec2", "RunInstances", "AmazonEC2")
            );
        }

        // @ParameterizedTest
        // @MethodSource("edgeCaseTestCases")
        // this test only makes sense if it can connect to a running instance of the API
        void testProcessWithEdgeCases(String instanceType, String serviceCode, String operation, String productCode) {
            Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();
            api.enrich(row, enriched);

            // Should not add any enrichment for edge cases
            assertTrue(enriched.isEmpty());
        }

        static Stream<Arguments> edgeCaseTestCases() {
            return Stream.of(
                    Arguments.of("t3.micro@test", "AmazonEC2", "RunInstances", "AmazonEC2"),
                    Arguments.of("t3.micro".repeat(100), "AmazonEC2", "RunInstances", "AmazonEC2")
            );
        }
    }

    @Nested
    class NetworkTests {
        private MockWebServer mockWebServer;
        private BoaviztAPI api;
        private StructType schema;

        @BeforeEach
        void setUp() throws IOException {
            mockWebServer = new MockWebServer();
            mockWebServer.start(0);
            final String address = "http://localhost:" + mockWebServer.getPort();
            api = new BoaviztAPI();
            Map<String, Object> params = new HashMap<>();
            params.put("address", address);
            api.init(params);
            schema = Utils.getSchema(api);
        }

        @AfterEach
        void tearDown() throws IOException {
            mockWebServer.shutdown();
        }

        private Map<Column, Object> callEnrich(String productInstanceType, String productServiceCode, String lineItemOperation, String lineItemProductCode, double usageAmount){
            Object[] values = new Object[]{productInstanceType, productServiceCode, lineItemOperation, lineItemProductCode, usageAmount, null, null, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();
            api.enrich(row, enriched);
            return enriched;
        }

        @Test
        void testProcessEC2InstanceWithValidData() throws IOException {
            // Mock the API response
            String mockResponse = createMockResponse("t3.micro");
            mockWebServer.enqueue(new MockResponse()
                    .setBody(mockResponse)
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json"));

            Map<Column, Object> enriched = callEnrich("t3.micro", "AmazonEC2", "RunInstances", "AmazonEC2", 10);

            // The map should contain enrichment data
            assertFalse(enriched.isEmpty());
        }

        @Test
        void testProcessElasticSearchInstanceWithValidData() throws IOException {
            // Mock the API response
            String mockResponse = createMockResponse("t3.micro");
            mockWebServer.enqueue(new MockResponse()
                    .setBody(mockResponse)
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json"));

            Map<Column, Object> enriched = callEnrich("t3.micro.search", "AmazonES", "ESDomain", "AmazonES", 1.0);

            // The map should contain enrichment data
            assertFalse(enriched.isEmpty());
        }

        @Test
        void testProcessRDSInstanceWithValidData() throws IOException {
            // Mock the API response
            String mockResponse = createMockResponse("t3.micro");
            mockWebServer.enqueue(new MockResponse()
                    .setBody(mockResponse)
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json"));

            Map<Column, Object> enriched = callEnrich("db.t3.micro", "AmazonRDS", "CreateDBInstance", "AmazonRDS", 1.0);

            // The map should contain enrichment data
            assertFalse(enriched.isEmpty());
        }

        @ParameterizedTest
        @MethodSource("validEC2OperationTestCases")
        void testProcessEC2WithDifferentOperationPrefixes(String operation) throws IOException {
            // Mock the API response
            String mockResponse = createMockResponse("t3.micro");
            mockWebServer.enqueue(new MockResponse()
                    .setBody(mockResponse)
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json"));

            Map<Column, Object> enriched = callEnrich("t3.micro", "AmazonEC2", operation, "AmazonEC2", 1.0);

            // The map should contain enrichment data
            assertFalse(enriched.isEmpty());
        }

        static Stream<Arguments> validEC2OperationTestCases() {
            return Stream.of(
                    Arguments.of("RunInstances"),
                    Arguments.of("RunInstances:0002"),
                    Arguments.of("RunInstances:0010")
            );
        }

        @ParameterizedTest
        @MethodSource("validRDSOperationTestCases")
        void testProcessRDSWithDifferentOperationPrefixes(String operation) throws IOException {
            // Mock the API response
            String mockResponse = createMockResponse("t3.micro");
            mockWebServer.enqueue(new MockResponse()
                    .setBody(mockResponse)
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json"));

            Map<Column, Object> enriched = callEnrich("db.t3.micro", "AmazonRDS", operation, "AmazonRDS", 1.0);

            // The map should contain enrichment data
            assertFalse(enriched.isEmpty());
        }

        static Stream<Arguments> validRDSOperationTestCases() {
            return Stream.of(
                    Arguments.of("CreateDBInstance"),
                    Arguments.of("CreateDBInstance:0002"),
                    Arguments.of("CreateDBInstance:0010")
            );
        }

        @ParameterizedTest
        @MethodSource("complexInstanceTypeTestCases")
        void testProcessWithComplexInstanceTypes(String instanceType) throws IOException {
            // Mock the API response
            String mockResponse = createMockResponse(instanceType);
            mockWebServer.enqueue(new MockResponse()
                    .setBody(mockResponse)
                    .setResponseCode(200)
                    .addHeader("Content-Type", "application/json"));

            Map<Column, Object> enriched = callEnrich(instanceType, "AmazonEC2", "RunInstances", "AmazonEC2", 1.0);

            // The map should contain enrichment data
            assertFalse(enriched.isEmpty());
        }

        static Stream<Arguments> complexInstanceTypeTestCases() {
            return Stream.of(
                    Arguments.of("db.r5.24xlarge"),
                    Arguments.of("c5.18xlarge.search"),
                    Arguments.of("m5.12xlarge")
            );
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
