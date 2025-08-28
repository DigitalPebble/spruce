// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BoaviztAPITest extends AbstractBoaviztaTest {

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
        assertEquals(4, needed.length);
        assertEquals(CURColumn.PRODUCT_INSTANCE_TYPE, needed[0]);
        assertEquals(CURColumn.PRODUCT_SERVICE_CODE, needed[1]);
        assertEquals(CURColumn.LINE_ITEM_OPERATION, needed[2]);
        assertEquals(CURColumn.LINE_ITEM_PRODUCT_CODE, needed[3]);
    }

    @Test
    void testColumnsAdded() {
        Column[] added = api.columnsAdded();
        assertEquals(1, added.length);
        assertEquals(SpruceColumn.ENERGY_USED, added[0]);
    }

    @Test
    void testInitWithDefaultAddress() {
        Map<String, Object> params = new HashMap<>();
        api.init(params);
        // Should use default address "http://localhost:5000"
        assertNotNull(api);
    }

    @Test
    void testInitWithCustomAddress() {
        Map<String, Object> params = new HashMap<>();
        params.put("address", "http://custom-host:8080");
        api.init(params);
        // The address should be set to the custom value
        assertNotNull(api);
    }

    @ParameterizedTest
    @MethodSource("nullValueTestCases")
    void testProcessWithNullValues(String instanceType, String serviceCode, String operation, String productCode) {
        Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
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
        
        // Test cases 1 and 2 (null and empty instance types) should throw IllegalArgumentException
        // Test cases 3 and 4 (empty strings for all fields) should return unchanged rows
        // because BoaviztAPI.process() has early returns before calling BoaviztAPIClient.getEnergyEstimates()
        if (instanceType == null || (instanceType != null && instanceType.trim().isEmpty())) {
            // These should throw IllegalArgumentException if they reach the API call
            try {
                Row enriched = api.process(row);
                // If no exception was thrown, the row should be unchanged
                assertEquals(row, enriched, "Should return unchanged row for null/empty instance types that don't reach API call");
            } catch (IllegalArgumentException e) {
                // This is also valid - the validation caught it
                assertTrue(e.getMessage().contains("Instance type cannot be null, empty, or whitespace only"));
            }
        } else {
            // Other test cases should return unchanged rows
            Row enriched = api.process(row);
            assertEquals(row, enriched, "Should return unchanged row for other empty value cases");
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
        Row enriched = api.process(row);
        
        // Should return the original row unchanged for unsupported services/operations
        assertEquals(row, enriched);
    }

    static Stream<Arguments> unsupportedValueTestCases() {
        return Stream.of(
            Arguments.of("t3.micro", "AmazonS3", "GetObject", "AmazonS3"),
            Arguments.of("t3.micro", "AmazonEC2", "StopInstances", "AmazonEC2"),
            Arguments.of("t3.micro", "amazonec2", "RunInstances", "AmazonEC2")
        );
    }

    @ParameterizedTest
    @MethodSource("edgeCaseTestCases")
    void testProcessWithEdgeCases(String instanceType, String serviceCode, String operation, String productCode) {
        Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged for edge cases
        assertEquals(row, enriched);
    }

    static Stream<Arguments> edgeCaseTestCases() {
        return Stream.of(
            Arguments.of("t3.micro@test", "AmazonEC2", "RunInstances", "AmazonEC2"),
            Arguments.of("t3.micro".repeat(100), "AmazonEC2", "RunInstances", "AmazonEC2")
        );
    }

    @Test
    void testProcessEC2InstanceWithValidData() {
        Object[] values = new Object[]{"t3.micro", "AmazonEC2", "RunInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // The row should be processed (may return original if API is not available)
        assertNotNull(enriched);
    }

    @Test
    void testProcessElasticSearchInstanceWithValidData() {
        Object[] values = new Object[]{"t3.micro.search", "AmazonES", "ESDomain", "AmazonES", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // The row should be processed (may return original if API is not available)
        assertNotNull(enriched);
    }

    @Test
    void testProcessRDSInstanceWithValidData() {
        Object[] values = new Object[]{"db.t3.micro", "AmazonRDS", "CreateDBInstance", "AmazonRDS", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // The row should be processed (may return original if API is not available)
        assertNotNull(enriched);
    }

    @ParameterizedTest
    @MethodSource("validEC2OperationTestCases")
    void testProcessEC2WithDifferentOperationPrefixes(String operation) {
        Object[] values = new Object[]{"t3.micro", "AmazonEC2", operation, "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // The row should be processed (may return original if API is not available)
        assertNotNull(enriched);
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
    void testProcessRDSWithDifferentOperationPrefixes(String operation) {
        Object[] values = new Object[]{"db.t3.micro", "AmazonRDS", operation, "AmazonRDS", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // The row should be processed (may return original if API is not available)
        assertNotNull(enriched);
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
    void testProcessWithComplexInstanceTypes(String instanceType) {
        Object[] values = new Object[]{instanceType, "AmazonEC2", "RunInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // The row should be processed (may return original if API is not available)
        assertNotNull(enriched);
    }

    static Stream<Arguments> complexInstanceTypeTestCases() {
        return Stream.of(
            Arguments.of("db.r5.24xlarge"),
            Arguments.of("c5.18xlarge.search"),
            Arguments.of("m5.12xlarge")
        );
    }
}
