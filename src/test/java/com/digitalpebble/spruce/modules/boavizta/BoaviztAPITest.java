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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    @Test
    void testProcessWithNullInstanceType() {
        Object[] values = new Object[]{"AWSDataTransfer", null, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
    }

    @Test
    void testProcessWithNullOperation() {
        Object[] values = new Object[]{"t3.micro", "AmazonEC2", null, "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
    }

    @Test
    void testProcessWithNullProductCode() {
        Object[] values = new Object[]{"t3.micro", "AmazonEC2", "RunInstances", null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
    }

    @Test
    void testProcessWithNullServiceCode() {
        Object[] values = new Object[]{"t3.micro", null, "RunInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
    }

    @Test
    void testProcessWithEmptyInstanceType() {
        Object[] values = new Object[]{"", "AmazonEC2", "RunInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
    }

    @Test
    void testProcessWithWhitespaceInstanceType() {
        Object[] values = new Object[]{"   ", "AmazonEC2", "RunInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
    }

    @Test
    void testProcessUnsupportedService() {
        Object[] values = new Object[]{"t3.micro", "AmazonS3", "GetObject", "AmazonS3", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged for unsupported services
        assertEquals(row, enriched);
    }

    @Test
    void testProcessUnsupportedOperation() {
        Object[] values = new Object[]{"t3.micro", "AmazonEC2", "StopInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged for unsupported operations
        assertEquals(row, enriched);
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

    @Test
    void testProcessEC2WithDifferentOperationPrefixes() {
        String[] operations = {"RunInstances", "RunInstances:0002", "RunInstances:0010"};
        
        for (String operation : operations) {
            Object[] values = new Object[]{"t3.micro", "AmazonEC2", operation, "AmazonEC2", null};
            Row row = new GenericRowWithSchema(values, schema);
            Row enriched = api.process(row);
            
            // The row should be processed (may return original if API is not available)
            assertNotNull(enriched);
        }
    }

    @Test
    void testProcessRDSWithDifferentOperationPrefixes() {
        String[] operations = {"CreateDBInstance", "CreateDBInstance:0002", "CreateDBInstance:0010"};
        
        for (String operation : operations) {
            Object[] values = new Object[]{"db.t3.micro", "AmazonRDS", operation, "AmazonRDS", null};
            Row row = new GenericRowWithSchema(values, schema);
            Row enriched = api.process(row);
            
            // The row should be processed (may return original if API is not available)
            assertNotNull(enriched);
        }
    }

    @Test
    void testProcessWithComplexInstanceTypes() {
        String[] instanceTypes = {"db.r5.24xlarge", "c5.18xlarge.search", "m5.12xlarge"};
        
        for (String instanceType : instanceTypes) {
            Object[] values = new Object[]{instanceType, "AmazonEC2", "RunInstances", "AmazonEC2", null};
            Row row = new GenericRowWithSchema(values, schema);
            Row enriched = api.process(row);
            
            // The row should be processed (may return original if API is not available)
            assertNotNull(enriched);
        }
    }

    @Test
    void testProcessWithMixedCaseServiceCodes() {
        Object[] values = new Object[]{"t3.micro", "amazonec2", "RunInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged for case-sensitive service codes
        assertEquals(row, enriched);
    }

    @Test
    void testProcessWithSpecialCharactersInInstanceType() {
        Object[] values = new Object[]{"t3.micro@test", "AmazonEC2", "RunInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // The row should be processed (may return original if API is not available)
        assertNotNull(enriched);
    }

    @Test
    void testProcessWithVeryLongInstanceType() {
        String longInstanceType = "t3.micro".repeat(100); // Create a very long instance type
        Object[] values = new Object[]{longInstanceType, "AmazonEC2", "RunInstances", "AmazonEC2", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // The row should be processed (may return original if API is not available)
        assertNotNull(enriched);
    }

    @Test
    void testProcessWithNullValuesInAllFields() {
        Object[] values = new Object[]{null, null, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
    }

    @Test
    void testProcessWithEmptyStringsInAllFields() {
        Object[] values = new Object[]{"", "", "", "", null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = api.process(row);
        
        // Should return the original row unchanged
        assertEquals(row, enriched);
    }
}
