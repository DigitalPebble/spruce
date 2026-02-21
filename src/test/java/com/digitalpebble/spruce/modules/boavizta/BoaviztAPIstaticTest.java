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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BoaviztAPIstaticTest {

    @Nested
    class ValidationTests {
        private BoaviztAPIstatic module;
        private StructType schema;

        @BeforeEach
        void setUp() {
            module = new BoaviztAPIstatic();
            schema = Utils.getSchema(module);
            module.init(new HashMap<>());
        }

        @Test
        void testColumnsNeeded() {
            Column[] needed = module.columnsNeeded();
            assertEquals(5, needed.length);
            assertEquals(CURColumn.PRODUCT_INSTANCE_TYPE, needed[0]);
            assertEquals(CURColumn.PRODUCT_SERVICE_CODE, needed[1]);
            assertEquals(CURColumn.LINE_ITEM_OPERATION, needed[2]);
            assertEquals(CURColumn.LINE_ITEM_PRODUCT_CODE, needed[3]);
            assertEquals(CURColumn.USAGE_AMOUNT, needed[4]);
        }

        @Test
        void testColumnsAdded() {
            Column[] added = module.columnsAdded();
            assertEquals(3, added.length);
            assertEquals(SpruceColumn.ENERGY_USED, added[0]);
            assertEquals(SpruceColumn.EMBODIED_EMISSIONS, added[1]);
            assertEquals(SpruceColumn.EMBODIED_ADP, added[2]);
        }

        @ParameterizedTest
        @MethodSource("nullValueTestCases")
        void testProcessWithNullValues(String instanceType, String serviceCode, String operation, String productCode) {
            Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null, null, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();
            module.enrich(row, enriched);

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
            Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null, null, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();

            if (instanceType == null || instanceType.trim().isEmpty()) {
                try {
                    module.enrich(row, enriched);
                    assertTrue(enriched.isEmpty(), "Should not enrich for null/empty instance types");
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains("Instance type cannot be null, empty, or whitespace only"));
                }
            } else {
                module.enrich(row, enriched);
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
            Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null, null, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();
            module.enrich(row, enriched);

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

        @ParameterizedTest
        @MethodSource("edgeCaseTestCases")
        void testProcessWithEdgeCases(String instanceType, String serviceCode, String operation, String productCode) {
            Object[] values = new Object[]{instanceType, serviceCode, operation, productCode, null, null, null};
            Row row = new GenericRowWithSchema(values, schema);
            Map<Column, Object> enriched = new HashMap<>();
            module.enrich(row, enriched);

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
}
