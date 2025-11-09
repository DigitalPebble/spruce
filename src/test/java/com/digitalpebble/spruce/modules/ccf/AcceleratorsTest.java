// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AcceleratorsTest {

    private static final Accelerators accelerators = new Accelerators();

    private static final StructType schema = Utils.getSchema(accelerators);

    @BeforeAll
    static void initialize() {
        accelerators.init(Map.of());
    }

    //  {PRODUCT_INSTANCE_TYPE, LINE_ITEM_OPERATION, LINE_ITEM_PRODUCT_CODE, USAGE_AMOUNT};
    private static Stream<Arguments> provideArgsWithType() {
        return Stream.of(Arguments.of("g5.xlarge", "RunInstances", "AmazonEC2", 1.0d, 0.0855d), Arguments.of("g5.xlarge", "RunInstances", "AmazonEC2", 2.0d, 0.171d), Arguments.of("g4dn.xlarge", "RunInstances", "AmazonEC2", 1.0d, 0.0395d), Arguments.of("c5.xlarge", "RunInstances", "AmazonEC2", 1.0d, null), Arguments.of("g6.8xlarge", "RunInstances", "AmazonEC2", 1.0d, 0.04d));
    }

    @ParameterizedTest
    @MethodSource("provideArgsWithType")
    void process(String PRODUCT_INSTANCE_TYPE, String LINE_ITEM_OPERATION, String LINE_ITEM_PRODUCT_CODE, double USAGE_AMOUNT, Double expected) {
        Object[] values = new Object[]{PRODUCT_INSTANCE_TYPE, LINE_ITEM_OPERATION, LINE_ITEM_PRODUCT_CODE, USAGE_AMOUNT, null};
        Row row = new GenericRowWithSchema(values, schema);
        Row result = accelerators.process(row);
        if (expected != null) {
            assertEquals(expected, ENERGY_USED.getDouble(result), 0.0001);
        } else {
            assertTrue(ENERGY_USED.isNullAt(result));
        }
    }

}