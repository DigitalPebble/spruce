// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import com.digitalpebble.spruce.modules.boavizta.Impacts;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the shared behaviour that lives in {@link AbstractBoaviztaAws} and its parent
 * {@code AbstractBoaviztaModule}: the AWS CUR extraction paths (EC2 / ESDomain / RDS), the
 * declared input/output columns, the unknown-instance-type cache, and the impacts × usage
 * multiplication. Variant-specific behaviour (static CSV vs live API) is covered in
 * {@link BoaviztAPIstaticTest} and {@link BoaviztAPITest}.
 */
class AbstractBoaviztaAwsTest {

    private TestBoavizta module;
    private StructType schema;

    @BeforeEach
    void setUp() {
        module = new TestBoavizta();
        module.init(new HashMap<>());
        schema = Utils.getSchema(module);
    }

    @Test
    void columnsNeededReflectsAwsCurColumns() {
        assertArrayEquals(new Column[]{
                CURColumn.PRODUCT_INSTANCE_TYPE,
                CURColumn.PRODUCT_SERVICE_CODE,
                CURColumn.LINE_ITEM_OPERATION,
                CURColumn.LINE_ITEM_PRODUCT_CODE,
                CURColumn.USAGE_AMOUNT
        }, module.columnsNeeded());
    }

    @Test
    void columnsAddedReflectsImpactsOutputs() {
        assertArrayEquals(new Column[]{
                SpruceColumn.ENERGY_USED,
                SpruceColumn.EMBODIED_EMISSIONS,
                SpruceColumn.EMBODIED_ADP
        }, module.columnsAdded());
    }

    @Test
    void ec2RunInstancesUsesInstanceTypeVerbatim() {
        module.impactsByType.put("t3.micro", new Impacts(0.001, 5.0, 1.0e-4));
        Map<Column, Object> enriched = enrich("t3.micro", "AmazonEC2", "RunInstances", "AmazonEC2", 10.0);

        assertEquals(0.01, (double) enriched.get(SpruceColumn.ENERGY_USED), 1e-12);
        assertEquals(50.0, (double) enriched.get(SpruceColumn.EMBODIED_EMISSIONS), 1e-12);
        assertEquals(1.0e-3, (double) enriched.get(SpruceColumn.EMBODIED_ADP), 1e-15);
    }

    @Test
    void ec2RunInstancesAcceptsSuffixedOperation() {
        module.impactsByType.put("m5.large", new Impacts(1.0, 1.0, 1.0));
        Map<Column, Object> enriched = enrich("m5.large", "AmazonEC2", "RunInstances:0002", "AmazonEC2", 1.0);
        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
    }

    @Test
    void esDomainStripsSearchSuffixBeforeLookup() {
        module.impactsByType.put("t3.micro", new Impacts(1.0, 1.0, 1.0));
        Map<Column, Object> enriched = enrich("t3.micro.search", "AmazonES", "ESDomain", "AmazonES", 1.0);
        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
    }

    @Test
    void rdsCreateDbInstanceStripsDbPrefixBeforeLookup() {
        module.impactsByType.put("t3.micro", new Impacts(1.0, 1.0, 1.0));
        Map<Column, Object> enriched = enrich("db.t3.micro", "AmazonRDS", "CreateDBInstance", "AmazonRDS", 1.0);
        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
    }

    @Test
    void unknownInstanceTypeIsCachedAndNotRetried() {
        Map<Column, Object> first = enrich("t3.unknown", "AmazonEC2", "RunInstances", "AmazonEC2", 1.0);
        assertTrue(first.isEmpty());
        assertEquals(1, module.lookupCalls.get());

        Map<Column, Object> second = enrich("t3.unknown", "AmazonEC2", "RunInstances", "AmazonEC2", 1.0);
        assertTrue(second.isEmpty());
        assertEquals(1, module.lookupCalls.get(), "Unknown instance types should not be looked up twice");
    }

    @ParameterizedTest
    @MethodSource("nonRelevantRows")
    void nonRelevantRowsAreSkippedBeforeLookup(String instanceType, String serviceCode,
                                                String operation, String productCode) {
        Map<Column, Object> enriched = enrich(instanceType, serviceCode, operation, productCode, 1.0);
        assertTrue(enriched.isEmpty());
        assertEquals(0, module.lookupCalls.get(),
                "Lookup should not be triggered for rows that do not match an EC2/ES/RDS pattern");
    }

    static Stream<Arguments> nonRelevantRows() {
        return Stream.of(
                // missing fields
                Arguments.of(null, "AmazonEC2", "RunInstances", "AmazonEC2"),
                Arguments.of("t3.micro", "AmazonEC2", null, "AmazonEC2"),
                Arguments.of("t3.micro", "AmazonEC2", "RunInstances", null),
                // wrong service / operation
                Arguments.of("t3.micro", "AmazonS3", "GetObject", "AmazonS3"),
                Arguments.of("t3.micro", "AmazonEC2", "StopInstances", "AmazonEC2"),
                // EC2 path requires service code match — different service code skips
                Arguments.of("t3.micro", "AmazonOther", "RunInstances", "AmazonEC2"),
                // case-sensitive product code match
                Arguments.of("t3.micro", "amazonec2", "RunInstances", "amazonec2")
        );
    }

    private Map<Column, Object> enrich(String instanceType, String serviceCode,
                                        String operation, String productCode, double usage) {
        Object[] values = new Object[]{
                instanceType, serviceCode, operation, productCode, usage,
                null, null, null
        };
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);
        return enriched;
    }

    /**
     * Stub subclass that exposes a controllable lookup so we can drive the abstract template
     * (extraction + impacts × usage + unknown caching) without depending on a real backend.
     */
    private static final class TestBoavizta extends AbstractBoaviztaAws {
        final Map<String, Impacts> impactsByType = new HashMap<>();
        final AtomicInteger lookupCalls = new AtomicInteger();

        @Override
        public void init(Map<String, Object> params) {
        }

        @Override
        protected Impacts lookupImpacts(String instanceType) {
            lookupCalls.incrementAndGet();
            return impactsByType.get(instanceType);
        }
    }
}
