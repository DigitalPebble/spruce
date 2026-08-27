// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

import com.digitalpebble.spruce.AWSFOCUSColumn;
import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import com.digitalpebble.spruce.modules.boavizta.Impacts;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the shared behaviour that lives in {@link AbstractBoaviztaAws} and its parent
 * {@code AbstractBoaviztaModule}: the AWS CUR extraction paths (EC2 / ESDomain / RDS / MQ /
 * ElastiCache), the
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
                CURColumn.USAGE_AMOUNT,
                CURColumn.LINE_ITEM_USAGE_TYPE
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

    /** In a CUR the {@code mq.} prefix is already stripped from {@code product_instance_type}. */
    @Test
    void amazonMqCreateBrokerResolvesBrokerInstanceType() {
        module.impactsByType.put("m5.large", new Impacts(1.0, 2.0, 3.0));
        Map<Column, Object> enriched = enrich("m5.large", "AmazonMQ", "CreateBroker:0001", "AmazonMQ", 1.0,
                "USE1-ActiveMQ-InstanceUsage:mq.m5.large");

        assertEquals(1.0, (double) enriched.get(SpruceColumn.ENERGY_USED), 1e-12);
    }

    /**
     * A RabbitMQ cluster bills one unit per cluster-hour, so the impacts of a single broker have to
     * be multiplied by the number of nodes in the deployment.
     */
    @Test
    void amazonMqClusterScalesUsageByNodeCount() {
        module.impactsByType.put("m5.large", new Impacts(1.0, 2.0, 3.0));
        Map<Column, Object> enriched = enrich("m5.large", "AmazonMQ", "CreateBroker:0001", "AmazonMQ", 10.0,
                "USE1-RabbitMQ-3-InstanceUsage:mq.m5.large");

        assertEquals(30.0, (double) enriched.get(SpruceColumn.ENERGY_USED), 1e-12);
        assertEquals(60.0, (double) enriched.get(SpruceColumn.EMBODIED_EMISSIONS), 1e-12);
        assertEquals(90.0, (double) enriched.get(SpruceColumn.EMBODIED_ADP), 1e-12);
    }

    @Test
    void elastiCacheStripsCachePrefixBeforeLookup() {
        module.impactsByType.put("t3.medium", new Impacts(1.0, 1.0, 1.0));
        Map<Column, Object> enriched = enrich("cache.t3.medium", "AmazonElastiCache", "CreateCacheCluster:0001",
                "AmazonElastiCache", 1.0, "NodeUsage:cache.t3.medium");

        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
    }

    @ParameterizedTest
    @MethodSource("nodeCounts")
    void nodeCountReadFromUsageType(String usageType, int expected) {
        assertEquals(expected, AbstractBoaviztaAws.nodeCount(usageType));
    }

    static Stream<Arguments> nodeCounts() {
        return Stream.of(
                Arguments.of("USE1-RabbitMQ-3-InstanceUsage:mq.m5.large", 3),
                Arguments.of("USE1-ActiveMQ-Multi-AZ-InstanceUsage:mq.m5.large", 2),
                Arguments.of("USE1-ActiveMQ-InstanceUsage:mq.m5.large", 1),
                Arguments.of("USE1-RabbitMQ-InstanceUsage:mq.m7g.large", 1),
                // meters without the marker at all bill per instance
                Arguments.of("EUW2-BoxUsage:t3.xlarge", 1),
                Arguments.of("InstanceUsage:db.t3.micro", 1),
                Arguments.of("NodeUsage:cache.t3.medium", 1),
                Arguments.of(null, 1)
        );
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
                Arguments.of("t3.micro", "amazonec2", "RunInstances", "amazonec2"),
                // MQ and ElastiCache lines that are not broker/node usage
                Arguments.of("m5.large", "AmazonMQ", "CreateConfiguration", "AmazonMQ"),
                Arguments.of("cache.t3.medium", "AmazonElastiCache", "CreateSnapshot", "AmazonElastiCache")
        );
    }

    private Map<Column, Object> enrich(String instanceType, String serviceCode,
                                        String operation, String productCode, double usage) {
        return enrich(instanceType, serviceCode, operation, productCode, usage, null);
    }

    private Map<Column, Object> enrich(String instanceType, String serviceCode,
                                        String operation, String productCode, double usage,
                                        String usageType) {
        Object[] values = new Object[]{
                instanceType, serviceCode, operation, productCode, usage, usageType,
                null, null, null
        };
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);
        return enriched;
    }

    /**
     * FOCUS reports carry no {@code product_instance_type}: the instance type is parsed from the
     * SkuMeter (the CUR usage type, e.g. {@code EUW2-BoxUsage:t3.micro}) and {@code x_ServiceCode}
     * stands in for both the product and service codes.
     */
    @Nested
    class FOCUSBinding {

        private TestBoavizta focusModule;
        private StructType focusSchema;

        @BeforeEach
        void setUp() {
            focusModule = new TestBoavizta();
            focusModule.bindReportFormat(ReportFormat.FOCUS);
            focusModule.init(new HashMap<>());
            focusSchema = Utils.getSchema(focusModule);
        }

        @Test
        void columnsNeededReflectsFOCUSColumns() {
            assertArrayEquals(new Column[]{
                    FOCUSColumn.SKU_METER,
                    AWSFOCUSColumn.X_SERVICE_CODE,
                    AWSFOCUSColumn.X_OPERATION,
                    FOCUSColumn.CONSUMED_QUANTITY
            }, focusModule.columnsNeeded());
        }

        @Test
        void ec2InstanceTypeParsedFromSkuMeter() {
            focusModule.impactsByType.put("t3.micro", new Impacts(0.001, 5.0, 1.0e-4));
            Map<Column, Object> enriched = enrich("EUW2-BoxUsage:t3.micro", "AmazonEC2", "RunInstances", 10.0);
            assertEquals(0.01, (double) enriched.get(SpruceColumn.ENERGY_USED), 1e-12);
        }

        @Test
        void esDomainStripsSearchSuffixBeforeLookup() {
            focusModule.impactsByType.put("t3.micro", new Impacts(1.0, 1.0, 1.0));
            Map<Column, Object> enriched = enrich("ESInstance:t3.micro.search", "AmazonES", "ESDomain", 1.0);
            assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
        }

        @Test
        void rdsStripsDbPrefixBeforeLookup() {
            focusModule.impactsByType.put("t3.micro", new Impacts(1.0, 1.0, 1.0));
            Map<Column, Object> enriched = enrich("InstanceUsage:db.t3.micro", "AmazonRDS", "CreateDBInstance", 1.0);
            assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
        }

        /** Unlike the CUR, the SkuMeter keeps the {@code mq.} prefix, so it has to be stripped. */
        @Test
        void amazonMqStripsMqPrefixAndScalesByNodeCount() {
            focusModule.impactsByType.put("m5.large", new Impacts(1.0, 1.0, 1.0));
            Map<Column, Object> enriched = enrich("USE1-RabbitMQ-3-InstanceUsage:mq.m5.large", "AmazonMQ",
                    "CreateBroker:0001", 10.0);
            assertEquals(30.0, (double) enriched.get(SpruceColumn.ENERGY_USED), 1e-12);
        }

        @Test
        void elastiCacheStripsCachePrefixBeforeLookup() {
            focusModule.impactsByType.put("t3.medium", new Impacts(1.0, 1.0, 1.0));
            Map<Column, Object> enriched = enrich("NodeUsage:cache.t3.medium", "AmazonElastiCache",
                    "CreateCacheCluster:0001", 1.0);
            assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
        }

        @ParameterizedTest
        @MethodSource("nonRelevantFocusRows")
        void nonRelevantRowsAreSkippedBeforeLookup(String skuMeter, String serviceCode, String operation) {
            Map<Column, Object> enriched = enrich(skuMeter, serviceCode, operation, 1.0);
            assertTrue(enriched.isEmpty());
            assertEquals(0, focusModule.lookupCalls.get());
        }

        static Stream<Arguments> nonRelevantFocusRows() {
            return Stream.of(
                    // colon-bearing meters that are not instance lines are kept out by the operation gates
                    Arguments.of("EBS:VolumeUsage.gp3", "AmazonEC2", "CreateVolume-Gp3"),
                    // meters without a colon carry no instance type
                    Arguments.of("Requests-Tier1", "AmazonS3", "GetObject"),
                    Arguments.of(null, "AmazonEC2", "RunInstances"),
                    Arguments.of("BoxUsage:t3.micro", "AmazonEC2", null)
            );
        }

        private Map<Column, Object> enrich(String skuMeter, String serviceCode, String operation, double usage) {
            Object[] values = new Object[]{
                    skuMeter, serviceCode, operation, usage,
                    null, null, null
            };
            Row row = new GenericRowWithSchema(values, focusSchema);
            Map<Column, Object> enriched = new HashMap<>();
            focusModule.enrich(row, enriched);
            return enriched;
        }
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
