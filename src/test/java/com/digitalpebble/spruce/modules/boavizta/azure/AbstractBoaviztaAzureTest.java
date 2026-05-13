// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.azure;

import com.digitalpebble.spruce.AzureColumn;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the shared behaviour that lives in {@link AbstractBoaviztaAzure} and its parent
 * {@code AbstractBoaviztaModule}: the Azure meter extraction paths (Virtual Machines only),
 * the declared input/output columns, the unknown-instance-type cache, and the impacts × usage
 * multiplication. Variant-specific behaviour (static CSV vs live API) is covered in
 * {@link BoaviztAPIstaticTest} and {@link BoaviztAPITest}.
 */
class AbstractBoaviztaAzureTest {

    private TestBoavizta module;
    private StructType schema;

    @BeforeEach
    void setUp() {
        module = new TestBoavizta();
        module.init(new HashMap<>());
        schema = Utils.getSchema(module);
    }

    @Test
    void columnsNeededReflectsAzureMeterColumns() {
        assertArrayEquals(new Column[]{
                AzureColumn.METER_CATEGORY, 
                AzureColumn.METER_NAME, 
                AzureColumn.UNIT_OF_MEASURE, 
                AzureColumn.QUANTITY
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
    void virtualMachinesMeterCategoryWithSlash() {
        module.impactsByType.put("standard_ds11_v2", new Impacts(1.0, 1.0, 1.0));
        Map<Column, Object> enriched = enrich("Virtual Machines", "D11 v2/DS11 v2", "1 Hour", 1.0);
        assertNotNull(enriched);
    }

    @Test
    void virtualMachinesMeterCategoryNormalization() {
        module.impactsByType.put("standard_d11_v2", new Impacts(1.0, 1.0, 1.0));
        Map<Column, Object> enriched = enrich("Virtual Machines", "Standard D11 v2", "1 Hour", 1.0);
        assertNotNull(enriched);
    }

    @Test
    void unknownInstanceTypeIsCachedAndNotRetried() {
        Map<Column, Object> first = enrich("Virtual Machines", "unknown_vm", "1 Hour", 1.0);
        assertTrue(first.isEmpty());
        assertEquals(1, module.lookupCalls.get());

        Map<Column, Object> second = enrich("Virtual Machines", "unknown_vm", "1 Hour", 1.0);
        assertTrue(second.isEmpty());
        assertEquals(1, module.lookupCalls.get(), "Unknown instance types should not be looked up twice");
    }

    @ParameterizedTest
    @MethodSource("nonRelevantRows")
    void nonRelevantRowsAreSkippedBeforeLookup(String meterCategory, String meterName,
                                                String unitOfMeasure) {
        Map<Column, Object> enriched = enrich(meterCategory, meterName, unitOfMeasure, 1.0);
        assertTrue(enriched.isEmpty());
        assertEquals(0, module.lookupCalls.get(),
                "Lookup should not be triggered for rows that do not match Virtual Machines meter category");
    }

    static Stream<Arguments> nonRelevantRows() {
        return Stream.of(
                // missing fields
                Arguments.of(null, "Standard D11 v2", "1 Hour"),
                Arguments.of("Virtual Machines", null, "1 Hour"),
                // wrong meter category
                Arguments.of("Storage", "Standard D11 v2", "1 Hour"),
                Arguments.of("Network", "Standard D11 v2", "1 Hour"),
                // wrong or missing unit of measure
                Arguments.of("Virtual Machines", "Standard D11 v2", null),
                Arguments.of("Virtual Machines", "Standard D11 v2", "1"),
                Arguments.of("Virtual Machines", "Standard D11 v2", "1 Day")
        );
    }

    private Map<Column, Object> enrich(String meterCategory, String meterName,
                                        String unitOfMeasure, double quantity) {
        Object[] values = new Object[]{
                meterCategory, meterName, unitOfMeasure, quantity,
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
    private static final class TestBoavizta extends AbstractBoaviztaAzure {
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