// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the static-CSV variant: the bundled {@code boavizta/instanceTypes.csv} is loaded and
 * resolved correctly. Extraction logic and the abstract enrichment template are exercised in
 * {@link AbstractBoaviztaAwsTest}.
 */
public class BoaviztAPIstaticTest {

    private BoaviztAPIstatic module;
    private StructType schema;

    @BeforeEach
    void setUp() {
        module = new BoaviztAPIstatic();
        module.init(new HashMap<>());
        schema = Utils.getSchema(module);
    }

    @Test
    void enrichesEc2RowWithImpactsFromBundledCsv() {
        // t3.micro is a well-known entry in src/main/resources/boavizta/instanceTypes.csv
        Map<Column, Object> enriched = enrich("t3.micro", "AmazonEC2", "RunInstances", "AmazonEC2", 2.0);

        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
        assertNotNull(enriched.get(SpruceColumn.EMBODIED_EMISSIONS));
        assertNotNull(enriched.get(SpruceColumn.EMBODIED_ADP));
        assertTrue((double) enriched.get(SpruceColumn.ENERGY_USED) > 0,
                "Energy should scale with usage_amount > 0");
    }

    @Test
    void unknownInstanceTypeProducesNoEnrichment() {
        Map<Column, Object> enriched = enrich("definitely-not-a-real-type",
                "AmazonEC2", "RunInstances", "AmazonEC2", 1.0);
        assertTrue(enriched.isEmpty());
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
}
