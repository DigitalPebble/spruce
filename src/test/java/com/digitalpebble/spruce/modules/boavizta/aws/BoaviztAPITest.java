// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.SpruceColumn;
import com.digitalpebble.spruce.Utils;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the live-API variant: HTTP wiring, response parsing, caching and error handling.
 * Each test uses a unique instance type so it stays isolated from the static cache shared
 * across {@code BoaviztAPI} instances. Extraction logic and the abstract enrichment template
 * are exercised in {@link AbstractBoaviztaAwsTest}.
 */
public class BoaviztAPITest {

    private MockWebServer server;
    private BoaviztAPI module;
    private StructType schema;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start(0);
        module = new BoaviztAPI();
        Map<String, Object> params = new HashMap<>();
        params.put("address", "http://localhost:" + server.getPort());
        module.init(params);
        schema = Utils.getSchema(module);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    void successfulApiResponsePopulatesAllOutputColumns() throws InterruptedException {
        server.enqueue(jsonResponse(MOCK_RESPONSE_BODY));

        Map<Column, Object> enriched = enrich("type-success", "AmazonEC2", "RunInstances", "AmazonEC2", 2.0);

        assertNotNull(enriched.get(SpruceColumn.ENERGY_USED));
        assertNotNull(enriched.get(SpruceColumn.EMBODIED_EMISSIONS));
        assertNotNull(enriched.get(SpruceColumn.EMBODIED_ADP));

        RecordedRequest recorded = server.takeRequest();
        assertTrue(recorded.getPath().contains("instance_type=type-success"));
        assertTrue(recorded.getPath().contains("provider=aws"));
    }

    @Test
    void cacheAvoidsSecondHttpCallForSameInstanceType() {
        // Only one response enqueued — a second HTTP call would fail the assertion below.
        server.enqueue(jsonResponse(MOCK_RESPONSE_BODY));

        enrich("type-cache", "AmazonEC2", "RunInstances", "AmazonEC2", 1.0);
        Map<Column, Object> second = enrich("type-cache", "AmazonEC2", "RunInstances", "AmazonEC2", 4.0);

        assertEquals(1, server.getRequestCount(),
                "Second enrich for the same instance type should hit the cache");
        assertNotNull(second.get(SpruceColumn.ENERGY_USED));
    }

    @Test
    void notFoundResponseShortCircuitsSubsequentLookups() {
        // Only one response enqueued — the second call must be short-circuited by the
        // unknown-instance-type cache in AbstractBoaviztaModule.
        server.enqueue(new MockResponse().setResponseCode(404).setStatus("HTTP/1.1 404 Not Found"));

        Map<Column, Object> first = enrich("type-notfound", "AmazonEC2", "RunInstances", "AmazonEC2", 1.0);
        Map<Column, Object> second = enrich("type-notfound", "AmazonEC2", "RunInstances", "AmazonEC2", 1.0);

        assertTrue(first.isEmpty());
        assertTrue(second.isEmpty());
        assertEquals(1, server.getRequestCount());
    }

    @Test
    void serverErrorIsSwallowedAndProducesNoEnrichment() {
        server.enqueue(new MockResponse().setResponseCode(500));

        Map<Column, Object> enriched = enrich("type-error", "AmazonEC2", "RunInstances", "AmazonEC2", 1.0);

        assertTrue(enriched.isEmpty(),
                "5xx errors should not propagate out of enrich() and should not write columns");
    }

    private Map<Column, Object> enrich(String instanceType, String serviceCode, String operation,
                                        String productCode, double usage) {
        Object[] values = new Object[]{
                instanceType, serviceCode, operation, productCode, usage,
                null, null, null
        };
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(row, enriched);
        return enriched;
    }

    private static MockResponse jsonResponse(String body) {
        return new MockResponse()
                .setBody(body)
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json");
    }

    private static final String MOCK_RESPONSE_BODY = """
            {
                "impacts": {
                    "gwp": {
                          "unit": "kgCO2eq",
                          "embedded": {
                            "value": 0.0086
                          }
                    },
                    "fe": {
                        "use": {
                            "value": 15.5,
                            "unit": "MJ"
                        },
                        "embedded": "not implemented"
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
