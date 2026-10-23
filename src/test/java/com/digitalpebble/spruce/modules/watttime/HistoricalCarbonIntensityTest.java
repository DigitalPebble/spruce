// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.watttime;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.Provider;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs the module against a mock of the WattTime API. Each test uses its own cloud region and
 * WattTime region so that it stays isolated from the cache shared across instances.
 */
class HistoricalCarbonIntensityTest {

    private static final StructType CUR_SCHEMA = new StructType(new StructField[]{
            StructField.apply(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(), DataTypes.StringType, true, null),
            StructField.apply(CURColumn.LINE_ITEM_USAGE_END_DATE.getLabel(), DataTypes.StringType, true, null)});

    private static final StructType FOCUS_SCHEMA = new StructType(new StructField[]{
            StructField.apply(FOCUSColumn.CHARGE_PERIOD_START.getLabel(), DataTypes.StringType, true, null),
            StructField.apply(FOCUSColumn.CHARGE_PERIOD_END.getLabel(), DataTypes.StringType, true, null)});

    private MockWebServer server;
    private HistoricalCarbonIntensity module;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start(0);
        module = new HistoricalCarbonIntensity() {
            @Override
            String getenv(String name) {
                return "test";
            }
        };
        module.init(Map.of("url", "http://localhost:" + server.getPort()), Provider.AWS);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    /**
     * Answers like WattTime would for a cloud region located in the given WattTime region, or
     * outside the coverage of WattTime if the region is null.
     */
    private void serve(String wattTimeRegion, String historicalData) {
        server.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                return switch (request.getRequestUrl().encodedPath()) {
                    case "/login" -> json("{\"token\":\"token\"}");
                    case "/v3/region-from-loc" -> wattTimeRegion == null
                            ? json("{\"error\":\"No coverage available for these coordinates and signal_type\"}").setResponseCode(404)
                            : json("{\"region\":\"" + wattTimeRegion
                            + "\",\"region_full_name\":\"Test\",\"signal_type\":\"co2_aoer\"}");
                    case "/v3/historical" -> "co2_aoer".equals(request.getRequestUrl().queryParameter("signal_type"))
                            ? json("{\"data\":[" + historicalData + "],\"meta\":{}}")
                            : new MockResponse().setResponseCode(400);
                    default -> new MockResponse().setResponseCode(404);
                };
            }
        });
    }

    private static MockResponse json(String body) {
        return new MockResponse().setHeader("Content-Type", "application/json").setBody(body);
    }

    private static String point(String time, double lbsPerMWh) {
        return "{\"point_time\":\"" + time + "\",\"value\":" + lbsPerMWh + "}";
    }

    private static Row curRow(String usageStart, String usageEnd) {
        return new GenericRowWithSchema(new Object[]{usageStart, usageEnd}, CUR_SCHEMA);
    }

    private Map<Column, Object> enrich(Row row, String region, Double previousIntensity) {
        Map<Column, Object> enriched = new HashMap<>();
        enriched.put(ENERGY_USED, 1.0);
        enriched.put(REGION, region);
        enriched.put(CARBON_INTENSITY, previousIntensity);
        module.enrich(row, enriched);
        return enriched;
    }

    @Test
    void setsTheIntensityOfTheHourInGramsPerKWh() {
        serve("REGION_HOURLY", point("2025-01-15T10:00:00Z", 1000));

        Map<Column, Object> enriched = enrich(curRow("2025-01-15T10:00:00Z", "2025-01-15T11:00:00Z"), "us-west-1", 300.0);

        // 1 lb = 453.592 g and 1 MWh = 1000 kWh
        assertEquals(453.592, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
    }

    @Test
    void averagesThePointsOfTheUsagePeriod() {
        List<String> points = new ArrayList<>();
        points.add(point("2025-02-09T23:00:00Z", 9000));
        for (int hour = 0; hour < 24; hour++) {
            points.add(point(String.format("2025-02-10T%02d:00:00Z", hour), hour < 12 ? 1000 : 3000));
        }
        points.add(point("2025-02-11T00:00:00Z", 9000));
        serve("REGION_DAILY", String.join(",", points));

        Map<Column, Object> enriched = enrich(curRow("2025-02-10T00:00:00Z", "2025-02-11T00:00:00Z"), "us-east-1", 300.0);

        // 1000 lbs/MWh in the morning and 3000 in the afternoon, 2000 on average over the day;
        // the points of the day before and after are left out
        assertEquals(907.184, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
    }

    @Test
    void keepsThePreviousIntensityOutsideTheCoverageOfWattTime() {
        serve(null, "");

        Map<Column, Object> enriched = enrich(curRow("2025-03-10T10:00:00Z", "2025-03-10T11:00:00Z"), "eu-west-1", 300.0);

        assertEquals(300.0, (Double) enriched.get(CARBON_INTENSITY));
    }

    @Test
    void readsTheUsagePeriodOfFocusRows() {
        serve("REGION_FOCUS", point("2025-04-10T10:00:00Z", 1000));
        Row row = new GenericRowWithSchema(new Object[]{"2025-04-10T10:00:00Z", "2025-04-10T11:00:00Z"}, FOCUS_SCHEMA);

        Map<Column, Object> enriched = enrich(row, "ap-southeast-2", 300.0);

        assertEquals(453.592, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
    }
}
