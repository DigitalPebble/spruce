// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.watttime;

import com.digitalpebble.spruce.AzureColumn;
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
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
        module.init(Map.of(), Provider.AWS);
        module.url = "http://localhost:" + server.getPort();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    /**
     * Answers like WattTime would for a cloud region located in the given WattTime region, or
     * outside the coverage of WattTime if the region is null. Null historical data means that the
     * account has no access to the figures of the WattTime region.
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
                    case "/v3/historical" -> !"co2_aoer".equals(request.getRequestUrl().queryParameter("signal_type"))
                            ? new MockResponse().setResponseCode(400)
                            : historicalData == null
                            ? json("{\"error\":\"INVALID_SCOPE\"}").setResponseCode(403)
                            : json("{\"data\":[" + historicalData + "],\"meta\":{}}");
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

    /**
     * Returns a point for each hour of the day, 1000 lbs/MWh in the morning and 3000 in the
     * afternoon, between points of 9000 the hour before and the hour after the day.
     */
    private static String hourlyPoints(LocalDate day) {
        List<String> points = new ArrayList<>();
        points.add(point(day.minusDays(1) + "T23:00:00Z", 9000));
        for (int hour = 0; hour < 24; hour++) {
            points.add(point(String.format("%sT%02d:00:00Z", day, hour), hour < 12 ? 1000 : 3000));
        }
        points.add(point(day.plusDays(1) + "T00:00:00Z", 9000));
        return String.join(",", points);
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
        serve("REGION_DAILY", hourlyPoints(LocalDate.of(2025, 2, 10)));

        Map<Column, Object> enriched = enrich(curRow("2025-02-10T00:00:00Z", "2025-02-11T00:00:00Z"), "us-east-1", 300.0);

        // 2000 lbs/MWh on average over the day, the points of the day before and after are left out
        assertEquals(907.184, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
    }

    @Test
    void keepsThePreviousIntensityOutsideTheCoverageOfWattTime() {
        serve(null, "");

        Map<Column, Object> enriched = enrich(curRow("2025-03-10T10:00:00Z", "2025-03-10T11:00:00Z"), "eu-west-1", 300.0);

        assertEquals(300.0, (Double) enriched.get(CARBON_INTENSITY));
    }

    @Test
    void keepsThePreviousIntensityWhenTheAccountHasNoAccessToTheFigures() {
        serve("REGION_NO_ACCESS", null);

        Map<Column, Object> enriched = enrich(curRow("2025-03-10T10:00:00Z", "2025-03-10T11:00:00Z"), "ap-northeast-1", 300.0);

        assertEquals(300.0, (Double) enriched.get(CARBON_INTENSITY));
    }

    @Test
    void keepsThePreviousIntensityOfRegionsWithoutCoordinates() {
        serve("REGION_UNKNOWN", point("2025-03-10T10:00:00Z", 1000));

        Map<Column, Object> enriched = enrich(curRow("2025-03-10T10:00:00Z", "2025-03-10T11:00:00Z"), "global", 300.0);

        assertEquals(300.0, (Double) enriched.get(CARBON_INTENSITY));
    }

    @Test
    void readsTheUsagePeriodOfFocusRows() {
        serve("REGION_FOCUS", point("2025-04-10T10:00:00Z", 1000));
        Row row = new GenericRowWithSchema(new Object[]{"2025-04-10T10:00:00Z", "2025-04-10T11:00:00Z"}, FOCUS_SCHEMA);

        Map<Column, Object> enriched = enrich(row, "ap-southeast-2", 300.0);

        assertEquals(453.592, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
    }

    @Test
    void averagesTheDayOfAzureRows() {
        module.init(Map.of(), Provider.AZURE);
        serve("REGION_AZURE", hourlyPoints(LocalDate.of(2025, 6, 12)));
        StructType schema = new StructType(new StructField[]{
                StructField.apply(AzureColumn.DATE.getLabel(), DataTypes.DateType, true, null)});
        Row row = new GenericRowWithSchema(new Object[]{Date.valueOf("2025-06-12")}, schema);

        Map<Column, Object> enriched = enrich(row, "westeurope", 300.0);

        // the row covers the whole day in UTC: 2000 lbs/MWh on average
        assertEquals(907.184, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
    }

    @Test
    void readsTheDayOfAzureCsvExports() {
        module.init(Map.of(), Provider.AZURE);
        serve("REGION_AZURE_CSV", hourlyPoints(LocalDate.of(2025, 6, 13)));
        StructType schema = new StructType(new StructField[]{
                StructField.apply(AzureColumn.DATE.getLabel(), DataTypes.StringType, true, null)});
        // the CSV exports carry US-formatted dates that Spark leaves as strings
        Row row = new GenericRowWithSchema(new Object[]{"06/13/2025"}, schema);

        Map<Column, Object> enriched = enrich(row, "northeurope", 300.0);

        assertEquals(907.184, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
    }

    @Test
    void keepsThePreviousIntensityWhenTheDatesCannotBeRead() {
        serve("REGION_UNREAD", point("2025-01-15T10:00:00Z", 1000));

        Map<Column, Object> enriched = enrich(curRow("01/15/2025 10:00", "01/15/2025 11:00"), "us-west-2", 300.0);

        assertEquals(300.0, (Double) enriched.get(CARBON_INTENSITY));
    }

    @Test
    void keepsThePreviousIntensityWhenTheUsageEndsBeforeItStarts() {
        serve("REGION_REVERSED", point("2025-03-10T10:00:00Z", 1000));

        Map<Column, Object> enriched = enrich(curRow("2025-03-10T11:00:00Z", "2025-03-10T10:00:00Z"), "eu-west-2", 300.0);

        assertEquals(300.0, (Double) enriched.get(CARBON_INTENSITY));
    }

    @Test
    void keepsThePreviousIntensityOfRowsSpanningTwoMonths() {
        serve("REGION_SPANNING", point("2025-08-31T12:00:00Z", 1000));

        Map<Column, Object> enriched = enrich(curRow("2025-08-31T12:00:00Z", "2025-09-01T12:00:00Z"), "sa-east-1", 300.0);

        assertEquals(300.0, (Double) enriched.get(CARBON_INTENSITY));
    }

    @Test
    void logsInAgainWhenTheTokenHasExpired() {
        AtomicInteger logins = new AtomicInteger();
        server.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                return switch (request.getRequestUrl().encodedPath()) {
                    case "/login" -> json("{\"token\":\"token" + logins.incrementAndGet() + "\"}");
                    case "/v3/region-from-loc" -> json("{\"region\":\"REGION_EXPIRED\"}");
                    // the first token has expired by the time the figures are requested
                    case "/v3/historical" -> "Bearer token2".equals(request.getHeader("Authorization"))
                            ? json("{\"data\":[" + point("2025-07-01T10:00:00Z", 1000) + "],\"meta\":{}}")
                            : new MockResponse().setResponseCode(401);
                    default -> new MockResponse().setResponseCode(404);
                };
            }
        });

        Map<Column, Object> enriched = enrich(curRow("2025-07-01T10:00:00Z", "2025-07-01T11:00:00Z"), "ca-central-1", 300.0);

        assertEquals(453.592, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
        assertEquals(2, logins.get());
    }

    @Test
    void readsTheUsagePeriodFromTimestamps() {
        serve("REGION_PARQUET", point("2025-05-20T10:00:00Z", 1000));
        StructType schema = new StructType(new StructField[]{
                StructField.apply(CURColumn.LINE_ITEM_USAGE_START_DATE.getLabel(), DataTypes.TimestampType, true, null),
                StructField.apply(CURColumn.LINE_ITEM_USAGE_END_DATE.getLabel(), DataTypes.TimestampType, true, null)});
        // reports in Parquet carry the dates as Spark timestamps
        Row row = new GenericRowWithSchema(new Object[]{
                Timestamp.from(Instant.parse("2025-05-20T10:00:00Z")),
                Timestamp.from(Instant.parse("2025-05-20T11:00:00Z"))}, schema);

        Map<Column, Object> enriched = enrich(row, "eu-central-1", 300.0);

        assertEquals(453.592, (Double) enriched.get(CARBON_INTENSITY), 1e-9);
    }
}
