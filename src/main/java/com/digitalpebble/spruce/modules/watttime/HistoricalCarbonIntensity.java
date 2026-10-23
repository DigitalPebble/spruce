// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.watttime;

import com.digitalpebble.spruce.CURColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.Provider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Populates the CARBON_INTENSITY field with the average operating emissions rate (AOER) that
 * the WattTime API publishes for the grid region of the cloud region, at the time the usage was
 * incurred. The data is fetched while the job runs, with the credentials of the user's WattTime
 * account, read from the environment variables WATTTIME_USERNAME and WATTTIME_PASSWORD.
 */
public class HistoricalCarbonIntensity implements EnrichmentModule {

    static final String USERNAME_VARIABLE = "WATTTIME_USERNAME";
    static final String PASSWORD_VARIABLE = "WATTTIME_PASSWORD";

    private static final String DEFAULT_URL = "https://api.watttime.org";
    private static final String SIGNAL_TYPE = "co2_aoer";

    // 1 lb = 453.592 g and 1 MWh = 1000 kWh
    private static final double LBS_PER_MWH_TO_G_PER_KWH = 0.453592;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OkHttpClient CLIENT = new OkHttpClient();

    // shared by the instances of the JVM so that each partition does not query the same data again
    // WattTime region keyed by "provider:region" e.g. "aws:us-west-1", empty outside the coverage of WattTime
    private static final Map<String, Optional<String>> WATTTIME_REGIONS = new ConcurrentHashMap<>();
    // intensities in gCO2/kWh by point time, keyed by WattTime region and month
    private static final Map<String, NavigableMap<Instant, Double>> INTENSITIES = new ConcurrentHashMap<>();

    private String url = DEFAULT_URL;
    private String username;
    private String password;
    private String token;
    private JsonNode cloudRegions;

    /** Set via {@link #init(Map, Provider)}, left null so that a lookup without it fails loudly. */
    private Provider provider;

    @Override
    public void init(Map<String, Object> params, Provider provider) {
        this.provider = provider;
        init(params);
    }

    @Override
    public void init(Map<String, Object> params) {
        String u = (String) params.get("url");
        if (u != null) {
            url = u;
        }
        username = getenv(USERNAME_VARIABLE);
        password = getenv(PASSWORD_VARIABLE);
        if (username == null || password == null) {
            throw new IllegalStateException("Set " + USERNAME_VARIABLE + " and " + PASSWORD_VARIABLE
                    + " to the credentials of a WattTime account");
        }
        try {
            cloudRegions = MAPPER.readTree(getClass().getClassLoader().getResource("cloud_regions.json"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    String getenv(String name) {
        return System.getenv(name);
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED, REGION};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{CARBON_INTENSITY};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (!enrichedValues.containsKey(ENERGY_USED)) {
            return;
        }

        String region = REGION.getString(enrichedValues);
        if (region == null) {
            return;
        }

        UsagePeriod period = usagePeriod(row);
        if (period == null) {
            return;
        }
        Instant start = period.start();
        Instant end = period.end();

        Optional<String> wattTimeRegion = WATTTIME_REGIONS.computeIfAbsent(provider.csvKey + ":" + region, k -> wattTimeRegion(region));
        if (wattTimeRegion.isEmpty()) {
            return;
        }
        YearMonth month = YearMonth.from(start.atZone(ZoneOffset.UTC));
        NavigableMap<Instant, Double> intensities = INTENSITIES.computeIfAbsent(wattTimeRegion.get() + ":" + month,
                k -> fetchIntensities(wattTimeRegion.get(), month));

        Double intensity = average(intensities, start, end);
        if (intensity != null) {
            enrichedValues.put(CARBON_INTENSITY, intensity);
        }
    }

    /** The period [start, end) a line item was incurred in. */
    record UsagePeriod(Instant start, Instant end) {
    }

    /**
     * Returns the period the line item was incurred in, read from the FOCUS columns, else from the
     * CUR ones, or null if the row has no usable date. The FOCUS columns come first: they exist in
     * native CUR reports too, but are still null when this module runs.
     */
    static UsagePeriod usagePeriod(Row row) {
        String start = FOCUSColumn.CHARGE_PERIOD_START.getString(row, true);
        String end = FOCUSColumn.CHARGE_PERIOD_END.getString(row, true);
        if (start == null || end == null) {
            start = CURColumn.LINE_ITEM_USAGE_START_DATE.getString(row, true);
            end = CURColumn.LINE_ITEM_USAGE_END_DATE.getString(row, true);
        }
        if (start == null || end == null) {
            return null;
        }
        return new UsagePeriod(OffsetDateTime.parse(start).toInstant(), OffsetDateTime.parse(end).toInstant());
    }

    /**
     * Returns the average of the intensities over the usage period [start, end), or null if
     * WattTime has no figure for that period. Each point is valid from its time until the next
     * point, e.g. for 5 minutes or an hour depending on the WattTime region.
     */
    static Double average(NavigableMap<Instant, Double> intensities, Instant start, Instant end) {
        NavigableMap<Instant, Double> period = intensities.subMap(start, true, end, false);
        if (period.isEmpty()) {
            return null;
        }
        double sum = 0;
        for (double value : period.values()) {
            sum += value;
        }
        return sum / period.size();
    }

    /** Returns the WattTime region serving the location of the given cloud region, if WattTime covers it. */
    private Optional<String> wattTimeRegion(String region) {
        JsonNode location = cloudRegions.path(provider.csvKey).path("cloud_regions").path(region);
        HttpUrl request = HttpUrl.get(url + "/v3/region-from-loc").newBuilder()
                .addQueryParameter("latitude", location.path("latitude").asText())
                .addQueryParameter("longitude", location.path("longitude").asText())
                .addQueryParameter("signal_type", SIGNAL_TYPE)
                .build();
        try {
            return Optional.of(get(request).get("region").asText());
        } catch (WattTimeException e) {
            if (e.code == 404) {
                return Optional.empty();
            }
            throw e;
        }
    }

    /** Returns the intensities published for the given WattTime region over the given month. */
    private NavigableMap<Instant, Double> fetchIntensities(String wattTimeRegion, YearMonth month) {
        HttpUrl request = HttpUrl.get(url + "/v3/historical").newBuilder()
                .addQueryParameter("region", wattTimeRegion)
                .addQueryParameter("signal_type", SIGNAL_TYPE)
                .addQueryParameter("start", month.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toString())
                .addQueryParameter("end", month.plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toString())
                .build();
        NavigableMap<Instant, Double> intensities = new TreeMap<>();
        for (JsonNode point : get(request).get("data")) {
            intensities.put(OffsetDateTime.parse(point.get("point_time").asText()).toInstant(),
                    point.get("value").asDouble() * LBS_PER_MWH_TO_G_PER_KWH);
        }
        return intensities;
    }

    private JsonNode get(HttpUrl request) {
        if (token == null) {
            token = login();
        }
        return call(new Request.Builder().url(request).header("Authorization", "Bearer " + token).build());
    }

    private String login() {
        Request request = new Request.Builder().url(url + "/login")
                .header("Authorization", Credentials.basic(username, password))
                .build();
        return call(request).get("token").asText();
    }

    private static JsonNode call(Request request) {
        try (Response response = CLIENT.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new WattTimeException(response.code(), request.url().encodedPath());
            }
            return MAPPER.readTree(response.body().string());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not reach the WattTime API at " + request.url().host(), e);
        }
    }

    /** Thrown when the WattTime API answers with an error status. */
    static class WattTimeException extends IllegalStateException {
        final int code;

        WattTimeException(int code, String path) {
            super("WattTime API returned HTTP " + code + " for " + path);
            this.code = code;
        }
    }
}
