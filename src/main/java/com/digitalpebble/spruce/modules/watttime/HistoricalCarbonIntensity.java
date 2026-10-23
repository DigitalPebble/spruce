// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.watttime;

import com.digitalpebble.spruce.AzureColumn;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
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

    private static final Logger log = LoggerFactory.getLogger(HistoricalCarbonIntensity.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OkHttpClient CLIENT = new OkHttpClient();

    // shared by all instances of this module in the JVM, so partitions reuse what was already fetched
    // WattTime region keyed by "provider:region" e.g. "aws:us-west-1", empty outside the coverage of WattTime
    private static final Map<String, Optional<String>> WATTTIME_REGIONS = new ConcurrentHashMap<>();
    // intensities in gCO2e/kWh by point time, keyed by WattTime region and month
    private static final Map<String, NavigableMap<Instant, Double>> INTENSITIES = new ConcurrentHashMap<>();

    String url = DEFAULT_URL;
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
        // the figures are fetched month by month, a row spanning two months keeps its previous value
        YearMonth month = YearMonth.from(start.atZone(ZoneOffset.UTC));
        if (end.isAfter(startOf(month.plusMonths(1)))) {
            return;
        }

        Optional<String> wattTimeRegion = WATTTIME_REGIONS.computeIfAbsent(provider.csvKey + ":" + region, k -> wattTimeRegion(region));
        if (wattTimeRegion.isEmpty()) {
            return;
        }
        NavigableMap<Instant, Double> intensities = INTENSITIES.computeIfAbsent(wattTimeRegion.get() + ":" + month,
                k -> fetchIntensities(wattTimeRegion.get(), month));

        Double intensity = average(intensities, start, end);
        if (intensity != null) {
            enrichedValues.put(CARBON_INTENSITY, intensity);
        }
    }

    private static Instant startOf(YearMonth month) {
        return month.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
    }

    /** The period [start, end) a line item was incurred in. */
    record UsagePeriod(Instant start, Instant end) {
    }

    /**
     * Returns the period the line item was incurred in, read from the FOCUS columns, else from the
     * CUR ones, else from the Azure date, which covers the whole day in UTC, or null if the row has
     * no usable date or ends before it starts. Native CUR reports have the FOCUS columns too, added
     * empty by SparkJob.
     */
    static UsagePeriod usagePeriod(Row row) {
        Instant start = FOCUSColumn.CHARGE_PERIOD_START.getInstant(row);
        Instant end = FOCUSColumn.CHARGE_PERIOD_END.getInstant(row);
        if (start == null || end == null) {
            start = CURColumn.LINE_ITEM_USAGE_START_DATE.getInstant(row);
            end = CURColumn.LINE_ITEM_USAGE_END_DATE.getInstant(row);
        }
        if (start == null || end == null) {
            // Azure reports carry the day only
            start = AzureColumn.DATE.getInstant(row);
            end = start == null ? null : start.plus(Duration.ofDays(1));
        }
        if (start == null || end == null || !end.isAfter(start)) {
            return null;
        }
        return new UsagePeriod(start, end);
    }

    /** Returns the mean of the intensities whose time falls in [start, end), or null if there are none. */
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

    /**
     * Returns the WattTime region serving the location of the given cloud region, if
     * cloud_regions.json has its coordinates and WattTime covers them.
     */
    private Optional<String> wattTimeRegion(String region) {
        JsonNode location = cloudRegions.path(provider.csvKey).path("cloud_regions").path(region);
        if (!location.hasNonNull("latitude") || !location.hasNonNull("longitude")) {
            return Optional.empty();
        }
        HttpUrl endpoint = HttpUrl.get(url + "/v3/region-from-loc").newBuilder()
                .addQueryParameter("latitude", location.path("latitude").asText())
                .addQueryParameter("longitude", location.path("longitude").asText())
                .addQueryParameter("signal_type", SIGNAL_TYPE)
                .build();
        try {
            return Optional.of(get(endpoint).get("region").asText());
        } catch (WattTimeException e) {
            if (e.code == 404) {
                return Optional.empty();
            }
            throw e;
        }
    }

    /**
     * Returns the intensities published for the given WattTime region over the given month, none
     * if the account has no access to them.
     */
    private NavigableMap<Instant, Double> fetchIntensities(String wattTimeRegion, YearMonth month) {
        HttpUrl endpoint = HttpUrl.get(url + "/v3/historical").newBuilder()
                .addQueryParameter("region", wattTimeRegion)
                .addQueryParameter("signal_type", SIGNAL_TYPE)
                .addQueryParameter("start", startOf(month).toString())
                .addQueryParameter("end", startOf(month.plusMonths(1)).toString())
                .build();
        NavigableMap<Instant, Double> intensities = new TreeMap<>();
        JsonNode body;
        try {
            body = get(endpoint);
        } catch (WattTimeException e) {
            if (e.code != 403) {
                throw e;
            }
            log.warn("The WattTime account has no access to the figures of {} for {}", wattTimeRegion, month);
            return intensities;
        }
        for (JsonNode point : body.get("data")) {
            intensities.put(OffsetDateTime.parse(point.get("point_time").asText()).toInstant(),
                    point.get("value").asDouble() * LBS_PER_MWH_TO_G_PER_KWH);
        }
        return intensities;
    }

    private JsonNode get(HttpUrl endpoint) {
        if (token == null) {
            token = login();
        }
        try {
            return call(authorized(endpoint));
        } catch (WattTimeException e) {
            // the login token has expired
            if (e.code != 401) {
                throw e;
            }
            token = login();
            return call(authorized(endpoint));
        }
    }

    private Request authorized(HttpUrl endpoint) {
        return new Request.Builder().url(endpoint).header("Authorization", "Bearer " + token).build();
    }

    private String login() {
        Request request = new Request.Builder().url(url + "/login")
                .header("Authorization", Credentials.basic(username, password))
                .build();
        try {
            return call(request).get("token").asText();
        } catch (WattTimeException e) {
            // a distinct exception, so that the callers do not take it for missing figures
            throw new IllegalStateException("Could not log in to WattTime with the credentials in "
                    + USERNAME_VARIABLE + " and " + PASSWORD_VARIABLE, e);
        }
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
