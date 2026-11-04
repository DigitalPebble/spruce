// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.Utils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.spark.sql.Row;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Provider-neutral skeleton for enrichment modules backed by Boavizta data.
 *
 * <p>Subclasses provide:
 * <ul>
 *   <li>{@link #extractInstanceType(Row)} — how to read and normalise the instance type from the
 *       provider's billing data (one implementation per cloud provider);</li>
 *   <li>{@link #lookupImpacts(String)} — how to retrieve per-instance impacts (the static-CSV
 *       and live-API variants).</li>
 * </ul>
 *
 * <p>This class owns the bits that are the same regardless of provider or lookup mode:
 * the output columns, the unknown-instance-type cache, and the multiplication of impacts by
 * the row's usage amount.
 */
public abstract class AbstractBoaviztaModule implements EnrichmentModule {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBoaviztaModule.class);
    /**
     * Impacts parsed from the bundled CSVs, keyed by resource location. A bundled resource cannot
     * change while the JVM runs, so each file is parsed once and the table shared by every module
     * instance the Spark tasks of that executor create. Read-only once built.
     */
    private static final ConcurrentHashMap<String, Map<String, Impacts>> STATIC_IMPACTS =
            new ConcurrentHashMap<>();
    /**
     * Instance types we have already failed to resolve, to avoid repeated lookups.
     */
    protected final Set<String> unknownInstanceTypes = ConcurrentHashMap.newKeySet();
    /**
     * The table this module looks up, shared with every other instance loading the same file, and
     * empty in the variants that query the API instead.
     */
    protected Map<String, Impacts> staticImpacts = Map.of();
    /**
     * Cached impacts map for efficiency
     */
    protected Cache<String, @Nullable Impacts> cache = Caffeine.newBuilder().build();
    /**
     * Active cloud provider for lookups. Subclasses may seed a sensible default.
     */
    protected Provider provider;

    @Override
    public void init(Map<String, Object> params, Provider provider) {
        if (provider != null) {
            this.provider = provider;
        }
        // Initialize cache if not already done
        if (cache == null) {
            cache = Caffeine.newBuilder().build();
        }
        init(params);
    }

    @Override
    public final Column[] columnsAdded() {
        return new Column[]{ENERGY_USED, EMBODIED_EMISSIONS, EMBODIED_ADP};
    }

    /**
     * Loads impacts data from a resource file, or reuses the table another module instance in this
     * JVM has already parsed from it. Called by the subclasses backed by a bundled CSV.
     *
     * @param resourceLocation The location of the CSV resource file
     */
    public void loadImpactsFromStaticFile(String resourceLocation) {
        staticImpacts = STATIC_IMPACTS.computeIfAbsent(
                resourceLocation, AbstractBoaviztaModule::parseStaticFile);
    }

    /**
     * Parses one impacts CSV, each line holding a comma separated instance type, usage energy,
     * embodied emissions and adp.
     */
    private static Map<String, Impacts> parseStaticFile(String resourceLocation) {
        final List<String> estimates;
        try {
            estimates = Utils.loadLinesResources(resourceLocation);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final Map<String, Impacts> impacts = new HashMap<>();
        for (String line : estimates) {
            if (line.startsWith("#") || line.trim().isEmpty()) {
                continue;
            }
            String[] parts = line.split(",");
            if (parts.length != 4) {
                throw new RuntimeException("Invalid estimates mapping line: " + line);
            }
            impacts.put(parts[0].trim(), new Impacts(
                    Double.parseDouble(parts[1].trim()),
                    Double.parseDouble(parts[2].trim()),
                    Double.parseDouble(parts[3].trim())));
        }
        return Map.copyOf(impacts);
    }

    /**
     * Returns the normalised instance type to look up, or {@code null} if the row is not relevant
     * to this module (e.g. wrong service, unrelated operation).
     */
    protected abstract String extractInstanceType(Row row);

    /**
     * Returns the per-instance impacts for {@code instanceType}, or {@code null} if the type is
     * unknown to this lookup.
     */
    protected abstract Impacts lookupImpacts(String instanceType);

    /**
     * Return the usage amount based on the provider's specific field
     */
    protected abstract double getUsageAmount(Row row);

    @Override
    public final void enrich(Row row, Map<Column, Object> enrichedValues) {
        String instanceType = extractInstanceType(row);
        if (instanceType == null) {
            return;
        }
        // the static variants hold the whole table from the start, so this is their only lookup
        Impacts impacts = staticImpacts.get(instanceType);

        if (impacts == null) {
            if (unknownInstanceTypes.contains(instanceType)) {
                return;
            }
            // get from API
            impacts = cache.getIfPresent(instanceType);
            if (impacts == null) {
                impacts = lookupImpacts(instanceType);
                if (impacts != null) {
                    cache.put(instanceType, impacts);
                }
            }
            if (impacts == null) {
                LOG.info("Unknown instance type {}", instanceType);
                unknownInstanceTypes.add(instanceType);
                return;
            }
        }

        double amount = getUsageAmount(row);
        enrichedValues.put(ENERGY_USED, impacts.getFinalEnergyKWh() * amount);
        enrichedValues.put(EMBODIED_EMISSIONS, impacts.getEmbeddedEmissionsGramsCO2eq() * amount);
        enrichedValues.put(EMBODIED_ADP, impacts.getAbioticDepletionPotentialGramsSbeq() * amount);
    }
}
