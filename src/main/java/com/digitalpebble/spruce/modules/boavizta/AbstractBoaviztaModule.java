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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.digitalpebble.spruce.CURColumn.USAGE_AMOUNT;
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
     * Instance types we have already failed to resolve, to avoid repeated lookups.
     */
    protected final Set<String> unknownInstanceTypes = ConcurrentHashMap.newKeySet();
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
     * Loads impacts data from a resource file. This method is designed to be called by subclasses
     * that need to load their specific CSV files. The values are then stored in the cache.
     *
     * @param resourceLocation The location of the CSV resource file
     */
    public void populateCacheFromStaticFile(String resourceLocation) {
        synchronized (AbstractBoaviztaModule.class) {
            if (cache == null) {
                cache = Caffeine.newBuilder().build();
            }
            try {
                // Load the resource file
                java.util.List<String> estimates = Utils.loadLinesResources(resourceLocation);
                // estimates consists of comma separated instance type, usage energy, embodied emissions, adp
                estimates.forEach(line -> {
                    if (line.startsWith("#") || line.trim().isEmpty()) {
                        return;
                    }
                    String[] parts = line.split(",");
                    if (parts.length == 4) {
                        String instanceType = parts[0].trim();
                        double energyUsed = Double.parseDouble(parts[1].trim());
                        double embodied = Double.parseDouble(parts[2].trim());
                        double adp = Double.parseDouble(parts[3].trim());
                        cache.put(instanceType, new Impacts(energyUsed, embodied, adp));
                    } else {
                        throw new RuntimeException("Invalid estimates mapping line: " + line);
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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

    @Override
    public final void enrich(Row row, Map<Column, Object> enrichedValues) {
        String instanceType = extractInstanceType(row);
        if (instanceType == null) {
            return;
        }
        if (unknownInstanceTypes.contains(instanceType)) {
            return;
        }

        // check in cache (static variants will have put them there from the start)
        Impacts impacts = cache.getIfPresent(instanceType);
        // get from API
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

        // TODO this is an AWS specific field - it has no place here
        double amount = USAGE_AMOUNT.getDouble(row);
        enrichedValues.put(ENERGY_USED, impacts.getFinalEnergyKWh() * amount);
        enrichedValues.put(EMBODIED_EMISSIONS, impacts.getEmbeddedEmissionsGramsCO2eq() * amount);
        enrichedValues.put(EMBODIED_ADP, impacts.getAbioticDepletionPotentialGramsSbeq() * amount);
    }
}
