// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Provider;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.digitalpebble.spruce.CURColumn.USAGE_AMOUNT;
import static com.digitalpebble.spruce.SpruceColumn.EMBODIED_ADP;
import static com.digitalpebble.spruce.SpruceColumn.EMBODIED_EMISSIONS;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;

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

    /** Active cloud provider for lookups. Subclasses may seed a sensible default. */
    protected Provider provider;

    /** Instance types we have already failed to resolve, to avoid repeated lookups. */
    protected final Set<String> unknownInstanceTypes = ConcurrentHashMap.newKeySet();

    @Override
    public void init(Map<String, Object> params, Provider provider) {
        if (provider != null) {
            this.provider = provider;
        }
        init(params);
    }

    @Override
    public final Column[] columnsAdded() {
        return new Column[]{ENERGY_USED, EMBODIED_EMISSIONS, EMBODIED_ADP};
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

        Impacts impacts = lookupImpacts(instanceType);
        if (impacts == null) {
            LOG.info("Unknown instance type {}", instanceType);
            unknownInstanceTypes.add(instanceType);
            return;
        }

        double amount = USAGE_AMOUNT.getDouble(row);
        enrichedValues.put(ENERGY_USED, impacts.getFinalEnergyKWh() * amount);
        enrichedValues.put(EMBODIED_EMISSIONS, impacts.getEmbeddedEmissionsGramsCO2eq() * amount);
        enrichedValues.put(EMBODIED_ADP, impacts.getAbioticDepletionPotentialGramsSbeq() * amount);
    }
}
