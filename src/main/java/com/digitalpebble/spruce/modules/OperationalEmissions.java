// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Populate the field OPERATIONAL_EMISSIONS
 * for rows where energy usage has been estimated, taking into account the PUE if present.
 **/
public class OperationalEmissions implements EnrichmentModule {

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED, CARBON_INTENSITY, PUE};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{OPERATIONAL_EMISSIONS};
    }

    @Override
    public void enrich(Row inputRow, Map<Column, Object> enrichedValues) {
        Object energyObj = enrichedValues.get(ENERGY_USED);
        if (energyObj == null) return;

        Object ciObj = enrichedValues.get(CARBON_INTENSITY);
        if (ciObj == null) return;

        final double energyUsed = (Double) energyObj;

        // take into account the PUE if present
        Object pueObj = enrichedValues.get(PUE);
        final double pue = pueObj != null ? (Double) pueObj : 1.0;
        final double carbon_intensity = (Double) ciObj;
        final double emissions = energyUsed * carbon_intensity * pue;

        enrichedValues.put(OPERATIONAL_EMISSIONS, emissions);
    }
}
