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
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        Double energyUsed = ENERGY_USED.getDouble(enrichedValues);
        if (energyUsed == null) return;

        Double carbon_intensity = CARBON_INTENSITY.getDouble(enrichedValues);
        if (carbon_intensity == null) return;

        // take into account the PUE if present
        Double pueVal = PUE.getDouble(enrichedValues);
        final double pue = pueVal != null ? pueVal : 1.0;
        final double emissions = energyUsed * carbon_intensity * pue;

        enrichedValues.put(OPERATIONAL_EMISSIONS, emissions);
    }
}
