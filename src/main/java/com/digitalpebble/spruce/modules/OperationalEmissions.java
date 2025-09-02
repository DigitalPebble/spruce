// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

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
    public Row process(Row row) {
        if (ENERGY_USED.isNullAt(row)) {
            return row;
        }

        if (CARBON_INTENSITY.isNullAt(row)) {
            return row;
        }

        final double energyUsed = ENERGY_USED.getDouble(row);

        // take into account the PUE if present
        final double pue = PUE.isNullAt(row) ? 1.0 : PUE.getDouble(row);
        final double carbon_intensity = CARBON_INTENSITY.getDouble(row);
        final double emissions = energyUsed * carbon_intensity * pue;

        return EnrichmentModule.withUpdatedValue(row, OPERATIONAL_EMISSIONS, emissions);
    }
}
