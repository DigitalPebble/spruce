// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ember;

import com.digitalpebble.spruce.Column;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Populate the CARBON_INTENSITY field using Ember's cloud region carbon intensity data.
 */
public class AverageCarbonIntensity extends AbstractEmberCarbonIntensity {

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED, REGION};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (!enrichedValues.containsKey(ENERGY_USED)) {
            return;
        }

        String locationCode = REGION.getString(enrichedValues);
        if (locationCode == null) {
            return;
        }

        Double coeff = getIntensity(getProvider(), locationCode);
        if (coeff == null) {
            return;
        }
        enrichedValues.put(CARBON_INTENSITY, coeff);
    }
}
