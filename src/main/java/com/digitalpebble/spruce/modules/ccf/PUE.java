// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static com.digitalpebble.spruce.SpruceColumn.PUE;


/**
 * Applies a constant PUE factor
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology/#power-usage-effectiveness">CCF methodology</a>
 **/
public class PUE implements EnrichmentModule {

    final double aws_pue = 1.135;

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{PUE};
    }

    @Override
    public Row process(Row row) {
        // apply only to rows corresponding for which energy usage
        // exists and has been estimated
        if (ENERGY_USED.isNullAt(row)) {
            return row;
        }
        double energyUsed = ENERGY_USED.getDouble(row);
        if (energyUsed <= 0) return row;

        return EnrichmentModule.withUpdatedValue(row, PUE, aws_pue);
    }
}
