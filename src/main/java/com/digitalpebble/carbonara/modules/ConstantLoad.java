// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.carbonara.modules;

import com.digitalpebble.carbonara.CarbonaraColumn;
import com.digitalpebble.carbonara.Column;
import com.digitalpebble.carbonara.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

/** Populate the field CPU_load with a constant value (50 by default) as percentage of the CPU load **/
public class ConstantLoad implements EnrichmentModule {

    private double load_value = 50d;

    @Override
    public void init(Map<String, String> params) {
        // TODO set a different value via configuration
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{CarbonaraColumn.CPU_LOAD};
    }

    @Override
    public Row process(Row row) {
        return EnrichmentModule.withUpdatedValue(row, CarbonaraColumn.CPU_LOAD, load_value);
    }
}
