// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import static com.digitalpebble.spruce.SpruceColumn.CPU_LOAD;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

/** Populate the field CPU_load with a constant value (50 by default) as percentage of the CPU load **/
public class ConstantLoad implements EnrichmentModule {

    private double load_value = 50d;

    @Override
    public void init(Map<String, Object> params) {
        Double value = (Double) params.get("load_value");
        if (value != null) {
            load_value = value;
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[0];
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{CPU_LOAD};
    }

    @Override
    public Row process(Row row) {
        return EnrichmentModule.withUpdatedValue(row, CPU_LOAD, load_value);
    }
}
