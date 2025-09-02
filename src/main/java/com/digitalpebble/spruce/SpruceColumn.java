// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;

/** Defines columns added by the EnrichmentModules **/
public class SpruceColumn extends Column {

    public static Column ENERGY_USED = new SpruceColumn("energy_usage_kwh", DoubleType);
    public static Column CARBON_INTENSITY = new SpruceColumn("carbon_intensity", DoubleType);
    public static Column OPERATIONAL_EMISSIONS = new SpruceColumn("operational_emissions_co2eq_g", DoubleType);
    public static Column EMBODIED_EMISSIONS = new SpruceColumn("embodied_emissions_co2eq_g", DoubleType);
    public static Column CPU_LOAD = new SpruceColumn("cpu_load_percentage", DoubleType);
    public static Column PUE = new SpruceColumn("power_usage_effectiveness", DoubleType);

    private SpruceColumn(String l, DataType t) {
        super(l, t);
    }

}
