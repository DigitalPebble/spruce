// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.carbonara;

import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;

/** Defines columns added by the EnrichmentModules **/
public class CarbonaraColumn extends Column {

    public static Column ENERGY_USED = new CarbonaraColumn("energy_usage_kwh", DoubleType);
    public static Column CARBON_INTENSITY = new CarbonaraColumn("carbon_intensity", DoubleType);
    public static Column OPERATIONAL_EMISSIONS = new CarbonaraColumn("operational_emissions_co2eq_g", DoubleType);
    public static Column EMBODIED_EMISSIONS = new CarbonaraColumn("embodied_emissions_co2eq_g", DoubleType);
    public static Column CPU_LOAD = new CarbonaraColumn("cpu_load_percentage", DoubleType);

    private CarbonaraColumn(String l, DataType t) {
        super(l, t);
    }

}
