// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.types.DataType;

import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Defines columns added by the EnrichmentModules **/
public class SpruceColumn extends Column {

    public static SpruceColumn ENERGY_USED = new SpruceColumn("operational_energy_kwh", DoubleType);
    public static SpruceColumn CARBON_INTENSITY = new SpruceColumn("carbon_intensity", DoubleType);
    public static SpruceColumn OPERATIONAL_EMISSIONS = new SpruceColumn("operational_emissions_co2eq_g", DoubleType);
    public static SpruceColumn EMBODIED_EMISSIONS = new SpruceColumn("embodied_emissions_co2eq_g", DoubleType);
    public static SpruceColumn EMBODIED_ADP = new SpruceColumn("embodied_adp_sbeq_g", DoubleType);
    public static SpruceColumn CPU_LOAD = new SpruceColumn("cpu_load_percentage", DoubleType);
    public static SpruceColumn PUE = new SpruceColumn("power_usage_effectiveness", DoubleType);
    public static SpruceColumn REGION = new SpruceColumn("region", StringType);

    private SpruceColumn(String l, DataType t) {
        super(l, t);
    }

    /** Returns the String value for this column from the enriched values map, or null if absent. */
    public String getString(Map<Column, Object> enrichedValues) {
        return (String) enrichedValues.get(this);
    }

    /** Returns the Double value for this column from the enriched values map, or null if absent. */
    public Double getDouble(Map<Column, Object> enrichedValues) {
        return (Double) enrichedValues.get(this);
    }

}
