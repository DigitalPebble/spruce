/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara;

import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;

/** Defines columns added by the EnrichmentModules **/
public class CarbonaraColumn extends Column {

    public static CarbonaraColumn ENERGY_USED = new CarbonaraColumn("energy_usage_kwh", DoubleType);
    public static CarbonaraColumn CARBON_INTENSITY = new CarbonaraColumn("carbon_intensity", DoubleType);
    public static CarbonaraColumn OPERATIONAL_EMISSIONS = new CarbonaraColumn("operational_emissions_co2eq_g", DoubleType);
    public static CarbonaraColumn EMBODIED_EMISSIONS = new CarbonaraColumn("embodied_emissions_co2eq_g", DoubleType);

    private CarbonaraColumn(String l, DataType t) {
        super(l, t);
    }

}
