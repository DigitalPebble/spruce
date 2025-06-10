/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.module;

import com.digitalpebble.Columns;
import com.digitalpebble.EnrichmentModule;
import org.apache.spark.sql.Row;


import static com.digitalpebble.Columns.ENERGY_USED;

public class DummyModule implements com.digitalpebble.EnrichmentModule {

    /** Returns the columns added by this module **/
    public Columns[] columnsAdded(){
        return new Columns[]{ENERGY_USED};
    }

    @Override
    public Row process(Row row) {
        // generate a random value ?
        double random = 0.01 + Math.random() * (1 - 0.01);
        return EnrichmentModule.withUpdatedValue(row, ENERGY_USED.getLabel(), random);
    }

}
