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

package com.digitalpebble.carbonara;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EnrichementPipeline implements MapPartitionsFunction<Row, Row> {

    private final List<EnrichmentModule> transformers;

    /** Initialises the modules **/
    public EnrichementPipeline(List<EnrichmentModule> modules, Map<String, String> params) {
        this.transformers = modules;
        for (EnrichmentModule module : transformers) {
            module.init(params); // pass any parameters if needed
        }
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) {
        return new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public Row next() {
                Row row = input.next();
                // usage filter - only need to enrich entries that correspond to a usage (no tax, discount or fee)
                boolean usage = usageFilter(row);
                if (!usage) return row;
                for (EnrichmentModule transformer : transformers) {
                    row = transformer.process(row);
                }
                return row;
            }
        };
    }

    /** Returns true if the line item corresponds to a usage, false otherwise**/
    private boolean usageFilter (Row row){
        int index = row.fieldIndex(CURColumn.LINE_ITEM_TYPE.getLabel());
        if (index < 0){ return false; }
        String item_type = row.getString(index);
        // can be Usage (for on demand resources), SavingsPlanCoveredUsage or DiscountedUsage
        return item_type.endsWith("Usage");
    }
}
