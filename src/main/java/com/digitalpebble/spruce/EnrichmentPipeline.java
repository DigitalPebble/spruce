// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_TYPE;

/** Wraps the execution of the Enrichment Modules. There are as many instances of EnrichmentPipeline as there are partitions in the data. **/
public class EnrichmentPipeline implements MapPartitionsFunction<Row, Row> {

    private final List<EnrichmentModule> transformers;

    /** Initialises the modules **/
    public EnrichmentPipeline(List<EnrichmentModule> modules, Map<String, String> params) {
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
        final String item_type = LINE_ITEM_TYPE.getString(row);
        // can be Usage (for on demand resources), SavingsPlanCoveredUsage or DiscountedUsage
        return item_type != null && item_type.endsWith("Usage");
    }
}
