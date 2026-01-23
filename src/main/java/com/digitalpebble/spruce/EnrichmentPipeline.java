// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.Iterator;
import java.util.List;

import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_TYPE;

/** Wraps the execution of the Enrichment Modules. There are as many instances of EnrichmentPipeline as there are partitions in the data. **/
public class EnrichmentPipeline implements MapPartitionsFunction<Row, Row> {

    private transient List<EnrichmentModule> enrichmentModules;

    private final Config config;

    /** Initialises the modules **/
    public EnrichmentPipeline(Config config) {
        this.config = config;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) {

        if (enrichmentModules == null) {
            enrichmentModules = config.configureModules();
        }

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
                for (EnrichmentModule module : enrichmentModules) {
                    row = module.process(row);
                }
                return row;
            }
        };
    }

    /** Returns true if the line item corresponds to a usage, false otherwise **/
    private boolean usageFilter (Row row){
        String item_type = LINE_ITEM_TYPE.getString(row, true);
        if (item_type == null) {
            // try Azure
            item_type = AzureColumn.CHARGE_TYPE.getString(row);
        }
        // can be Usage (for on demand resources), SavingsPlanCoveredUsage or DiscountedUsage
        return item_type != null && item_type.endsWith("Usage");
    }
}
