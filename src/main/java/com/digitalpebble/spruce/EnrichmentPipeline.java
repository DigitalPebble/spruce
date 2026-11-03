// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.util.TaskCompletionListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_TYPE;

/** Wraps the execution of the Enrichment Modules. There are as many instances of EnrichmentPipeline as there are partitions in the data. **/
public class EnrichmentPipeline implements MapPartitionsFunction<Row, Row> {

    private transient List<EnrichmentModule> enrichmentModules;

    private final Config config;

    /** Optional per-module timing, null unless the job was asked to report metrics. **/
    private final ModuleTimings timings;

    /** Initialises the modules **/
    public EnrichmentPipeline(Config config) {
        this(config, null);
    }

    /**
     * Initialises the modules and, when {@code timings} is not null, wraps each of them so that
     * the time they spend enriching rows is measured and reported.
     **/
    public EnrichmentPipeline(Config config, ModuleTimings timings) {
        this.config = config;
        this.timings = timings;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) {

        if (enrichmentModules == null) {
            enrichmentModules = config.configureModules();
        }

        // rows enriched in this partition; an array so the completion listener can read it
        final long[] rowsEnriched = new long[1];

        final List<EnrichmentModule> modules;
        if (timings == null) {
            modules = enrichmentModules;
        } else {
            // accumulate into a plain array and flush it once, rather than touching the
            // accumulators on every module of every row
            final long[] nanos = new long[enrichmentModules.size()];
            List<EnrichmentModule> timed = new ArrayList<>(enrichmentModules.size());
            for (int i = 0; i < enrichmentModules.size(); i++) {
                timed.add(new TimedModule(enrichmentModules.get(i), nanos, i));
            }
            modules = timed;
            // the iterator is lazy and a task can be killed part way through a partition, so the
            // end of the input is not a reliable place to flush; task completion is
            TaskContext context = TaskContext.get();
            if (context != null) {
                context.addTaskCompletionListener(
                        (TaskCompletionListener) ctx -> timings.add(nanos, rowsEnriched[0]));
            }
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

                Map<Column, Object> enriched = new HashMap<>();
                for (EnrichmentModule module : modules) {
                    module.enrich(row, enriched);
                }
                rowsEnriched[0]++;

                // Materialise the final row — single copy instead of one per module
                Object[] values = new Object[row.size()];
                for (int i = 0; i < row.size(); i++) {
                    values[i] = row.get(i);
                }
                for (Map.Entry<Column, Object> entry : enriched.entrySet()) {
                    values[entry.getKey().resolveIndex(row)] = entry.getValue();
                }
                return new GenericRowWithSchema(values, row.schema());
            }
        };
    }

    /**
     * Returns true if the line item corresponds to a usage, false otherwise
     **/
    private boolean usageFilter(Row row) {
        if (config.getReportFormat() == ReportFormat.FOCUS) {
            // standard FOCUS values: Usage, Purchase, Tax, Credit, Adjustment
            String charge_category = FOCUSColumn.CHARGE_CATEGORY.getString(row);
            return "Usage".equals(charge_category);
        }
        if (config.getProvider().equals(Provider.AWS)) {
            String item_type = LINE_ITEM_TYPE.getString(row);
            // can be Usage (for on demand resources), SavingsPlanCoveredUsage or DiscountedUsage
            return item_type != null && item_type.endsWith("Usage");
        } else if (config.getProvider().equals(Provider.AZURE)) {
            String charge_type = AzureColumn.CHARGE_TYPE.getString(row);
            return charge_type != null && charge_type.endsWith("Usage");
        }
        return false;
    }
}
