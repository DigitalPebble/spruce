// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.SparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Collects the time spent in each enrichment module, using one named Spark accumulator per module
 * plus a count of the rows that went through the pipeline. The accumulators are registered on the
 * driver before the job runs, which is also what makes them visible in the Accumulators table of
 * the Spark UI, and are read back on the driver once the action has completed.
 *
 * <p>The values are a sum of wall time over every task on every executor, not an elapsed duration,
 * so they are meant for comparing modules against each other rather than against the runtime of
 * the job.
 *
 * <p>Accumulators updated from a transformation such as {@code mapPartitions} are not exactly
 * once: a speculative or retried task counts its time again, and so does re-reading a cached
 * Dataset. The row count is reported alongside the timings so a run that has double counted is
 * easy to spot.
 */
public class ModuleTimings implements Serializable {

    static final String TIME_PREFIX = "spruce.module_time_ns.";
    static final String ROWS_NAME = "spruce.rows_enriched";

    private final List<String> moduleNames;
    private final List<LongAccumulator> elapsedNanos;
    private final LongAccumulator rowsEnriched;

    private ModuleTimings(List<String> moduleNames, List<LongAccumulator> elapsedNanos,
                          LongAccumulator rowsEnriched) {
        this.moduleNames = moduleNames;
        this.elapsedNanos = elapsedNanos;
        this.rowsEnriched = rowsEnriched;
    }

    /**
     * Registers one accumulator per module, in the order the modules appear in the configuration,
     * which is the order the pipeline runs them in and the order the per-partition timing array is
     * indexed by.
     */
    public static ModuleTimings register(SparkContext sparkContext, List<EnrichmentModule> modules) {
        List<String> names = labels(modules);
        List<LongAccumulator> accumulators = new ArrayList<>(names.size());
        for (String name : names) {
            accumulators.add(sparkContext.longAccumulator(TIME_PREFIX + name));
        }
        return new ModuleTimings(names, accumulators, sparkContext.longAccumulator(ROWS_NAME));
    }

    /**
     * Labels the modules by simple class name, disambiguating the cases where the same class name
     * is used by more than one module in a configuration.
     */
    static List<String> labels(List<EnrichmentModule> modules) {
        List<String> names = new ArrayList<>(modules.size());
        Map<String, Integer> seen = new HashMap<>();
        for (EnrichmentModule module : modules) {
            String name = module.getClass().getSimpleName();
            int occurrence = seen.merge(name, 1, Integer::sum);
            names.add(occurrence == 1 ? name : name + "#" + occurrence);
        }
        return names;
    }

    /** Number of modules being timed. */
    public int size() {
        return moduleNames.size();
    }

    /**
     * Adds the timings gathered for one partition. Called from a task completion listener on the
     * executor, once per partition rather than once per row.
     *
     * @param nanos elapsed nanoseconds per module, indexed as the module list
     * @param rows  number of rows that went through the modules in this partition
     */
    public void add(long[] nanos, long rows) {
        if (nanos.length != elapsedNanos.size()) {
            throw new IllegalArgumentException("Expected " + elapsedNanos.size()
                    + " timings, got " + nanos.length);
        }
        for (int i = 0; i < nanos.length; i++) {
            elapsedNanos.get(i).add(nanos[i]);
        }
        rowsEnriched.add(rows);
    }

    /**
     * Renders the timings as a table, one line per module. Only meaningful once the action that
     * runs the pipeline has completed.
     */
    public String report() {
        long[] nanos = new long[elapsedNanos.size()];
        for (int i = 0; i < nanos.length; i++) {
            nanos[i] = elapsedNanos.get(i).value();
        }
        return format(moduleNames, nanos, rowsEnriched.value());
    }

    /** Table rendering, kept separate from the accumulators so it can be tested on its own. */
    static String format(List<String> moduleNames, long[] nanos, long rows) {
        long total = 0;
        for (long ns : nanos) {
            total += ns;
        }
        List<String> lines = new ArrayList<>(moduleNames.size() + 1);
        lines.add(String.format(Locale.ROOT,
                "Per-module timings: %,d rows enriched, %,d ms of module time summed over all tasks",
                rows, TimeUnit.NANOSECONDS.toMillis(total)));
        for (int i = 0; i < moduleNames.size(); i++) {
            lines.add(String.format(Locale.ROOT, "  %-34s %,9d ms %10.2f us/row %6.1f %%",
                    moduleNames.get(i),
                    TimeUnit.NANOSECONDS.toMillis(nanos[i]),
                    rows == 0 ? 0d : nanos[i] / 1000d / rows,
                    total == 0 ? 0d : 100d * nanos[i] / total));
        }
        return String.join(System.lineSeparator(), lines);
    }
}
