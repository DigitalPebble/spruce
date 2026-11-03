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
 * Collects the time spent in each enrichment module, both configuring it and enriching rows with
 * it, using named Spark accumulators plus counts of the rows, tasks and module initialisations
 * involved. The accumulators are registered on the driver before the job runs, which is also what
 * makes them visible in the Accumulators table of the Spark UI, and are read back on the driver
 * once the action has completed.
 *
 * <p>The values are a sum of wall time over every task on every executor, not an elapsed duration,
 * so they are meant for comparing modules against each other rather than against the runtime of
 * the job. Enrichment cost is per row and configuration cost is per task, so the two are reported
 * separately rather than added together.
 *
 * <p>Accumulators updated from a transformation such as {@code mapPartitions} are not exactly
 * once: a speculative or retried task counts its time again, and so does re-reading a cached
 * Dataset. The row and task counts are reported alongside the timings so a run that has double
 * counted is easy to spot.
 */
public class ModuleTimings implements Serializable {

    static final String ENRICH_PREFIX = "spruce.module_enrich_ns.";
    static final String INIT_PREFIX = "spruce.module_init_ns.";
    static final String ROWS_NAME = "spruce.rows_enriched";
    static final String TASKS_NAME = "spruce.tasks";
    static final String INITS_NAME = "spruce.module_initialisations";

    private final List<String> moduleNames;
    private final List<LongAccumulator> enrichNanos;
    private final List<LongAccumulator> initNanos;
    private final LongAccumulator rowsEnriched;
    private final LongAccumulator tasks;
    private final LongAccumulator initialisations;

    private ModuleTimings(List<String> moduleNames, List<LongAccumulator> enrichNanos,
                          List<LongAccumulator> initNanos, LongAccumulator rowsEnriched,
                          LongAccumulator tasks, LongAccumulator initialisations) {
        this.moduleNames = moduleNames;
        this.enrichNanos = enrichNanos;
        this.initNanos = initNanos;
        this.rowsEnriched = rowsEnriched;
        this.tasks = tasks;
        this.initialisations = initialisations;
    }

    /**
     * Registers the accumulators, one pair per module, in the order the modules appear in the
     * configuration. That is the order the pipeline runs them in and the order the per-partition
     * timing arrays are indexed by.
     */
    public static ModuleTimings register(SparkContext sparkContext, List<EnrichmentModule> modules) {
        List<String> names = labels(modules);
        List<LongAccumulator> enrich = new ArrayList<>(names.size());
        List<LongAccumulator> init = new ArrayList<>(names.size());
        for (String name : names) {
            enrich.add(sparkContext.longAccumulator(ENRICH_PREFIX + name));
            init.add(sparkContext.longAccumulator(INIT_PREFIX + name));
        }
        return new ModuleTimings(names, enrich, init,
                sparkContext.longAccumulator(ROWS_NAME),
                sparkContext.longAccumulator(TASKS_NAME),
                sparkContext.longAccumulator(INITS_NAME));
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
     * Adds the timings gathered for one task. Called from a task completion listener on the
     * executor, once per partition rather than once per row.
     *
     * @param enriching elapsed nanoseconds per module spent enriching rows, indexed as the module
     *                  list
     * @param init      elapsed nanoseconds per module spent being configured, or null if the
     *                  modules were already configured when this task ran
     * @param rows      number of rows that went through the modules in this task
     */
    public void add(long[] enriching, long[] init, long rows) {
        check(enriching);
        for (int i = 0; i < enriching.length; i++) {
            enrichNanos.get(i).add(enriching[i]);
        }
        if (init != null) {
            check(init);
            for (int i = 0; i < init.length; i++) {
                initNanos.get(i).add(init[i]);
            }
            initialisations.add(1);
        }
        rowsEnriched.add(rows);
        tasks.add(1);
    }

    private void check(long[] nanos) {
        if (nanos.length != moduleNames.size()) {
            throw new IllegalArgumentException("Expected " + moduleNames.size()
                    + " timings, got " + nanos.length);
        }
    }

    /**
     * Renders the timings as two tables, one for enrichment and one for configuration. Only
     * meaningful once the action that runs the pipeline has completed.
     */
    public String report() {
        return format(moduleNames, values(enrichNanos), values(initNanos),
                rowsEnriched.value(), tasks.value(), initialisations.value());
    }

    private static long[] values(List<LongAccumulator> accumulators) {
        long[] nanos = new long[accumulators.size()];
        for (int i = 0; i < nanos.length; i++) {
            nanos[i] = accumulators.get(i).value();
        }
        return nanos;
    }

    /** Table rendering, kept separate from the accumulators so it can be tested on its own. */
    static String format(List<String> moduleNames, long[] enriching, long[] init,
                         long rows, long tasks, long initialisations) {
        List<String> lines = new ArrayList<>(2 * moduleNames.size() + 3);
        lines.add(String.format(Locale.ROOT,
                "Per-module timings: %,d rows enriched over %,d tasks, %,d module initialisations",
                rows, tasks, initialisations));

        lines.add(String.format(Locale.ROOT,
                "  enriching rows: %,d ms summed over all tasks",
                TimeUnit.NANOSECONDS.toMillis(sum(enriching))));
        long enrichTotal = sum(enriching);
        for (int i = 0; i < moduleNames.size(); i++) {
            lines.add(String.format(Locale.ROOT, "    %-32s %,9d ms %10.2f us/row %6.1f %%",
                    moduleNames.get(i),
                    TimeUnit.NANOSECONDS.toMillis(enriching[i]),
                    rows == 0 ? 0d : enriching[i] / 1000d / rows,
                    enrichTotal == 0 ? 0d : 100d * enriching[i] / enrichTotal));
        }

        long initTotal = sum(init);
        lines.add(String.format(Locale.ROOT,
                "  configuring modules: %,d ms summed over %,d initialisations, %.2f ms each",
                TimeUnit.NANOSECONDS.toMillis(initTotal), initialisations,
                initialisations == 0 ? 0d : initTotal / 1_000_000d / initialisations));
        for (int i = 0; i < moduleNames.size(); i++) {
            lines.add(String.format(Locale.ROOT, "    %-32s %,9d ms %10.2f ms/init %6.1f %%",
                    moduleNames.get(i),
                    TimeUnit.NANOSECONDS.toMillis(init[i]),
                    initialisations == 0 ? 0d : init[i] / 1_000_000d / initialisations,
                    initTotal == 0 ? 0d : 100d * init[i] / initTotal));
        }
        return String.join(System.lineSeparator(), lines);
    }

    private static long sum(long[] nanos) {
        long total = 0;
        for (long ns : nanos) {
            total += ns;
        }
        return total;
    }
}
