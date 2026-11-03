// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;

import java.util.Map;

/**
 * Measures how long a module spends in {@link #enrich(Row, Map)} without the module knowing about
 * it. The elapsed nanoseconds are added to {@code nanos[index]}, a plain array owned by the
 * partition being processed; the {@link EnrichmentPipeline} flushes that array into Spark
 * accumulators once the task completes, so nothing is added to a shared structure per row.
 *
 * <p>Only {@code enrich} is instrumented, everything else is delegated untouched. Timing a module
 * from the outside keeps the measured span identical for every module, and the {@code finally}
 * block means the modules' early returns (several have five or six) cannot leave a call
 * unrecorded.
 *
 * <p>Instances are created inside a partition, after the modules have been configured, so the
 * schema validation in {@link SparkJob} still runs against the real modules.
 */
final class TimedModule implements EnrichmentModule {

    private final EnrichmentModule delegate;
    private final long[] nanos;
    private final int index;

    TimedModule(EnrichmentModule delegate, long[] nanos, int index) {
        this.delegate = delegate;
        this.nanos = nanos;
        this.index = index;
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        final long start = System.nanoTime();
        try {
            delegate.enrich(row, enrichedValues);
        } finally {
            nanos[index] += System.nanoTime() - start;
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return delegate.columnsNeeded();
    }

    @Override
    public Column[] columnsAdded() {
        return delegate.columnsAdded();
    }

    @Override
    public void init(Map<String, Object> params) {
        delegate.init(params);
    }

    @Override
    public void init(Map<String, Object> params, Provider provider) {
        delegate.init(params, provider);
    }

    @Override
    public void bindReportFormat(ReportFormat reportFormat) {
        delegate.bindReportFormat(reportFormat);
    }

    /** The module being timed. */
    EnrichmentModule getDelegate() {
        return delegate;
    }
}
