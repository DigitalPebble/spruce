// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Map;

/**
 * A module adds new columns to a Dataset and populates them based on its content.
 * The columns can represent energy or water consumption, carbon intensity, carbon emissions etc...
 *
 * <p>Modules read CUR (input) columns from the original {@link Row} and read/write
 * Spruce (enrichment) columns via a shared {@code Map<Column, Object>}.
 * The pipeline materialises one final Row at the end, avoiding per-module row copies.
 **/

public interface EnrichmentModule extends Serializable {

    /** Initialisation of the module; used to loads resources **/
    default void init(Map<String, Object> params){}

    /**
     * Called by the {@link Config} as soon as the report format is known — at configuration load
     * time, before {@link #columnsNeeded()} is used for schema validation and long before
     * {@link #init(Map)}. Modules whose input column bindings depend on the format (e.g. legacy
     * Azure {@code MeterCategory} vs FOCUS {@code x_SkuMeterCategory}) override this; the default
     * implementation does nothing.
     */
    default void bindReportFormat(ReportFormat reportFormat) {}

    /**
     * Provider-aware initialisation. Override this if the module's behaviour depends on the
     * active cloud provider (e.g. for region-keyed lookups). The default implementation
     * delegates to {@link #init(Map)} so existing modules don't need to change.
     */
    default void init(Map<String, Object> params, Provider provider) {
        init(params);
    }

    /**
     * Returns true if this module must run on every row, including non-usage ones (taxes, fees,
     * credits, savings plan negations). The default is false: most modules estimate the impacts
     * of usage, and running them on non-usage rows would double-count (e.g. a savings plan
     * negation row mirrors the usage rows it discounts). Normalisation modules that copy billing
     * values (e.g. FOCUSColumns) override this so the output carries them for all rows.
     */
    default boolean processesAllRows() {
        return false;
    }

    /** Returns the columns required by this module **/
    Column[] columnsNeeded();

    /** Returns the columns added by this module **/
    Column[] columnsAdded();

    /**
     * Enrich the given row by reading input columns from {@code row} and
     * reading/writing enrichment columns via {@code enrichedValues}.
     *
     * @param row        the immutable original row from the dataset
     * @param enrichedValues  shared map accumulating enrichment values across all modules
     */
    void enrich(Row row, Map<Column, Object> enrichedValues);
}
