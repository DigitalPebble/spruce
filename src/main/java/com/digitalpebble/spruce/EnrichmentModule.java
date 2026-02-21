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
