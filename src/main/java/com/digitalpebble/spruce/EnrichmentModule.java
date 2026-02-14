// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.io.Serializable;
import java.util.Map;

/**
 * A module adds new columns to a Dataset and populates them based on its content.
 * The columns can represent energy or water consumption, carbon intensity, carbon emissions etc...
 * The bulk of the work is done in the map function.
 **/

public interface EnrichmentModule extends Serializable {

    /** Initialisation of the module; used to loads resources **/
    default void init(Map<String, Object> params){}

    /** Returns the columns required by this module **/
    Column[] columnsNeeded();

    /** Returns the columns added by this module **/
    Column[] columnsAdded();

    Row process(Row row);

    static Row withUpdatedValue(Row row, Column column, Object newValue) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
        }
        int index = column.resolveIndex(row);
        values[index] = newValue;
        return new GenericRowWithSchema(values, row.schema());
    }

    static Row withUpdatedValue(Row row, Column column, Double newValue, boolean add) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
        }
        int index = column.resolveIndex(row);
        Object existing = values[index];
        if (add && existing instanceof Double) {
            values[index] = newValue + (Double)  existing;
        } else {
            values[index] = newValue;
        }
        return new GenericRowWithSchema(values, row.schema());
    }

    static Row withUpdatedValues(Row row, Map<Column, Object> updates) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
        }

        for (Map.Entry<Column, Object> entry : updates.entrySet()) {
            Column column = entry.getKey();
            Object newValue = entry.getValue();

            int index;
            try {
                index = column.resolveIndex(row);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Field not found in row: " + column.getLabel(), e);
            }

            values[index] = newValue;
        }

        return new GenericRowWithSchema(values, row.schema());
    }
}
