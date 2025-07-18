// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.io.Serializable;
import java.util.Map;

/** A module adds new columns to a Dataset and populates them based on its content.
 *  The columns can represent energy or water consumption, carbon intensity, carbon emissions etc...
 *  The bulk of the work is done in the map function.
 **/

public interface EnrichmentModule extends Serializable {

    /** Initialisation of the module; used to loads resources **/
    public default void init(Map<String, String> params){}

    /** Returns the columns added by this module **/
    public Column[] columnsAdded();

    public Row process(Row row);

    public static Row withUpdatedValue(Row row, Column column, Object newValue) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
        }
        int index = row.fieldIndex(column.getLabel());
        values[index] = newValue;
        return new GenericRowWithSchema(values, row.schema());
    }

    public static Row withUpdatedValues(Row row, Map<Column, Object> updates) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
        }

        for (Map.Entry<Column, Object> entry : updates.entrySet()) {
            String field = entry.getKey().getLabel();
            Object newValue = entry.getValue();

            int index;
            try {
                index = row.fieldIndex(field);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Field not found in row: " + field, e);
            }

            values[index] = newValue;
        }

        return new GenericRowWithSchema(values, row.schema());
    }
}
