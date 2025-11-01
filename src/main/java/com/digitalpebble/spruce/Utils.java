// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Utils {


    public static Map<String, Object> loadJSONResources(String resourceFileName) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        try (InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream(resourceFileName)) {

            if (inputStream == null) {
                throw new IOException("Resource file not found: " + resourceFileName);
            }

            return objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {
            });
        }
    }

    public static List<String> loadLinesResources(String resourceFileName) throws IOException {
        // Use the class loader to get the resource as an InputStream
        try (InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream(resourceFileName)) {
            if (inputStream == null) {
                throw new IOException("Resource file not found: " + resourceFileName);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.lines().toList();
            }
        }
    }


    /**
     * Returns the schema for a module based on the list of columns it needs and the ones it generates
     **/
    public static StructType getSchema(EnrichmentModule module) {
        final List<StructField> fields = new ArrayList<>();

        for (Column column : module.columnsNeeded()) {
            fields.add(StructField.apply(column.getLabel(), column.getType(), true, null));
        }

        for (Column column : module.columnsAdded()) {
            fields.add(StructField.apply(column.getLabel(), column.getType(), true, null));
        }

        return new StructType(fields.toArray(new StructField[fields.size()]));
    }

    /**
     * Clone a Row and give it new values
     **/
    public static Row withUpdatedValues(Row row, Map<String, Object> updates) {

        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
        }

        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            String field = entry.getKey();
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

    /**
     * Utility conversions used throughout the codebase.
     *
     * <p>Contains helper methods to convert between different units of usage.
     */
    public static class Conversions {
        /**
         * Converts a usage amount expressed in gigabyte-months (GB·month) to gigabyte-hours (GB·hour).
         *
         * <p>The conversion uses an average month length of 30.42 days. This is an approximation;
         * for precise conversions the exact number of days represented by the original timestamp
         * should be used instead.
         *
         * @param usageAmount usage in gigabyte-months
         * @return equivalent usage in gigabyte-hours
         * @implNote 1 month ≈ 30.42 days, so GB·month * 24 * 30.42 = GB·hour
         */
        public static double GBMonthsToGBHours(double usageAmount) {
            // would need to know the exact number of days from the timestamp
            // but go with average for now
            final double daysInMonth = 30.42d;
            return usageAmount * 24 * daysInMonth;
        }
    }
}
