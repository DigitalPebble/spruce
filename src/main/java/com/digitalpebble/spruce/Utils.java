// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import com.digitalpebble.spruce.modules.ccf.Networking;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

        try (InputStream inputStream = Utils.class
                .getClassLoader()
                .getResourceAsStream(resourceFileName)) {

            if (inputStream == null) {
                throw new IOException("Resource file not found: " + resourceFileName);
            }

            return objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
        }
    }

    public static List<String> loadLinesResources(String resourceFileName) throws IOException {
        // Use the class loader to get the resource as an InputStream
        try (InputStream inputStream = Utils.class
                .getClassLoader()
                .getResourceAsStream(resourceFileName)) {
            if (inputStream == null) {
                throw new IOException("Resource file not found: " + resourceFileName);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.lines().toList();
            }
        }
    }


    /** Returns the schema for a module based on the list of columns it needs and the ones it generates **/
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

}
