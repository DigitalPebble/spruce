// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.carbonara;

import com.digitalpebble.carbonara.modules.ccf.Networking;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

public abstract class Utils {


    public static Map<String, Object> loadJSONResources(String resourceFileName) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        try (InputStream inputStream = Networking.class
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

}
