/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

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
