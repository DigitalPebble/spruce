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

/** Normalises names for cloud provider e.g. when reading resource files **/
public enum Provider {

    AWS("Amazon Web Services"),
    GOOGLE("Google Cloud"),
    AZURE("Microsoft Azure");

    private final String text;

    private Provider(final String text) {
        this.text = text;
    }

    /// Returns the Provider enum constant that matches the given text.
    public static Provider fromString(String text) {
        for (Provider provider : Provider.values()) {
            if (provider.text.equalsIgnoreCase(text)) {
                return provider;
            }
        }
        throw new IllegalArgumentException("No constant with text " + text + " found");
    }

    @Override
    public String toString() {
        return text;
    }

}
