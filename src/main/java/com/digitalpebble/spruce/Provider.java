// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

/** Normalises names for cloud provider e.g. when reading resource files **/
public enum Provider {

    AWS("Amazon Web Services", "aws"),
    GOOGLE("Google Cloud", "gcp"),
    AZURE("Microsoft Azure", "azure");

    private final String text;
    /** Short lowercase key used in resource CSV files (e.g. water-stress.csv, wcf.csv). */
    public final String csvKey;

    private Provider(final String text, final String csvKey) {
        this.text = text;
        this.csvKey = csvKey;
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
