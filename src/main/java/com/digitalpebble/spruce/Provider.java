// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

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
