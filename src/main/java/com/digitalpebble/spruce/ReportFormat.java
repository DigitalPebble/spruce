// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

/**
 * Layout of the input billing report, independent of the cloud provider and of the file format
 * (CSV vs Parquet). {@link #NATIVE} designates the provider's own export (CUR for AWS, EA/MCA
 * cost details for Azure); {@link #FOCUS} designates a FOCUS (FinOps Open Cost &amp; Usage
 * Specification) export — 1.2 for AWS, 1.0 for Azure — whose standard columns are shared
 * across providers but whose {@code x_} extension columns remain provider-specific.
 **/
public enum ReportFormat {

    NATIVE,
    FOCUS;

    /// Returns the ReportFormat enum constant that matches the given text, ignoring case.
    public static ReportFormat fromString(String text) {
        for (ReportFormat format : ReportFormat.values()) {
            if (format.name().equalsIgnoreCase(text)) {
                return format;
            }
        }
        throw new IllegalArgumentException("No constant with text " + text + " found");
    }
}
