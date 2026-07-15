// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

import com.digitalpebble.spruce.AzureColumn;
import com.digitalpebble.spruce.AzureFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.RowColumn;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.AzureColumn.METER_CATEGORY;
import static com.digitalpebble.spruce.SpruceColumn.INSTANCE_TYPE;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Copies the Azure VM size (e.g. {@code Standard_DS2_v2}) into the provider-neutral
 * {@code instance_type} output column, so reporting does not need per-format extraction logic.
 *
 * <p>The size is held under the {@code ServiceType} key of a JSON details blob
 * ({@code AdditionalInfo} in cost details exports, {@code x_SkuDetails} in FOCUS ones);
 * {@link #bindReportFormat(ReportFormat)} selects the bindings. Rows are gated on the
 * "Virtual Machines" meter category as other services reuse {@code ServiceType} for
 * non-instance values (e.g. {@code SQLThreatDetection}, {@code Gateway}).
 **/
public class InstanceTypeExtraction implements EnrichmentModule {

    // Module-local on purpose: the JSON blobs carry VM names, so listing these columns in
    // AzureColumn/AzureFOCUSColumn would add them to the AnonymizeJob whitelist.
    public static final AzureColumn ADDITIONAL_INFO = new AzureColumn("AdditionalInfo", StringType);
    public static final AzureFOCUSColumn X_SKU_DETAILS = new AzureFOCUSColumn("x_SkuDetails", StringType);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private RowColumn skuDetails = ADDITIONAL_INFO;
    private RowColumn meterCategory = METER_CATEGORY;

    @Override
    public boolean processesAllRows() {
        // metadata, not an impact estimate: refund/unused-commitment rows keep their VM size
        return true;
    }

    @Override
    public void bindReportFormat(ReportFormat reportFormat) {
        if (reportFormat == ReportFormat.FOCUS) {
            skuDetails = X_SKU_DETAILS;
            meterCategory = AzureFOCUSColumn.X_SKU_METER_CATEGORY;
        } else {
            skuDetails = ADDITIONAL_INFO;
            meterCategory = METER_CATEGORY;
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{skuDetails, meterCategory};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{INSTANCE_TYPE};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (!"Virtual Machines".equals(meterCategory.getString(row))) {
            return;
        }
        String json = skuDetails.getString(row);
        if (json == null || json.isEmpty()) {
            return;
        }
        try {
            JsonNode serviceType = OBJECT_MAPPER.readTree(json).path("ServiceType");
            if (serviceType.isTextual() && !serviceType.asText().isEmpty()) {
                enrichedValues.put(INSTANCE_TYPE, serviceType.asText());
            }
        } catch (Exception ignored) {
        }
    }
}
