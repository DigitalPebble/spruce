// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

import com.digitalpebble.spruce.AzureFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.INSTANCE_TYPE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class InstanceTypeExtractionTest {

    private static final String VM_ADDITIONAL_INFO =
            "{\"UsageType\":\"ComputeHR\",\"ServiceType\":\"Standard_DS2_v2\",\"VMName\":\"web-1\",\"VCPUs\":2}";

    private final InstanceTypeExtraction module = new InstanceTypeExtraction();
    private final StructType schema = Utils.getSchema(module);

    @Test
    void extractsServiceTypeForVirtualMachines() {
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(generateRow(VM_ADDITIONAL_INFO, "Virtual Machines"), enriched);
        assertEquals("Standard_DS2_v2", enriched.get(INSTANCE_TYPE));
    }

    @Test
    void skipsOtherMeterCategories() {
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(generateRow("{\"ServiceType\":\"SQLThreatDetection\"}", "Microsoft Defender for Cloud"), enriched);
        assertFalse(enriched.containsKey(INSTANCE_TYPE));
    }

    @Test
    void skipsMissingServiceType() {
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(generateRow("{\"UsageType\":\"ComputeHR\"}", "Virtual Machines"), enriched);
        assertFalse(enriched.containsKey(INSTANCE_TYPE));
    }

    @Test
    void skipsInvalidJson() {
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(generateRow("not json", "Virtual Machines"), enriched);
        assertFalse(enriched.containsKey(INSTANCE_TYPE));
    }

    @Test
    void skipsNullAdditionalInfo() {
        Map<Column, Object> enriched = new HashMap<>();
        module.enrich(generateRow(null, "Virtual Machines"), enriched);
        assertFalse(enriched.containsKey(INSTANCE_TYPE));
    }

    private Row generateRow(String additionalInfo, String meterCategory) {
        return new GenericRowWithSchema(new Object[]{additionalInfo, meterCategory, null}, schema);
    }

    @Nested
    class FOCUSBinding {

        private final InstanceTypeExtraction focusModule = new InstanceTypeExtraction();
        private StructType focusSchema;

        @BeforeEach
        void initialize() {
            focusModule.bindReportFormat(ReportFormat.FOCUS);
            focusSchema = Utils.getSchema(focusModule);
        }

        @Test
        void columnsNeededReflectsFOCUSColumns() {
            assertArrayEquals(new Column[]{
                    InstanceTypeExtraction.X_SKU_DETAILS,
                    AzureFOCUSColumn.X_SKU_METER_CATEGORY
            }, focusModule.columnsNeeded());
        }

        @Test
        void extractsServiceTypeFromSkuDetails() {
            Map<Column, Object> enriched = new HashMap<>();
            Row row = new GenericRowWithSchema(new Object[]{VM_ADDITIONAL_INFO, "Virtual Machines", null}, focusSchema);
            focusModule.enrich(row, enriched);
            assertEquals("Standard_DS2_v2", enriched.get(INSTANCE_TYPE));
        }
    }
}
