// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

import com.digitalpebble.spruce.AzureFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.*;

class NetworkingTest {

    private final Networking networking = new Networking();
    private final StructType schema = Utils.getSchema(networking);

    @Test
    void processNoValues() {
        Object[] values = new Object[] {null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    @Test
    void processNonBandwidthService() {
        Object[] values = new Object[] {"Storage", null, 10d};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    @Test
    void processInterRegion() {
        Object[] values = new Object[] {"Bandwidth", "Inter-Region", 10d};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        double expected = networking.network_coefficient_inter * 10;
        assertEquals(expected, (Double) enriched.get(ENERGY_USED));
    }

    @Test
    void processUnknownTransferType() {
        Object[] values = new Object[] {"Bandwidth", "SomethingElse", 10d};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    @Test
    void initWithCustomCoefficients() {
        Networking custom = new Networking();
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> coefficients = new HashMap<>();
        coefficients.put("inter", 0.003);
        params.put("network_coefficients_kwh_gb", coefficients);
        custom.init(params);
        assertEquals(0.003, custom.network_coefficient_inter);
    }

    @Test
    void initWithNoCoefficientsKeepsDefaults() {
        Networking custom = new Networking();
        custom.init(new HashMap<>());
        assertEquals(0.0015, custom.network_coefficient_inter);
    }

    /**
     * The FOCUS binding reads the same values from the FOCUS column names; the estimation logic
     * is shared with the tests above.
     */
    @Nested
    class FOCUSBinding {

        private final Networking focusNetworking = focusNetworking();
        private final StructType focusSchema = Utils.getSchema(focusNetworking);

        private Networking focusNetworking() {
            Networking networking = new Networking();
            networking.bindReportFormat(ReportFormat.FOCUS);
            return networking;
        }

        @Test
        void columnsNeededReflectsFOCUSColumns() {
            assertArrayEquals(new Column[]{
                    AzureFOCUSColumn.X_SKU_METER_CATEGORY,
                    AzureFOCUSColumn.X_SKU_METER_SUBCATEGORY,
                    FOCUSColumn.CONSUMED_QUANTITY
            }, focusNetworking.columnsNeeded());
        }

        @Test
        void processInterRegion() {
            Map<Column, Object> enriched = enrich("Bandwidth", "Inter-Region", 10d);
            double expected = focusNetworking.network_coefficient_inter * 10;
            assertEquals(expected, (Double) enriched.get(ENERGY_USED));
        }

        @Test
        void processInterRegionWithoutQuantity() {
            Map<Column, Object> enriched = enrich("Bandwidth", "Inter-Region", null);
            assertFalse(enriched.containsKey(ENERGY_USED));
        }

        private Map<Column, Object> enrich(String meterCategory, String meterSubCategory, Double quantity) {
            Object[] values = new Object[]{meterCategory, meterSubCategory, quantity, null};
            Row row = new GenericRowWithSchema(values, focusSchema);
            Map<Column, Object> enriched = new HashMap<>();
            focusNetworking.enrich(row, enriched);
            return enriched;
        }
    }
}