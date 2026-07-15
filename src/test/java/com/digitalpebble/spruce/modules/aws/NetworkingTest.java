// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

import com.digitalpebble.spruce.AWSFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static org.junit.jupiter.api.Assertions.*;

class NetworkingTest {

    private final Networking networking = new Networking();
    private final StructType schema = Utils.getSchema(networking);

    @Test
    void processNoValues() {
        Object[] values = new Object[] {null, null, null, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    @Test
    void processNonDataTransferService() {
        Map<String, String> product = new HashMap<>();
        Object[] values = new Object[] {"AmazonEC2", JavaConverters.asScala(product), 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        assertFalse(enriched.containsKey(ENERGY_USED));
    }

    @Test
    void processIntraRegion() {
        Map<String, String> product = new HashMap<>();
        product.put("transfer_type", "IntraRegion");
        Object[] values = new Object[] {"AWSDataTransfer", JavaConverters.asScala(product), 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        double expected = networking.network_coefficient_intra * 10;
        assertEquals(expected, (Double) enriched.get(ENERGY_USED));
    }

    @Test
    void processInterRegion() {
        Map<String, String> product = new HashMap<>();
        product.put("transfer_type", "InterRegion");
        Object[] values = new Object[] {"AWSDataTransfer", JavaConverters.asScala(product), 10d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        double expected = networking.network_coefficient_inter * 10;
        assertEquals(expected, (Double) enriched.get(ENERGY_USED));
    }

    @Test
    void processAWSOutbound() {
        Map<String, String> product = new HashMap<>();
        product.put("transfer_type", "AWS Outbound");
        Object[] values = new Object[] {"AWSDataTransfer", JavaConverters.asScala(product), 5d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        double expected = networking.network_coefficient_extra * 5;
        assertEquals(expected, (Double) enriched.get(ENERGY_USED));
    }

    @Test
    void processAWSInbound() {
        Map<String, String> product = new HashMap<>();
        product.put("transfer_type", "AWS Inbound");
        Object[] values = new Object[] {"AWSDataTransfer", JavaConverters.asScala(product), 5d, null};
        Row row = new GenericRowWithSchema(values, schema);
        Map<Column, Object> enriched = new HashMap<>();
        networking.enrich(row, enriched);
        double expected = networking.network_coefficient_extra * 5;
        assertEquals(expected, (Double) enriched.get(ENERGY_USED));
    }

    @Test
    void processUnknownTransferType() {
        Map<String, String> product = new HashMap<>();
        product.put("transfer_type", "SomethingElse");
        Object[] values = new Object[] {"AWSDataTransfer", JavaConverters.asScala(product), 10d, null};
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
        coefficients.put("intra", 0.002);
        coefficients.put("inter", 0.003);
        coefficients.put("extra", 0.1);
        params.put("network_coefficients_kwh_gb", coefficients);
        custom.init(params);
        assertEquals(0.002, custom.network_coefficient_intra);
        assertEquals(0.003, custom.network_coefficient_inter);
        assertEquals(0.1, custom.network_coefficient_extra);
    }

    @Test
    void initWithNoCoefficientsKeepsDefaults() {
        Networking custom = new Networking();
        custom.init(new HashMap<>());
        assertEquals(0.001, custom.network_coefficient_intra);
        assertEquals(0.0015, custom.network_coefficient_inter);
        assertEquals(0.059, custom.network_coefficient_extra);
    }

    @Test
    void initWithPartialCoefficients() {
        Networking custom = new Networking();
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> coefficients = new HashMap<>();
        coefficients.put("inter", 0.005);
        params.put("network_coefficients_kwh_gb", coefficients);
        custom.init(params);
        assertEquals(0.001, custom.network_coefficient_intra);
        assertEquals(0.005, custom.network_coefficient_inter);
        assertEquals(0.059, custom.network_coefficient_extra);
    }

    /**
     * FOCUS reports carry no {@code product_servicecode} or {@code transfer_type}; the transfer
     * category is derived from the SkuMeter (usage type) suffixes instead.
     */
    @Nested
    class FOCUSBinding {

        private final Networking focusNetworking = new Networking();
        private StructType focusSchema;

        @org.junit.jupiter.api.BeforeEach
        void initialize() {
            focusNetworking.bindReportFormat(ReportFormat.FOCUS);
            focusSchema = Utils.getSchema(focusNetworking);
        }

        @Test
        void columnsNeededReflectsFOCUSColumns() {
            assertArrayEquals(new Column[]{
                    FOCUSColumn.SKU_METER,
                    AWSFOCUSColumn.X_SERVICE_CODE,
                    FOCUSColumn.CONSUMED_QUANTITY
            }, focusNetworking.columnsNeeded());
        }

        @ParameterizedTest
        @CsvSource({
                "DataTransfer-Regional-Bytes, intra",
                "USE1-DataTransfer-xAZ-In-Bytes, intra",
                "EUW2-DataTransfer-xAZ-Out-Bytes, intra",
                "DataTransfer-In-Bytes, extra",
                "EUW2-DataTransfer-Out-Bytes, extra",
                "AWS-In-Bytes, inter",
                "EUW2-USE1-AWS-Out-Bytes, inter",
                "USE1-CloudFront-In-Bytes, inter",
                "EUN1-CloudFrontChina-Out-Bytes, inter"
        })
        void classifiesTransferMeters(String skuMeter, String category) {
            Map<Column, Object> enriched = enrich(skuMeter, "AmazonEC2", 10d);
            double coefficient = switch (category) {
                case "intra" -> focusNetworking.network_coefficient_intra;
                case "inter" -> focusNetworking.network_coefficient_inter;
                default -> focusNetworking.network_coefficient_extra;
            };
            assertEquals(coefficient * 10, (Double) enriched.get(ENERGY_USED));
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "EBS:VolumeUsage.gp3",
                "USE1-DataProcessing-Bytes",
                "BoxUsage:m5.large",
                "Requests-Tier1"
        })
        void skipsNonTransferMeters(String skuMeter) {
            Map<Column, Object> enriched = enrich(skuMeter, "AmazonEC2", 10d);
            assertFalse(enriched.containsKey(ENERGY_USED));
        }

        @Test
        void skipsCloudFrontDelivery() {
            // CDN delivery lines: servicecode AmazonCloudFront in the CUR, not AWSDataTransfer
            Map<Column, Object> enriched = enrich("EU-DataTransfer-Out-Bytes", "AmazonCloudFront", 10d);
            assertFalse(enriched.containsKey(ENERGY_USED));
        }

        @Test
        void skipsNullSkuMeter() {
            Map<Column, Object> enriched = enrich(null, "AmazonEC2", 10d);
            assertFalse(enriched.containsKey(ENERGY_USED));
        }

        private Map<Column, Object> enrich(String skuMeter, String serviceCode, Double quantity) {
            Object[] values = new Object[]{skuMeter, serviceCode, quantity, null};
            Row row = new GenericRowWithSchema(values, focusSchema);
            Map<Column, Object> enriched = new HashMap<>();
            focusNetworking.enrich(row, enriched);
            return enriched;
        }
    }
}
