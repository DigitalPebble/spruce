// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

import com.digitalpebble.spruce.AWSFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;

/**
 * Provides an estimate of energy used for networking in and out of data centres.
 * Distinguishes between:
 *  - Intra-region: 0.001
 *  - Inter-region: 0.0015
 *  - External: 0.059
 *  The coefficients are taken from the Boavizta Cloud Emissions Working Group.
 *
 *  The relevance and usefulness of attributing emissions for networking based on usage is
 *  subject for debate as the energy use of networking is pretty constant independently of
 *  traffic. The consequences of reducing networking are probably negligible but since the
 *  approach in SPRUCE is attributional, we do the same for networking in order to be consistent.
 *
 *  <p>FOCUS reports carry neither {@code product_servicecode} (whose {@code AWSDataTransfer}
 *  value marks transfer lines in the CUR) nor the {@code transfer_type} product attribute, so
 *  the transfer category is derived from the {@code SkuMeter} column instead — its values are
 *  the CUR usage types, whose suffixes map one-to-one to the transfer types (verified on CUR
 *  data: {@code DataTransfer-Regional/xAZ-*-Bytes} ↔ IntraRegion, {@code *AWS-*-Bytes} and
 *  {@code *CloudFront-*-Bytes} ↔ InterRegion, {@code DataTransfer-In/Out-Bytes} ↔ AWS
 *  Inbound/Outbound). Rows whose {@code x_ServiceCode} is {@code AmazonCloudFront} (CDN
 *  delivery) are skipped, as they are in the CUR where their servicecode is not
 *  {@code AWSDataTransfer}.
 **/
public class Networking implements EnrichmentModule {

    private static final Logger LOG = LoggerFactory.getLogger(Networking.class);

    // estimated kWh/Gb
    public double network_coefficient_intra = 0.001;
    public double network_coefficient_inter = 0.0015;
    public double network_coefficient_extra = 0.059;

    private boolean focus = false;

    @Override
    public void bindReportFormat(ReportFormat reportFormat) {
        focus = reportFormat == ReportFormat.FOCUS;
    }

    @Override
    public void init(Map<String, Object> params) {
        Map<String, Object> network_coefficients = (Map<String, Object>) params.get("network_coefficients_kwh_gb");
        if (network_coefficients != null) {
            Number intra = (Number) network_coefficients.get("intra");
            if (intra != null) {
                network_coefficient_intra = intra.doubleValue();
            }
            Number inter = (Number) network_coefficients.get("inter");
            if (inter != null) {
                network_coefficient_inter = inter.doubleValue();
            }
            Number extra = (Number) network_coefficients.get("extra");
            if (extra != null) {
                network_coefficient_extra = extra.doubleValue();
            }
        }
        LOG.info("network_coefficients_kwh_gb: intra={}, inter={}, extra={}", network_coefficient_intra, network_coefficient_inter, network_coefficient_extra);
    }

    @Override
    public Column[] columnsNeeded() {
        if (focus) {
            return new Column[]{FOCUSColumn.SKU_METER, AWSFOCUSColumn.X_SERVICE_CODE, FOCUSColumn.CONSUMED_QUANTITY};
        }
        return new Column[]{PRODUCT_SERVICE_CODE, PRODUCT, USAGE_AMOUNT};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        if (focus) {
            enrichFocus(row, enrichedValues);
            return;
        }
        String service_code = PRODUCT_SERVICE_CODE.getString(row);
        if (!"AWSDataTransfer".equals(service_code)) {
            return;
        }
        String transfer_type = Utils.getStringFromProductMap(row, "transfer_type", "");

        double network_coefficient = 0d;

        if (transfer_type.startsWith("Inter")) {
            network_coefficient = network_coefficient_inter;
        }
        else if (transfer_type.startsWith("IntraRegion")) {
            network_coefficient = network_coefficient_intra;
        }
        else if (transfer_type.startsWith("AWS Inbound")) {
            network_coefficient = network_coefficient_extra;
        }
        else if (transfer_type.startsWith("AWS Outbound")) {
            network_coefficient = network_coefficient_extra;
        }
        else {
            LOG.info("Transfer type not recognized: {}", transfer_type);
            return;
        }

        // get the amount of data transferred
        double amount_gb = USAGE_AMOUNT.getDouble(row);
        double energy_gb = amount_gb * network_coefficient;

        enrichedValues.put(ENERGY_USED, energy_gb);
    }

    private void enrichFocus(Row row, Map<Column, Object> enrichedValues) {
        // CDN delivery is not counted in the CUR either (servicecode AmazonCloudFront)
        if ("AmazonCloudFront".equals(AWSFOCUSColumn.X_SERVICE_CODE.getString(row))) {
            return;
        }
        String skuMeter = FOCUSColumn.SKU_METER.getString(row);
        Double network_coefficient = classifyUsageType(skuMeter);
        if (network_coefficient == null) {
            return;
        }

        // get the amount of data transferred
        double amount_gb = FOCUSColumn.CONSUMED_QUANTITY.getDouble(row);
        double energy_gb = amount_gb * network_coefficient;

        enrichedValues.put(ENERGY_USED, energy_gb);
    }

    /**
     * Maps a usage type (SkuMeter) to the network coefficient of its transfer category, or null
     * for non-transfer meters. The suffixes match the CUR {@code transfer_type} values one-to-one,
     * regardless of the region prefixes (e.g. {@code EUW2-} or {@code EUW2-USE1-}).
     */
    Double classifyUsageType(String usageType) {
        if (usageType == null) {
            return null;
        }
        // IntraRegion and IntraRegion-xAZ-In/Out
        if (usageType.endsWith("DataTransfer-Regional-Bytes")
                || usageType.endsWith("DataTransfer-xAZ-In-Bytes")
                || usageType.endsWith("DataTransfer-xAZ-Out-Bytes")) {
            return network_coefficient_intra;
        }
        // AWS Inbound/Outbound (to/from the internet)
        if (usageType.endsWith("DataTransfer-In-Bytes")
                || usageType.endsWith("DataTransfer-Out-Bytes")) {
            return network_coefficient_extra;
        }
        // InterRegion Inbound/Outbound and Inter Region Peering
        if (usageType.endsWith("AWS-In-Bytes")
                || usageType.endsWith("AWS-Out-Bytes")) {
            return network_coefficient_inter;
        }
        // origin fetches between CloudFront and AWS services, InterRegion in the CUR
        if (usageType.contains("CloudFront")
                && (usageType.endsWith("-In-Bytes") || usageType.endsWith("-Out-Bytes"))) {
            return network_coefficient_inter;
        }
        return null;
    }
}
