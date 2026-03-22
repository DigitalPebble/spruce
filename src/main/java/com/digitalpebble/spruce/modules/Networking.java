// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
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
 **/
public class Networking implements EnrichmentModule {

    private static final Logger LOG = LoggerFactory.getLogger(Networking.class);

    // estimated kWh/Gb
    double network_coefficient_intra = 0.001;
    double network_coefficient_inter = 0.0015;
    double network_coefficient_extra = 0.059;

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
        return new Column[]{PRODUCT_SERVICE_CODE, PRODUCT, USAGE_AMOUNT};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String service_code = PRODUCT_SERVICE_CODE.getString(row);
        if (!"AWSDataTransfer".equals(service_code)) {
            return;
        }
        int index = PRODUCT.resolveIndex(row);
        Map<Object, Object> productMap = row.getJavaMap(index);
        String transfer_type = (String) productMap.getOrDefault("transfer_type", "");

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
}
