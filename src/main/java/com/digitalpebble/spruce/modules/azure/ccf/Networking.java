// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.digitalpebble.spruce.AzureColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;

/**
 * Provides an estimate of energy used for networking in and out of data centres.
 * Applies a flat coefficient per Gb
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#networking">CCF methodology</a>
 * @see <a href="https://azure.microsoft.com/en-us/pricing/details/bandwidth/">Azure documentation</a>
 * @see <a href="https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/9f2cf436e5ad020830977e52c3b0a1719d20a8b9/packages/azure/src/lib/ConsumptionManagement.ts#L546">CCF Implementation</a>
 **/
public class Networking implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(Networking.class);

    // estimated kWh/Gb
    double network_coefficient = 0.001;

    @Override
    public void init(Map<String, Object> params) {
        Double coef = (Double) params.get("network_coefficient");
        if (coef != null) {
            network_coefficient = coef;
        }
        log.info("network_coefficient: {}", network_coefficient);
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{METER_CATEGORY};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String meterCategory = METER_CATEGORY.getString(row);
        if (!"Bandwidth".equals(meterCategory)) {
            return;
        }

        String transfer_type = METER_SUBCATEGORY.getString(row);
        if (!"InterRegion".equals(transfer_type)) {
            return;
        }

        // get the amount of data transferred
        double amount_gb = QUANTITY.getDouble(row);
        double energy_gb = amount_gb * network_coefficient;

        enrichedValues.put(ENERGY_USED, energy_gb);
    }
}
