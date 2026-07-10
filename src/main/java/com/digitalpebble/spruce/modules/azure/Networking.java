// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

import com.digitalpebble.spruce.AzureColumn;
import com.digitalpebble.spruce.AzureFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.RowColumn;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
 *  @see <a href="https://azure.microsoft.com/en-us/pricing/details/bandwidth/">Azure documentation</a>
 **/

public class Networking extends com.digitalpebble.spruce.modules.aws.Networking{

    private static final Logger LOG = LoggerFactory.getLogger(Networking.class);

    protected RowColumn meterCategory = AzureColumn.METER_CATEGORY;
    protected RowColumn meterSubCategory = AzureColumn.METER_SUBCATEGORY;
    protected RowColumn quantity = AzureColumn.QUANTITY;

    @Override
    public void bindReportFormat(ReportFormat reportFormat) {
        if (reportFormat == ReportFormat.FOCUS) {
            meterCategory = AzureFOCUSColumn.X_SKU_METER_CATEGORY;
            meterSubCategory = AzureFOCUSColumn.X_SKU_METER_SUBCATEGORY;
            quantity = FOCUSColumn.CONSUMED_QUANTITY;
        } else {
            meterCategory = AzureColumn.METER_CATEGORY;
            meterSubCategory = AzureColumn.METER_SUBCATEGORY;
            quantity = AzureColumn.QUANTITY;
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{meterCategory, meterSubCategory, quantity};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String meterCategory = this.meterCategory.getString(row);
        if (!"Bandwidth".equals(meterCategory)) {
            return;
        }

        String transfer_type = this.meterSubCategory.getString(row);

        double network_coefficient = 0d;

        if ("Inter-Region".equals(transfer_type)) {
            network_coefficient = network_coefficient_inter;
        }
        // TODO detect other types
        else {
            LOG.info("Transfer type not recognized: {}", transfer_type);
            return;
        }

        // get the amount of data transferred
        if (quantity.isNullAt(row)) {
            return;
        }
        double amount_gb = quantity.getDouble(row);
        double energy_gb = amount_gb * network_coefficient;

        enrichedValues.put(ENERGY_USED, energy_gb);
    }
}
