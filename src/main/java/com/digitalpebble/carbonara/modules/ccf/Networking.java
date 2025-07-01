// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.carbonara.modules.ccf;

import com.digitalpebble.carbonara.Column;
import com.digitalpebble.carbonara.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.carbonara.CURColumn.*;
import static com.digitalpebble.carbonara.CarbonaraColumn.ENERGY_USED;

/**
 * Provides an estimate of energy used for networking in and out of data centres.
 * Applies a flat coefficient per Gb
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#networking">CCF methodology</a>
 * @see <a href="https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/main/packages/aws/src/lib/CostAndUsageTypes.ts#L108">resource file</a>
 **/
public class Networking implements EnrichmentModule {

    // estimated kWh/Gb
    final double network_coefficient = 0.001;

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public Row process(Row row) {
        String service_code = PRODUCT_SERVICE_CODE.getString(row);
        if (service_code == null || !service_code.equals("AWSDataTransfer")) {
            return row;
        }
        //  apply only to rows corresponding to networking in or out of a region
        int index = row.fieldIndex(PRODUCT.getLabel());
        Map<Object, Object> productMap = row.getJavaMap(index);
        String transfer_type = (String) productMap.getOrDefault("transfer_type", "");

        if (!transfer_type.startsWith("InterRegion")) {
            return row;
        }

        // TODO consider extending to AWS Outbound and Inbound

        // get the amount of data transferred
        double amount_gb = USAGE_AMOUNT.getDouble(row);
        double energy_gb = amount_gb * network_coefficient;

        return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, energy_gb);
    }
}
