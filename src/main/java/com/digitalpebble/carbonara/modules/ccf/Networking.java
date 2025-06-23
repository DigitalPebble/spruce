/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara.modules.ccf;

import com.digitalpebble.carbonara.Column;
import com.digitalpebble.carbonara.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.carbonara.CURColumn.PRODUCT_SERVICE_CODE;
import static com.digitalpebble.carbonara.CURColumn.USAGE_AMOUNT;
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
        int index = row.fieldIndex(PRODUCT_SERVICE_CODE.getLabel());
        String service_code = row.getString(index);
        if (service_code == null || !service_code.equals("AWSDataTransfer")) {
            return row;
        }
        //  apply only to rows corresponding to networking in or out of a region
        index = row.fieldIndex("product");
        Map<Object, Object> productMap = row.getJavaMap(index);
        String transfer_type = (String) productMap.getOrDefault("transfer_type", "");

        if (!transfer_type.startsWith("InterRegion")) {
            return row;
        }

        // TODO consider extending to AWS Outbound and Inbound

        // get the amount of data transferred
        index = row.fieldIndex(USAGE_AMOUNT.getLabel());
        double amount_gb = row.getDouble(index);
        double energy_gb = amount_gb * network_coefficient;

        return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, energy_gb);
    }
}
