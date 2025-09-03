// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static com.digitalpebble.spruce.Utils.loadJSONResources;

/**
 * Provides an estimate of energy used for storage.
 * Applies a flat coefficient per Gb
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#storage">CCF methodology</a>
 * @see <a href="https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/9f2cf436e5ad020830977e52c3b0a1719d20a8b9/packages/aws/src/lib/CostAndUsageTypes.ts#L25">resource file</a>
 **/
public class Storage implements EnrichmentModule {

    //  0.65 Watt-Hours per Terabyte-Hour for HDD
    double hdd_gb_coefficient = 0.65 / 1024d;
    //  1.2 Watt-Hours per Terabyte-Hour for SSD
    double ssd_gb_coefficient = 1.2 / 1024d;

    List<String> ssd_usage_types;
    List<String> hdd_usage_types;
    List<String> ssd_services;

    @Override
    public void init(Map<String, Object> params) {
        Double coef = (Double) params.get("hdd_gb_coefficient");
        if (coef != null) {
            hdd_gb_coefficient = coef / 1024d;
        }
        coef = (Double) params.get("ssd_gb_coefficient");
        if (coef != null) {
            ssd_gb_coefficient = coef / 1024d;
        }

        try {
            Map<String, Object> map = loadJSONResources("ccf/storage.json");
            ssd_usage_types = (List<String>) map.get("SSD_USAGE_TYPES");
            hdd_usage_types = (List<String>) map.get("HDD_USAGE_TYPES");
            ssd_services = (List<String>) map.get("SSD_SERVICES");
        } catch (
                IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{LINE_ITEM_OPERATION, USAGE_AMOUNT, LINE_ITEM_USAGE_TYPE};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public Row process(Row row) {
        String operation = LINE_ITEM_OPERATION.getString(row);
        if (operation == null) {
            return row;
        }

        // EC2 instances
        if (operation.startsWith("CreateVolume")) {
            //  work out which coefficient should be applied
            // if the line item operation is CreateVolume without a suffix then it is hdd, sdd otherwise
            // (https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volume-types.html)
            boolean isHDD = operation.equals("CreateVolume");
            return enrich(row, isHDD);
        }

        // implement the logic from CCF

        // TODO detection based on service names
        // TODO handle replication

        String usage_type = LINE_ITEM_USAGE_TYPE.getString(row);
        if (usage_type == null) {
            return row;
        }

        // loop on the values from the resources
        for (String ssd : ssd_usage_types) {
            if (usage_type.endsWith(ssd)) {
                return enrich(row, false);
            }
        }

        for (String hdd : hdd_usage_types) {
            if (usage_type.endsWith(hdd)) {
                return enrich(row, true);
            }
        }

        // Log so that can improve coverage in the longer term
        String product_product_family = PRODUCT_PRODUCT_FAMILY.getString(row);
        if ("Storage".equals(product_product_family)) {
            System.out.println("Storage type not found for " + operation + " "+ usage_type);
        }

        // not been found
        return row;
    }


    private Row enrich(Row row, boolean isHDD) {
        double coefficient = isHDD ? hdd_gb_coefficient : ssd_gb_coefficient;

        // in gb months
        double amount_gb = USAGE_AMOUNT.getDouble(row);

        double energy_gb = amount_gb * coefficient;

        return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, energy_gb);
    }
}