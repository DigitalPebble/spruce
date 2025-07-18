// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.CarbonaraColumn.ENERGY_USED;

/**
 * Provides an estimate of energy used for storage.
 * Applies a flat coefficient per Gb
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#storage">CCF methodology</a>
 * @see <a href="https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/9f2cf436e5ad020830977e52c3b0a1719d20a8b9/packages/aws/src/lib/CostAndUsageTypes.ts#L25">resource file</a>
 **/
public class Storage implements EnrichmentModule {

    //  0.65 Watt-Hours per Terabyte-Hour for HDD
    final double hdd_gb_coefficient = 0.65 / 1024d;
    //  1.2 Watt-Hours per Terabyte-Hour for SSD
    final double ssd_gb_coefficient = 1.2 / 1024d;

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public Row process(Row row) {
        String operation = LINE_ITEM_OPERATION.getString(row);
        if (operation == null || !operation.startsWith("CreateVolume")) {
            return row;
        }

        // in gb months
        double amount_gb = USAGE_AMOUNT.getDouble(row);

        //  work out which coefficient should be applied
        // if the line item operation is CreateVolume without a suffix then it is hdd, sdd otherwise
        // (https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volume-types.html)
        boolean isSSD = operation.equals("CreateVolume");

        double coefficient = isSSD? ssd_gb_coefficient: hdd_gb_coefficient;

        double energy_gb = amount_gb * coefficient;

        return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, energy_gb);
    }
}
