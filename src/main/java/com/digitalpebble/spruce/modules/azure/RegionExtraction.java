// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import static com.digitalpebble.spruce.AzureColumn.RESOURCE_LOCATION;
import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.REGION;

/**
 * Extracts the region information from the input and stores it in a SPRUCE column Region
 **/
public class RegionExtraction implements EnrichmentModule {

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{RESOURCE_LOCATION};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{REGION};
    }

    @Override
    public Row process(Row row) {
        String locationCode = RESOURCE_LOCATION.getString(row);
        if (locationCode != null) {
            return EnrichmentModule.withUpdatedValue(row, REGION, locationCode);
        }
        return row;
    }
}
