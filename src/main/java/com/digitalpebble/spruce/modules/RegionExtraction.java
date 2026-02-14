// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Extracts the region information from the input and stores it in a SPRUCE column Region
 **/
public class RegionExtraction implements EnrichmentModule {

    private static final Column[] location_columns = new Column[]{PRODUCT_REGION_CODE, PRODUCT_FROM_REGION_CODE, PRODUCT_TO_REGION_CODE};

    @Override
    public Column[] columnsNeeded() {
        return location_columns;
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{REGION};
    }

    @Override
    public void enrich(Row inputRow, Map<Column, Object> enrichedValues) {
        // get the location
        // in most cases you have a product_region_code but can be product_to_region_code or product_from_region_code
        // when the traffic is between two regions or to/from the outside
        for (Column c : location_columns) {
            String locationCode = c.getString(inputRow);
            if (locationCode != null) {
                enrichedValues.put(REGION, locationCode);
                return;
            }
        }
    }
}
