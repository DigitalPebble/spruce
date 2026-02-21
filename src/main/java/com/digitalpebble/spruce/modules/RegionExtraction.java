// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.CURColumn;
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

    private static final CURColumn[] location_columns = new CURColumn[]{PRODUCT_REGION_CODE, PRODUCT_FROM_REGION_CODE, PRODUCT_TO_REGION_CODE};

    @Override
    public Column[] columnsNeeded() {
        return location_columns;
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{REGION};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        // get the location
        // in most cases you have a product_region_code but can be product_to_region_code or product_from_region_code
        // when the traffic is between two regions or to/from the outside
        for (CURColumn c : location_columns) {
            String locationCode = c.getString(row);
            if (locationCode != null) {
                enrichedValues.put(REGION, locationCode);
                return;
            }
        }
    }
}
