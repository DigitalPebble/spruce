// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.focus;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.FOCUSColumn.REGION_ID;
import static com.digitalpebble.spruce.SpruceColumn.REGION;

/**
 * Extracts the region information from the standard FOCUS column RegionId and normalises it
 * into a SPRUCE column Region. Provider-neutral: RegionId already holds the provider's region
 * code (e.g. "eastus", "us-east-1").
 **/
public class RegionExtraction implements EnrichmentModule {

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{REGION_ID};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{REGION};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String regionId = REGION_ID.getString(row);
        if (regionId != null) {
            enrichedValues.put(REGION, regionId.toLowerCase());
        }
    }
}
