// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.modules.boavizta.AbstractBoaviztaModule;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.digitalpebble.spruce.AzureColumn.*;

/**
 * Azure-specific extraction of the Boavizta instance type from a row.
 *
 * <p>Subclasses only need to plug in the lookup variant via {@link #lookupImpacts(String)}.
 */
abstract class AbstractBoaviztaAzure extends AbstractBoaviztaModule {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBoaviztaAzure.class);

    private static final Column[] COLUMNS_NEEDED = new Column[]{
            METER_CATEGORY, METER_NAME, UNIT_OF_MEASURE, QUANTITY
    };

    AbstractBoaviztaAzure() {
        // The class is AZURE-specific by definition; default the provider so callers that bypass
        // the provider-aware init still get correct behaviour.
        this.provider = Provider.AZURE;
    }

    @Override
    public final Column[] columnsNeeded() {
        return COLUMNS_NEEDED;
    }

    @Override
    protected final double getUsageAmount(Row row) {
        // TODO when more units of measure are supported, normalise value accordingly
        return QUANTITY.getDouble(row);
    }

    @Override
    protected final String extractInstanceType(Row row) {
        String meterCategory = METER_CATEGORY.getString(row);
        String meterName = METER_NAME.getString(row);

        String unit = UNIT_OF_MEASURE.getString(row);
        if (!"1 Hour".equals(unit)) {
            LOG.info(String.format("Unexpected Unit of Measure: %s", unit));
            return null;
        }

        if (!"Virtual Machines".equals(meterCategory))
        {
            return null;
        }

        if (meterName == null)
        {
            return null;
        }

        // If the meter name contains a slash (e.g. "D11 v2/DS11 v2") take the second
        // part as requested (whatever comes after the first '/'). Trim then
        // normalise by lowercasing and replacing spaces with underscores.
        meterName = meterName.trim();
        String[] parts = meterName.split("/", 2);
        if (parts.length > 1) {
            meterName = parts[1].trim();
        }

        meterName = meterName.toLowerCase().replace(' ', '_');

        return meterName;
    }
}
