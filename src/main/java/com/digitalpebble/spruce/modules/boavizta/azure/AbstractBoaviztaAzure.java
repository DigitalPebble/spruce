// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.azure;

import com.digitalpebble.spruce.AzureColumn;
import com.digitalpebble.spruce.AzureFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.RowColumn;
import com.digitalpebble.spruce.modules.boavizta.AbstractBoaviztaModule;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure-specific extraction of the Boavizta instance type from a row.
 *
 * <p>Subclasses only need to plug in the lookup variant via {@link #lookupImpacts(String)}.
 * The values read (meter names, units) are identical in legacy and FOCUS reports, only the
 * column labels differ: {@link #bindReportFormat(ReportFormat)} selects the bindings.
 */
abstract class AbstractBoaviztaAzure extends AbstractBoaviztaModule {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBoaviztaAzure.class);

    protected RowColumn meterCategory = AzureColumn.METER_CATEGORY;
    protected RowColumn meterName = AzureColumn.METER_NAME;
    protected RowColumn unitOfMeasure = AzureColumn.UNIT_OF_MEASURE;
    protected RowColumn quantity = AzureColumn.QUANTITY;

    AbstractBoaviztaAzure() {
        // The class is AZURE-specific by definition; default the provider so callers that bypass
        // the provider-aware init still get correct behaviour.
        this.provider = Provider.AZURE;
    }

    @Override
    public final void bindReportFormat(ReportFormat reportFormat) {
        if (reportFormat == ReportFormat.FOCUS) {
            meterCategory = AzureFOCUSColumn.X_SKU_METER_CATEGORY;
            meterName = AzureFOCUSColumn.X_SKU_METER_NAME;
            unitOfMeasure = AzureFOCUSColumn.X_PRICING_UNIT_DESCRIPTION;
            quantity = FOCUSColumn.CONSUMED_QUANTITY;
        } else {
            meterCategory = AzureColumn.METER_CATEGORY;
            meterName = AzureColumn.METER_NAME;
            unitOfMeasure = AzureColumn.UNIT_OF_MEASURE;
            quantity = AzureColumn.QUANTITY;
        }
    }

    @Override
    public final Column[] columnsNeeded() {
        return new Column[]{meterCategory, meterName, unitOfMeasure, quantity};
    }

    @Override
    protected final double getUsageAmount(Row row) {
        // TODO when more units of measure are supported, normalise value accordingly
        return quantity.getDouble(row);
    }

    @Override
    protected final String extractInstanceType(Row row) {
        String meterCategory = this.meterCategory.getString(row);
        String meterName = this.meterName.getString(row);

        if (!"Virtual Machines".equals(meterCategory))
        {
            return null;
        }

        if (meterName == null)
        {
            return null;
        }

        String unit = unitOfMeasure.getString(row);
        if (!"1 Hour".equals(unit)) {
            LOG.info(String.format("Unexpected Unit of Measure: %s", unit));
            return null;
        }

        // no usage amount to multiply the impacts by (seen in FOCUS reports)
        if (quantity.isNullAt(row)) {
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
