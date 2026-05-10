// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.digitalpebble.spruce.AzureColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;

/**
 * Provides an estimate of energy used for Azure storage capacity meters.
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#storage">CCF methodology</a>
 **/
public class Storage implements EnrichmentModule {

    private static final Logger LOG = LoggerFactory.getLogger(Storage.class);

    //  0.65 Watt-Hours per Terabyte-Hour for HDD
    double hdd_gb_coefficient = 0.65 / 1024d;
    //  1.2 Watt-Hours per Terabyte-Hour for SSD
    double ssd_gb_coefficient = 1.2 / 1024d;

    @Override
    public void init(Map<String, Object> params) {
        Double coef = (Double) params.get("hdd_coefficient_tb_h");
        if (coef != null) {
            hdd_gb_coefficient = coef / 1024d;
        }
        coef = (Double) params.get("ssd_coefficient_tb_h");
        if (coef != null) {
            ssd_gb_coefficient = coef / 1024d;
        }

        LOG.info("hdd_gb_coefficient: {}", hdd_gb_coefficient);
        LOG.info("ssd_gb_coefficient: {}", ssd_gb_coefficient);
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{METER_CATEGORY, METER_SUBCATEGORY, METER_NAME, UNIT_OF_MEASURE, QUANTITY};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String meterCategory = METER_CATEGORY.getString(row);
        if (!"Storage".equals(meterCategory)) {
            return;
        }

        String meterName = METER_NAME.getString(row);
        if (meterName == null || !meterName.contains("Data Stored")) {
            return;
        }

        String unit = UNIT_OF_MEASURE.getString(row);
        double gbMonths = getGbMonths(QUANTITY.getDouble(row), unit);
        if (Double.isNaN(gbMonths)) {
            return;
        }

        int replication = getReplicationFactor(meterName);
        computeEnergy(gbMonths, replication, enrichedValues);
    }

    double getGbMonths(double quantity, String unit) {
        if ("1 GB/Month".equals(unit)) {
            return quantity;
        }
        if ("10 GB/Month".equals(unit)) {
            return quantity * 10;
        }
        if ("100 GB/Month".equals(unit)) {
            return quantity * 100;
        }
        if ("1 TB/Month".equals(unit)) {
            return quantity * 1000;
        }
        return Double.NaN;
    }

    int getReplicationFactor(String meterName) {
        if (meterName == null) {
            return 1;
        }
        if (meterName.contains("GZRS") || meterName.contains("RA-GRS") || meterName.contains("GRS")) {
            return 6;
        }
        if (meterName.contains("LRS") || meterName.contains("ZRS")) {
            return 3;
        }
        return 1;
    }

    private void computeEnergy(double gbMonths, int replication, Map<Column, Object> enrichedValues) {
        double gbHours = Utils.Conversions.GBMonthsToGBHours(gbMonths);
        double energyKwh = gbHours / 1000 * ssd_gb_coefficient * replication;
        enrichedValues.put(ENERGY_USED, energyKwh);
    }
}
