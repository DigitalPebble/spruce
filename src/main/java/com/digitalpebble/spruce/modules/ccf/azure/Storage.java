// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.digitalpebble.spruce.AzureColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;

/**
 * Provides an estimate of energy used for Azure storage capacity meters.
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#storage">CCF methodology</a>
 **/
public class Storage implements EnrichmentModule {

    private static final Logger LOG = LoggerFactory.getLogger(Storage.class);

    private static final Pattern MANAGED_DISK_PATTERN = Pattern.compile("\\b([PES]\\d{1,2})\\b");
    private static final Map<String, Integer> SSD_MANAGED_DISKS_STORAGE_GB = Map.ofEntries(
            Map.entry("P1", 4),
            Map.entry("P2", 8),
            Map.entry("P3", 16),
            Map.entry("P4", 32),
            Map.entry("P6", 64),
            Map.entry("P10", 128),
            Map.entry("P15", 256),
            Map.entry("P20", 512),
            Map.entry("P30", 1024),
            Map.entry("P40", 2048),
            Map.entry("P50", 4096),
            Map.entry("P60", 8192),
            Map.entry("P70", 16384),
            Map.entry("P80", 32767),
            Map.entry("E1", 4),
            Map.entry("E2", 8),
            Map.entry("E3", 16),
            Map.entry("E4", 32),
            Map.entry("E6", 64),
            Map.entry("E10", 128),
            Map.entry("E15", 256),
            Map.entry("E20", 512),
            Map.entry("E30", 1024),
            Map.entry("E40", 2048),
            Map.entry("E50", 4096),
            Map.entry("E60", 8192),
            Map.entry("E70", 16384),
            Map.entry("E80", 32767)
    );
    private static final Map<String, Integer> HDD_MANAGED_DISKS_STORAGE_GB = Map.ofEntries(
            Map.entry("S4", 32),
            Map.entry("S6", 64),
            Map.entry("S10", 128),
            Map.entry("S15", 256),
            Map.entry("S20", 512),
            Map.entry("S30", 1024),
            Map.entry("S40", 2048),
            Map.entry("S50", 4096),
            Map.entry("S60", 8192),
            Map.entry("S70", 16384),
            Map.entry("S80", 32767)
    );

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
        if (meterName == null) {
            return;
        }

        String unit = UNIT_OF_MEASURE.getString(row);
        double quantity = QUANTITY.getDouble(row);
        boolean isHDD = false;
        double gbMonths;

        if (meterName.contains("Data Stored")) {
            gbMonths = getGbMonths(quantity, unit);
        } else {
            ManagedDisk managedDisk = getManagedDisk(meterName);
            if (managedDisk == null || isDiskOperation(meterName)) {
                return;
            }
            gbMonths = getManagedDiskGbMonths(quantity, unit, managedDisk.sizeGb());
            isHDD = managedDisk.hdd();
        }

        if (Double.isNaN(gbMonths)) {
            return;
        }

        int replication = getReplicationFactor(meterName);
        computeEnergy(gbMonths, isHDD, replication, enrichedValues);
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
            // Match the existing storage coefficient normalisation: 1 TB = 1024 GB.
            return quantity * 1024;
        }
        return Double.NaN;
    }

    double getManagedDiskGbMonths(double quantity, String unit, int diskSizeGb) {
        if ("1/Month".equals(unit) || "1 /Month".equals(unit)) {
            return quantity * diskSizeGb;
        }
        if ("100/Month".equals(unit) || "100 /Month".equals(unit)) {
            return quantity * 100 * diskSizeGb;
        }
        return Double.NaN;
    }

    ManagedDisk getManagedDisk(String meterName) {
        Matcher matcher = MANAGED_DISK_PATTERN.matcher(meterName);
        if (!matcher.find()) {
            return null;
        }
        String diskType = matcher.group(1);

        Integer ssdSize = SSD_MANAGED_DISKS_STORAGE_GB.get(diskType);
        if (ssdSize != null) {
            return new ManagedDisk(ssdSize, false);
        }

        Integer hddSize = HDD_MANAGED_DISKS_STORAGE_GB.get(diskType);
        if (hddSize != null) {
            return new ManagedDisk(hddSize, true);
        }

        return null;
    }

    boolean isDiskOperation(String meterName) {
        return meterName.contains("Operations");
    }

    int getReplicationFactor(String meterName) {
        if (meterName == null) {
            return 1;
        }
        if (getManagedDisk(meterName) != null) {
            return 3;
        }
        if (meterName.contains("GZRS") || meterName.contains("RA-GRS") || meterName.contains("GRS")) {
            return 6;
        }
        if (meterName.contains("LRS") || meterName.contains("ZRS")) {
            return 3;
        }
        return 1;
    }

    private void computeEnergy(double gbMonths, boolean isHDD, int replication, Map<Column, Object> enrichedValues) {
        double coefficient = isHDD ? hdd_gb_coefficient : ssd_gb_coefficient;
        double gbHours = Utils.Conversions.GBMonthsToGBHours(gbMonths);
        double energyKwh = gbHours / 1000 * coefficient * replication;
        enrichedValues.put(ENERGY_USED, energyKwh);
    }

    record ManagedDisk(int sizeGb, boolean hdd) {
    }
}
