// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
    private static final double HOURS_PER_AZURE_PRICING_MONTH = 30d * 24d;
    private static final String MANAGED_DISK_REPLICATION_FACTOR = "STORAGE_DISKS";

    List<String> dataStoredUsageTypes;
    Map<String, Integer> storageUsageUnitMultipliers;
    Map<String, Integer> managedDiskUsageUnitMultipliers;
    Map<String, Integer> replicationFactors;
    Map<String, ManagedDisk> managedDisks;

    //  0.65 Watt-Hours per Terabyte-Hour for HDD
    double hdd_gb_coefficient = 0.65 / 1024d;
    //  1.2 Watt-Hours per Terabyte-Hour for SSD
    double ssd_gb_coefficient = 1.2 / 1024d;

    @Override
    @SuppressWarnings("unchecked")
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

        try {
            Map<String, Object> map = Utils.loadJSONResources("ccf/azure-storage.json");
            dataStoredUsageTypes = (List<String>) map.get("DATA_STORED_USAGE_TYPES");
            storageUsageUnitMultipliers = (Map<String, Integer>) map.get("STORAGE_USAGE_UNITS");
            managedDiskUsageUnitMultipliers = (Map<String, Integer>) map.get("MANAGED_DISK_USAGE_UNITS");
            replicationFactors = (Map<String, Integer>) map.get("REPLICATION_FACTORS");
            managedDisks = loadManagedDisks((Map<String, Object>) map.get("MANAGED_DISKS"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        if (QUANTITY.isNullAt(row)) {
            return;
        }

        String meterSubCategory = METER_SUBCATEGORY.getString(row);
        String unit = UNIT_OF_MEASURE.getString(row);
        double quantity = QUANTITY.getDouble(row);
        boolean isHDD = false;
        double gbHours;
        int replication;

        // Generic storage capacity meters already report GB-month or TB-month usage.
        // Managed Disks report a fraction of a provisioned disk-month, so resolve the
        // disk SKU first and use its provisioned capacity rather than used disk space.
        if (containsAny(dataStoredUsageTypes, meterName)) {
            double gbMonths = getGbMonths(quantity, unit);
            if (Double.isNaN(gbMonths)) {
                LOG.debug("Storage unit not found for {} {}", meterName, unit);
                return;
            }
            gbHours = Utils.Conversions.GBMonthsToGBHours(gbMonths);
            replication = getReplicationFactor(meterName);
        } else {
            ManagedDisk managedDisk = getManagedDisk(meterName, meterSubCategory);
            if (managedDisk == null || isDiskOperation(meterName)) {
                return;
            }
            gbHours = getManagedDiskGbHours(quantity, unit, managedDisk.sizeGb());
            isHDD = managedDisk.hdd();
            replication = replicationFactors.get(MANAGED_DISK_REPLICATION_FACTOR);
        }

        if (Double.isNaN(gbHours)) {
            LOG.debug("Storage unit not found for {} {}", meterName, unit);
            return;
        }

        computeEnergy(gbHours, isHDD, replication, enrichedValues);
    }

    double getGbMonths(double quantity, String unit) {
        Integer multiplier = storageUsageUnitMultipliers.get(unit);
        if (multiplier != null) {
            return quantity * multiplier;
        }
        return Double.NaN;
    }

    double getManagedDiskGbHours(double quantity, String unit, int diskSizeGb) {
        Integer multiplier = managedDiskUsageUnitMultipliers.get(unit);
        if (multiplier != null) {
            return quantity * multiplier * diskSizeGb * HOURS_PER_AZURE_PRICING_MONTH;
        }
        return Double.NaN;
    }

    ManagedDisk getManagedDisk(String meterName, String meterSubCategory) {
        if (meterSubCategory == null || !meterSubCategory.contains("Managed Disks")) {
            return null;
        }
        Matcher matcher = MANAGED_DISK_PATTERN.matcher(meterName);
        if (!matcher.find()) {
            return null;
        }
        return managedDisks.get(matcher.group(1));
    }

    boolean isDiskOperation(String meterName) {
        return meterName.contains("Operations");
    }

    int getReplicationFactor(String meterName) {
        if (meterName == null) {
            return replicationFactors.get("DEFAULT");
        }
        if (meterName.contains("GZRS")) {
            return replicationFactors.get("STORAGE_GZRS");
        }
        if (meterName.contains("LRS") || meterName.contains("ZRS")) {
            return meterName.contains("LRS") ?
                    replicationFactors.get("STORAGE_LRS") : replicationFactors.get("STORAGE_ZRS");
        }
        if (meterName.contains("RA-GRS") || meterName.contains("GRS")) {
            return replicationFactors.get("STORAGE_GRS");
        }
        return replicationFactors.get("DEFAULT");
    }

    private void computeEnergy(double gbHours, boolean isHDD, int replication, Map<Column, Object> enrichedValues) {
        double coefficient = isHDD ? hdd_gb_coefficient : ssd_gb_coefficient;
        double energyKwh = gbHours / 1000 * coefficient * replication;
        enrichedValues.put(ENERGY_USED, energyKwh);
    }

    @SuppressWarnings("unchecked")
    private Map<String, ManagedDisk> loadManagedDisks(Map<String, Object> disks) {
        Map<String, ManagedDisk> loaded = new HashMap<>();
        for (Map.Entry<String, Object> entry : disks.entrySet()) {
            Map<String, Object> values = (Map<String, Object>) entry.getValue();
            Number capacityGb = (Number) values.get("capacity_gb");
            String storageType = (String) values.get("storage_type");
            loaded.put(entry.getKey(), new ManagedDisk(
                    capacityGb.intValue(),
                    "HDD".equals(storageType)
            ));
        }
        return loaded;
    }

    record ManagedDisk(int sizeGb, boolean hdd) {
    }

    private static boolean containsAny(List<String> values, String candidate) {
        for (String value : values) {
            if (candidate.contains(value)) {
                return true;
            }
        }
        return false;
    }
}
