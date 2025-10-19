// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static com.digitalpebble.spruce.Utils.loadJSONResources;

/**
 * Provides an estimate of energy used for storage.
 * Applies a flat coefficient per Gb
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#storage">CCF methodology</a>
 * @see <a href="https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/9f2cf436e5ad020830977e52c3b0a1719d20a8b9/packages/aws/src/lib/CostAndUsageTypes.ts#L25">resource file</a>
 **/
public class Storage implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(Storage.class);

    //  0.65 Watt-Hours per Terabyte-Hour for HDD
    double hdd_gb_coefficient = 0.65 / 1024d;
    //  1.2 Watt-Hours per Terabyte-Hour for SSD
    double ssd_gb_coefficient = 1.2 / 1024d;

    List<String> ssd_usage_types;
    List<String> hdd_usage_types;
    List<String> ssd_services;
    List<String> units;
    Map<String, Integer> replication_factors;

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

        log.info("hdd_gb_coefficient: {}", hdd_gb_coefficient);
        log.info("ssd_gb_coefficient: {}", ssd_gb_coefficient);

        try {
            Map<String, Object> map = loadJSONResources("ccf/storage.json");
            ssd_usage_types = (List<String>) map.get("SSD_USAGE_TYPES");
            hdd_usage_types = (List<String>) map.get("HDD_USAGE_TYPES");
            ssd_services = (List<String>) map.get("SSD_SERVICES");
            units = (List<String>) map.get("KNOWN_USAGE_UNITS");
            replication_factors = (Map<String, Integer>) map.get("REPLICATION_FACTORS");
        } catch (
                IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{LINE_ITEM_OPERATION, USAGE_AMOUNT, LINE_ITEM_USAGE_TYPE, PRODUCT_SERVICE_CODE, PRICING_UNIT};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public Row process(Row row) {
        final String operation = LINE_ITEM_OPERATION.getString(row);
        if (operation == null) {
            return row;
        }

        // implement the logic from CCF
        // first check that the unit corresponds to storage
        final String unit = PRICING_UNIT.getString(row);
        if (unit == null || !units.contains(unit)) {
            return row;
        }

        final String usage_type = LINE_ITEM_USAGE_TYPE.getString(row);
        if (usage_type == null) {
            return row;
        }

        final String serviceCode = PRODUCT_SERVICE_CODE.getString(row);
        int replication = getReplicationFactor(serviceCode, usage_type);

        // loop on the values from the resources
        for (String ssd : ssd_usage_types) {
            if (usage_type.endsWith(ssd)) {
                return enrich(row, false, replication);
            }
        }

        // check the services
        // https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/9f2cf436e5ad020830977e52c3b0a1719d20a8b9/packages/aws/src/lib/CostAndUsageReports.ts#L518
        if (serviceCode != null && !usage_type.contains("Backup")) {
            for (String service : ssd_services) {
                if (serviceCode.endsWith(service)) {
                    return enrich(row, false, replication);
                }
            }
        }

        for (String hdd : hdd_usage_types) {
            if (usage_type.endsWith(hdd)) {
                return enrich(row, true, replication);
            }
        }

        // Log so that can improve coverage in the longer term
        String product_product_family = PRODUCT_PRODUCT_FAMILY.getString(row);
        if ("Storage".equals(product_product_family)) {
            log.debug("Storage type not found for {} {}", operation, usage_type);
        }

        // not been found
        return row;
    }


    private Row enrich(Row row, boolean isHDD, int replication) {
        double coefficient = isHDD ? hdd_gb_coefficient : ssd_gb_coefficient;
        double amount = USAGE_AMOUNT.getDouble(row);
        String unit = PRICING_UNIT.getString(row);
        // normalisation
        if (!"GB-Hours".equals(unit)) {
           // it is in GBMonth
            amount = Utils.Conversions.GBMonthsToGBHours(amount);
        }
        //  to kwh
        double energy_kwh = amount /1000 * coefficient * replication;
        return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, energy_kwh);
    }

    /**
     * Get replication factor based on AWS service and usage type.
     */
    public int getReplicationFactor(String service, String usageType) {
        if (service == null || usageType == null) {
            return replication_factors.get("DEFAULT");
        }

        switch (service) {
            case "AmazonS3":
                if (containsAny(usageType, "TimedStorage-ZIA", "EarlyDelete-ZIA", "TimedStorage-RRS"))
                    return replication_factors.get("S3_ONE_ZONE_REDUCED_REDUNDANCY");
                if (containsAny(usageType, "TimedStorage", "EarlyDelete"))
                    return replication_factors.get("S3");
                return replication_factors.get("DEFAULT");

            case "AmazonEC2":
                if (usageType.contains("VolumeUsage"))
                    return replication_factors.get("EC2_EBS_VOLUME");
                if (usageType.contains("SnapshotUsage"))
                    return replication_factors.get("EC2_EBS_SNAPSHOT");
                return replication_factors.get("DEFAULT");

            case "AmazonEFS":
                return usageType.contains("ZIA") ?
                        replication_factors.get("EFS_ONE_ZONE") : replication_factors.get("EFS");

            case "AmazonRDS":
                if (usageType.contains("BackupUsage"))
                    return replication_factors.get("RDS_BACKUP");
                if (usageType.contains("Aurora"))
                    return replication_factors.get("RDS_AURORA");
                if (usageType.contains("Multi-AZ"))
                    return replication_factors.get("RDS_MULTI_AZ");
                return replication_factors.get("DEFAULT");

            case "AmazonDocDB":
                return usageType.contains("BackupUsage") ?
                        replication_factors.get("DOCUMENT_DB_BACKUP") : replication_factors.get("DOCUMENT_DB_STORAGE");

            case "AmazonDynamoDB":
                return replication_factors.get("DYNAMO_DB");

            case "AmazonECR":
                return usageType.contains("TimedStorage") ?
                        replication_factors.get("ECR_STORAGE") : replication_factors.get("DEFAULT");

            case "AmazonElastiCache":
                return usageType.contains("BackupUsage") ?
                        replication_factors.get("DOCUMENT_ELASTICACHE_BACKUP") : replication_factors.get("DEFAULT");

            case "AmazonSimpleDB":
                return usageType.contains("TimedStorage") ?
                        replication_factors.get("SIMPLE_DB") : replication_factors.get("DEFAULT");

            default:
                return replication_factors.get("DEFAULT");
        }
    }

    private static boolean containsAny(String usageType, String... patterns) {
        for (String p : patterns) if (usageType.contains(p)) return true;
        return false;
    }

}