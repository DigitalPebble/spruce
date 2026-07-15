// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ccf.aws;

import com.digitalpebble.spruce.AWSFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.RowColumn;
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
 * <p>The values read (operations, usage types, units) are identical in CUR and FOCUS reports,
 * only the column labels differ: {@link #bindReportFormat(ReportFormat)} selects the bindings.
 * Note that {@code x_ServiceCode} carries the CUR {@code line_item_product_code}, which matches
 * {@code product_servicecode} for the storage services handled here.
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#storage">CCF methodology</a>
 * @see <a href="https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/9f2cf436e5ad020830977e52c3b0a1719d20a8b9/packages/aws/src/lib/CostAndUsageTypes.ts#L25">resource file</a>
 **/
public class Storage implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(Storage.class);

    protected RowColumn operation = LINE_ITEM_OPERATION;
    protected RowColumn usageType = LINE_ITEM_USAGE_TYPE;
    protected RowColumn usageAmount = USAGE_AMOUNT;
    protected RowColumn serviceCode = PRODUCT_SERVICE_CODE;
    protected RowColumn pricingUnit = PRICING_UNIT;
    /** Only used for debug logging; absent from FOCUS reports. */
    protected RowColumn productFamily = PRODUCT_PRODUCT_FAMILY;

    @Override
    public void bindReportFormat(ReportFormat reportFormat) {
        if (reportFormat == ReportFormat.FOCUS) {
            operation = AWSFOCUSColumn.X_OPERATION;
            usageType = FOCUSColumn.SKU_METER;
            usageAmount = FOCUSColumn.CONSUMED_QUANTITY;
            serviceCode = AWSFOCUSColumn.X_SERVICE_CODE;
            pricingUnit = FOCUSColumn.PRICING_UNIT;
            productFamily = null;
        } else {
            operation = LINE_ITEM_OPERATION;
            usageType = LINE_ITEM_USAGE_TYPE;
            usageAmount = USAGE_AMOUNT;
            serviceCode = PRODUCT_SERVICE_CODE;
            pricingUnit = PRICING_UNIT;
            productFamily = PRODUCT_PRODUCT_FAMILY;
        }
    }

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
        return new Column[]{operation, usageAmount, usageType, serviceCode, pricingUnit};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        final String operation = this.operation.getString(row);
        if (operation == null) {
            return;
        }

        // implement the logic from CCF
        // first check that the unit corresponds to storage
        final String unit = this.pricingUnit.getString(row);
        if (unit == null || !units.contains(unit)) {
            return;
        }

        final String usage_type = this.usageType.getString(row);
        if (usage_type == null) {
            return;
        }

        final String serviceCode = this.serviceCode.getString(row);
        int replication = getReplicationFactor(serviceCode, usage_type);

        // loop on the values from the resources
        for (String ssd : ssd_usage_types) {
            if (usage_type.endsWith(ssd)) {
                computeEnergy(row, enrichedValues, false, replication);
                return;
            }
        }

        // check the services
        // https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/9f2cf436e5ad020830977e52c3b0a1719d20a8b9/packages/aws/src/lib/CostAndUsageReports.ts#L518
        if (serviceCode != null && !usage_type.contains("Backup")) {
            for (String service : ssd_services) {
                if (serviceCode.endsWith(service)) {
                    computeEnergy(row, enrichedValues, false, replication);
                    return;
                }
            }
        }

        for (String hdd : hdd_usage_types) {
            if (usage_type.endsWith(hdd)) {
                computeEnergy(row, enrichedValues, true, replication);
                return;
            }
        }

        // Log so that can improve coverage in the longer term
        String product_product_family = productFamily != null ? productFamily.getString(row) : null;
        if ("Storage".equals(product_product_family)) {
            log.debug("Storage type not found for {} {}", operation, usage_type);
        }
    }


    private void computeEnergy(Row row, Map<Column, Object> enrichedValues, boolean isHDD, int replication) {
        double coefficient = isHDD ? hdd_gb_coefficient : ssd_gb_coefficient;
        double amount = usageAmount.getDouble(row);
        String unit = pricingUnit.getString(row);
        // normalisation
        if (!"GB-Hours".equals(unit)) {
           // it is in GBMonth
            amount = Utils.Conversions.GBMonthsToGBHours(amount);
        }
        //  to kwh
        double energy_kwh = amount /1000 * coefficient * replication;
        enrichedValues.put(ENERGY_USED, energy_kwh);
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
