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
import static com.digitalpebble.spruce.SpruceColumn.EMBODIED_EMISSIONS;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;
import static com.digitalpebble.spruce.Utils.loadJSONResources;

/**
 * Provides an estimate of energy used for storage, and of the embodied emissions of the drives
 * holding it. Applies a flat coefficient per Gb
 *
 * <p>Embodied emissions are amortised over a five year service life. The two media are modelled
 * on different bases because they behave differently: a hard drive costs roughly the same to
 * manufacture whatever its capacity, since the platters, motor, actuator, casing and PCB are
 * near-fixed for a 3.5" unit and areal density does the work, whereas an SSD's die area scales
 * with capacity. Hence a constant per drive for HDD and a rate per GB for SSD.
 *
 * <p>The 30 kg CO2eq per drive is the convergence point of four independent sources spanning a
 * 40x range of drive capacities, which is itself the evidence for treating it as capacity
 * independent: Boavizta / Umweltbundesamt <i>Green Cloud Computing</i> 2021 (31.11 kg per unit),
 * a Seagate Exos X22 LCA (28.7 kg for a 22 TB drive), Seagate's published 0.27 kg per TB-year,
 * and Tannu &amp; Nair's meta-analysis of 24 vendor LCAs (0.02 kg/GB over a 512 GB to 6 TB
 * sample). Note that the last of those cannot be used as a per-GB rate on modern hardware: it
 * encodes the drive sizes of a pre-2023 corpus and overstates current per-byte embodied carbon
 * by an order of magnitude.
 *
 * <p>The 0.055 kg CO2eq per GB for SSD is where Boavizta's die-area formula (0.052) and the 2025
 * <i>Embodied Carbon Footprint of 3D NAND Memories</i> study (0.056) agree. Tannu &amp; Nair's
 * 0.16 kg/GB is roughly 3x higher because 3D NAND layer scaling has cut per-GB manufacturing
 * carbon since their corpus closed.
 *
 * <p>The assumed 15 TB drive is the installed-fleet average implied by Backblaze's 2025 Drive
 * Stats, which is the right basis for bytes sitting on hardware bought over several years;
 * current nearline shipments average nearer 22 TB, which would give 0.27 kg per TB-year instead
 * of 0.40. Both figures are configurable.
 *
 * <p>The values read (operations, usage types, units) are identical in CUR and FOCUS reports,
 * only the column labels differ: {@link #bindReportFormat(ReportFormat)} selects the bindings.
 * Note that {@code x_ServiceCode} carries the CUR {@code line_item_product_code}, which matches
 * {@code product_servicecode} for the storage services handled here.
 *
 * @see <a href="https://www.cloudcarbonfootprint.org/docs/methodology#storage">CCF methodology</a>
 * @see <a href="https://github.com/cloud-carbon-footprint/cloud-carbon-footprint/blob/9f2cf436e5ad020830977e52c3b0a1719d20a8b9/packages/aws/src/lib/CostAndUsageTypes.ts#L25">resource file</a>
 * @see <a href="https://doc.api.boavizta.org/Explanations/components/hdd/">Boavizta HDD embodied impacts</a>
 * @see <a href="https://doc.api.boavizta.org/Explanations/components/ssd/">Boavizta SSD embodied impacts</a>
 * @see <a href="https://tailpipe.ai/methodology/embodied-emissions-methodology-manufacture/">Tailpipe manufacture methodology, incl. the Exos X22 LCA</a>
 * @see <a href="https://www.seagate.com/blog/hard-drives-the-key-to-data-center-sustainability/">Seagate, embodied carbon per TB-year</a>
 * @see <a href="https://arxiv.org/pdf/2207.10793">Tannu &amp; Nair, The Dirty Secret of SSDs: Embodied Carbon</a>
 * @see <a href="https://www.backblaze.com/blog/backblaze-drive-stats-for-2025/">Backblaze Drive Stats 2025, fleet capacity mix</a>
 * @see <a href="https://github.com/DigitalPebble/spruce/issues/102">issue #102</a>
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

    /** Embodied emissions of one hard drive, in kg CO2eq; see the class javadoc for why this is
     *  a constant per drive rather than a rate per byte. */
    double hdd_embodied_kg_per_drive = 30d;
    /** Capacity assumed for one hard drive, in GB. */
    double hdd_capacity_gb = 15_000d;
    /** Embodied emissions of an SSD, in kg CO2eq per GB of capacity. */
    double ssd_embodied_kg_per_gb = 0.055d;
    /** Service life over which embodied emissions are amortised, in hours (5 years). */
    double storage_lifetime_hours = 43_800d;

    /** Grams CO2eq per GB-hour of stored data, derived in {@link #init(Map)}. */
    double hdd_embodied_g_per_gb_hour;
    double ssd_embodied_g_per_gb_hour;

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

        hdd_embodied_kg_per_drive = Utils.doubleParam(params, "hdd_embodied_kg_per_drive", hdd_embodied_kg_per_drive);
        hdd_capacity_gb = Utils.doubleParam(params, "hdd_capacity_gb", hdd_capacity_gb);
        ssd_embodied_kg_per_gb = Utils.doubleParam(params, "ssd_embodied_kg_per_gb", ssd_embodied_kg_per_gb);
        storage_lifetime_hours = Utils.doubleParam(params, "storage_lifetime_hours", storage_lifetime_hours);

        hdd_embodied_g_per_gb_hour =
                hdd_embodied_kg_per_drive * 1000d / (hdd_capacity_gb * storage_lifetime_hours);
        ssd_embodied_g_per_gb_hour = ssd_embodied_kg_per_gb * 1000d / storage_lifetime_hours;

        log.info("hdd_gb_coefficient: {}", hdd_gb_coefficient);
        log.info("ssd_gb_coefficient: {}", ssd_gb_coefficient);
        log.info("hdd_embodied_g_per_gb_hour: {}", hdd_embodied_g_per_gb_hour);
        log.info("ssd_embodied_g_per_gb_hour: {}", ssd_embodied_g_per_gb_hour);

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
        return new Column[]{ENERGY_USED, EMBODIED_EMISSIONS};
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
        // the replication factor applies to the hardware as well as to the energy: the same bytes
        // occupy that many times more physical drives, and so that much more embodied carbon
        double embodied_coefficient = isHDD ? hdd_embodied_g_per_gb_hour : ssd_embodied_g_per_gb_hour;
        enrichedValues.put(EMBODIED_EMISSIONS, amount * embodied_coefficient * replication);
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
