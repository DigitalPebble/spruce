// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

import com.digitalpebble.spruce.AWSFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.RowColumn;
import com.digitalpebble.spruce.Utils;
import com.digitalpebble.spruce.modules.boavizta.AbstractBoaviztaModule;
import org.apache.spark.sql.Row;

import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_OPERATION;
import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_PRODUCT_CODE;
import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_USAGE_TYPE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_INSTANCE_TYPE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_SERVICE_CODE;
import static com.digitalpebble.spruce.CURColumn.USAGE_AMOUNT;

/**
 * AWS-specific extraction of the Boavizta instance type from a CUR or FOCUS row. Recognises EC2,
 * OpenSearch (ESDomain), RDS, Amazon MQ and ElastiCache instance lines and normalises the instance
 * type the way the Boavizta data expects it (stripping the {@code .search} suffix and the
 * {@code db.}, {@code mq.} and {@code cache.} prefixes).
 *
 * <p>FOCUS reports carry no {@code product_instance_type}; the instance type is recovered from
 * the {@code SkuMeter} column instead, whose values are the CUR usage types (e.g.
 * {@code EUW2-BoxUsage:t3.xlarge}): the operation gates select the instance lines and the
 * instance type is the part after the colon. {@code x_ServiceCode} carries the CUR
 * {@code line_item_product_code}, which for instance lines matches {@code product_servicecode}.
 *
 * <p>Most services bill one instance-hour per running instance, but Amazon MQ bills one
 * <em>cluster</em>-hour whatever the deployment size: a three-node RabbitMQ cluster running for an
 * hour is a single unit of {@code USE1-RabbitMQ-3-InstanceUsage:mq.m5.large}, described in the CUR
 * as "3-node cluster mq.m5.large hour". The usage amount is scaled by the node count read back
 * from the usage type, otherwise such clusters would be counted as a single broker.
 *
 * <p>Subclasses only need to plug in the lookup variant via {@link #lookupImpacts(String)}.
 */
abstract class AbstractBoaviztaAws extends AbstractBoaviztaModule {

    private static final String INSTANCE_USAGE = "-InstanceUsage";
    private static final String MULTI_AZ = "-Multi-AZ";

    private static final Column[] COLUMNS_NEEDED = new Column[]{
            PRODUCT_INSTANCE_TYPE, PRODUCT_SERVICE_CODE, LINE_ITEM_OPERATION, LINE_ITEM_PRODUCT_CODE, USAGE_AMOUNT,
            LINE_ITEM_USAGE_TYPE
    };

    private static final Column[] COLUMNS_NEEDED_FOCUS = new Column[]{
            FOCUSColumn.SKU_METER, AWSFOCUSColumn.X_SERVICE_CODE, AWSFOCUSColumn.X_OPERATION, FOCUSColumn.CONSUMED_QUANTITY
    };

    private boolean focus = false;
    private RowColumn operation = LINE_ITEM_OPERATION;
    private RowColumn serviceCode = PRODUCT_SERVICE_CODE;
    private RowColumn productCode = LINE_ITEM_PRODUCT_CODE;
    private RowColumn usageAmount = USAGE_AMOUNT;
    private RowColumn usageType = LINE_ITEM_USAGE_TYPE;

    AbstractBoaviztaAws() {
        // The class is AWS-specific by definition; default the provider so callers that bypass
        // the provider-aware init still get correct behaviour.
        this.provider = Provider.AWS;
    }

    @Override
    public final void bindReportFormat(ReportFormat reportFormat) {
        focus = reportFormat == ReportFormat.FOCUS;
        if (focus) {
            operation = AWSFOCUSColumn.X_OPERATION;
            serviceCode = AWSFOCUSColumn.X_SERVICE_CODE;
            productCode = AWSFOCUSColumn.X_SERVICE_CODE;
            usageAmount = FOCUSColumn.CONSUMED_QUANTITY;
            usageType = FOCUSColumn.SKU_METER;
        } else {
            operation = LINE_ITEM_OPERATION;
            serviceCode = PRODUCT_SERVICE_CODE;
            productCode = LINE_ITEM_PRODUCT_CODE;
            usageAmount = USAGE_AMOUNT;
            usageType = LINE_ITEM_USAGE_TYPE;
        }
    }

    @Override
    public final Column[] columnsNeeded() {
        return focus ? COLUMNS_NEEDED_FOCUS : COLUMNS_NEEDED;
    }

    @Override
    protected final double getUsageAmount(Row row) {
        return usageAmount.getDouble(row) * nodeCount(this.usageType.getString(row));
    }

    /**
     * Number of instances covered by one billed unit of {@code usageType}. Amazon MQ encodes the
     * deployment size in the usage type: {@code RabbitMQ-3-InstanceUsage} is a three-node cluster
     * and {@code ActiveMQ-Multi-AZ-InstanceUsage} an active/standby pair, while a single-broker
     * deployment carries no marker at all. Everything else bills per instance, so returns 1.
     */
    static int nodeCount(String usageType) {
        if (usageType == null) {
            return 1;
        }
        int marker = usageType.indexOf(INSTANCE_USAGE);
        if (marker < 0) {
            return 1;
        }
        // a digit run immediately before "-InstanceUsage" is the node count
        int end = marker;
        int start = end;
        while (start > 0 && Character.isDigit(usageType.charAt(start - 1))) {
            start--;
        }
        if (start < end && start > 0 && usageType.charAt(start - 1) == '-') {
            return Integer.parseInt(usageType.substring(start, end));
        }
        if (usageType.regionMatches(true, Math.max(0, marker - MULTI_AZ.length()), MULTI_AZ, 0, MULTI_AZ.length())) {
            return 2;
        }
        return 1;
    }

    @Override
    protected final String extractInstanceType(Row row) {
        String instanceType = focus
                ? Utils.instanceTypeFromUsageType(FOCUSColumn.SKU_METER.getString(row))
                : PRODUCT_INSTANCE_TYPE.getString(row);
        if (instanceType == null) {
            return null;
        }

        final String serviceCode = this.serviceCode.getString(row);
        final String operation = this.operation.getString(row);
        final String productCode = this.productCode.getString(row);

        if (operation == null || productCode == null) {
            return null;
        }

        if (productCode.equals("AmazonEC2") && operation.startsWith("RunInstances") && "AmazonEC2".equals(serviceCode)) {
            return instanceType;
        }
        if (productCode.equals("AmazonES") && operation.equals("ESDomain")) {
            if (instanceType.endsWith(".search")) {
                return instanceType.substring(0, instanceType.length() - ".search".length());
            }
            return instanceType;
        }
        if (productCode.equals("AmazonRDS") && operation.startsWith("CreateDBInstance")) {
            return stripPrefix(instanceType, "db.");
        }
        // Amazon MQ and ElastiCache report the broker/node shape as an EC2 instance type behind a
        // service prefix. In a CUR product_instance_type already has it stripped for MQ but not for
        // ElastiCache; in a FOCUS report both keep it, as the value comes from the usage type.
        if (productCode.equals("AmazonMQ") && operation.startsWith("CreateBroker")) {
            return stripPrefix(instanceType, "mq.");
        }
        if (productCode.equals("AmazonElastiCache") && operation.startsWith("CreateCacheCluster")) {
            return stripPrefix(instanceType, "cache.");
        }
        return null;
    }

    private static String stripPrefix(String instanceType, String prefix) {
        return instanceType.startsWith(prefix) ? instanceType.substring(prefix.length()) : instanceType;
    }
}
