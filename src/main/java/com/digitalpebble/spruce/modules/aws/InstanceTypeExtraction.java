// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.aws;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.regex.Pattern;

import static com.digitalpebble.spruce.CURColumn.PRODUCT_INSTANCE_TYPE;
import static com.digitalpebble.spruce.SpruceColumn.INSTANCE_TYPE;

/**
 * Copies the AWS instance type (e.g. {@code m5.large}) into the provider-neutral
 * {@code instance_type} output column, so reporting does not need per-format extraction logic.
 *
 * <p>CUR reports carry it in {@code product_instance_type}; FOCUS reports carry the CUR usage
 * type as {@code SkuMeter} (e.g. {@code EUW2-BoxUsage:t3.xlarge}) and the part after the colon
 * is kept when it is shaped like an instance type. The shape check filters out the non-instance
 * meters that also contain a colon (e.g. {@code EBS:VolumeUsage.gp3},
 * {@code Fargate-vCPU-Hours:perCPU}): on a real CUR it reproduced {@code product_instance_type}
 * for all usage types carrying one, the only additions being genuine instance types the CUR
 * leaves blank (AmazonMQ brokers, marketplace {@code SoftwareUsage}).
 **/
public class InstanceTypeExtraction implements EnrichmentModule {

    private static final Pattern INSTANCE_TYPE_SHAPE = Pattern.compile(
            "([a-z0-9]+\\.)?[a-z][a-z0-9-]*\\.(nano|micro|small|medium|large|\\d*xlarge|metal[a-z0-9-]*)");

    private boolean focus = false;

    @Override
    public boolean processesAllRows() {
        // metadata, not an impact estimate: savings plan negation rows must carry the instance
        // type too so per-instance cost aggregations net out
        return true;
    }

    @Override
    public void bindReportFormat(ReportFormat reportFormat) {
        focus = reportFormat == ReportFormat.FOCUS;
    }

    @Override
    public Column[] columnsNeeded() {
        return focus ? new Column[]{FOCUSColumn.SKU_METER} : new Column[]{PRODUCT_INSTANCE_TYPE};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{INSTANCE_TYPE};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String instanceType;
        if (focus) {
            instanceType = Utils.instanceTypeFromUsageType(FOCUSColumn.SKU_METER.getString(row));
            if (instanceType != null && !INSTANCE_TYPE_SHAPE.matcher(instanceType).matches()) {
                instanceType = null;
            }
        } else {
            instanceType = PRODUCT_INSTANCE_TYPE.getString(row);
        }
        if (instanceType != null && !instanceType.isEmpty()) {
            enrichedValues.put(INSTANCE_TYPE, instanceType);
        }
    }
}
