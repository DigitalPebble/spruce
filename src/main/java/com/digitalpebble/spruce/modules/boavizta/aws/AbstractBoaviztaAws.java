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
import static com.digitalpebble.spruce.CURColumn.PRODUCT_INSTANCE_TYPE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_SERVICE_CODE;
import static com.digitalpebble.spruce.CURColumn.USAGE_AMOUNT;

/**
 * AWS-specific extraction of the Boavizta instance type from a CUR or FOCUS row. Recognises EC2,
 * OpenSearch (ESDomain) and RDS instance lines and normalises the instance type the way the
 * Boavizta data expects it (stripping the {@code .search} suffix and the {@code db.} prefix).
 *
 * <p>FOCUS reports carry no {@code product_instance_type}; the instance type is recovered from
 * the {@code SkuMeter} column instead, whose values are the CUR usage types (e.g.
 * {@code EUW2-BoxUsage:t3.xlarge}): the operation gates select the instance lines and the
 * instance type is the part after the colon. {@code x_ServiceCode} carries the CUR
 * {@code line_item_product_code}, which for instance lines matches {@code product_servicecode}.
 *
 * <p>Subclasses only need to plug in the lookup variant via {@link #lookupImpacts(String)}.
 */
abstract class AbstractBoaviztaAws extends AbstractBoaviztaModule {

    private static final Column[] COLUMNS_NEEDED = new Column[]{
            PRODUCT_INSTANCE_TYPE, PRODUCT_SERVICE_CODE, LINE_ITEM_OPERATION, LINE_ITEM_PRODUCT_CODE, USAGE_AMOUNT
    };

    private static final Column[] COLUMNS_NEEDED_FOCUS = new Column[]{
            FOCUSColumn.SKU_METER, AWSFOCUSColumn.X_SERVICE_CODE, AWSFOCUSColumn.X_OPERATION, FOCUSColumn.CONSUMED_QUANTITY
    };

    private boolean focus = false;
    private RowColumn operation = LINE_ITEM_OPERATION;
    private RowColumn serviceCode = PRODUCT_SERVICE_CODE;
    private RowColumn productCode = LINE_ITEM_PRODUCT_CODE;
    private RowColumn usageAmount = USAGE_AMOUNT;

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
        } else {
            operation = LINE_ITEM_OPERATION;
            serviceCode = PRODUCT_SERVICE_CODE;
            productCode = LINE_ITEM_PRODUCT_CODE;
            usageAmount = USAGE_AMOUNT;
        }
    }

    @Override
    public final Column[] columnsNeeded() {
        return focus ? COLUMNS_NEEDED_FOCUS : COLUMNS_NEEDED;
    }

    @Override
    protected final double getUsageAmount(Row row) {
        return usageAmount.getDouble(row);
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
            if (instanceType.startsWith("db.")) {
                return instanceType.substring(3);
            }
            return instanceType;
        }
        return null;
    }
}
