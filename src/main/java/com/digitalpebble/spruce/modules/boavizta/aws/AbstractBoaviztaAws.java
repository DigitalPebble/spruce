// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.aws;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.modules.boavizta.AbstractBoaviztaModule;
import org.apache.spark.sql.Row;

import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_OPERATION;
import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_PRODUCT_CODE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_INSTANCE_TYPE;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_SERVICE_CODE;
import static com.digitalpebble.spruce.CURColumn.USAGE_AMOUNT;

/**
 * AWS-specific extraction of the Boavizta instance type from a CUR row. Recognises EC2,
 * OpenSearch (ESDomain) and RDS instance lines and normalises the instance type the way the
 * Boavizta data expects it (stripping the {@code .search} suffix and the {@code db.} prefix).
 *
 * <p>Subclasses only need to plug in the lookup variant via {@link #lookupImpacts(String)}.
 */
abstract class AbstractBoaviztaAws extends AbstractBoaviztaModule {

    private static final Column[] COLUMNS_NEEDED = new Column[]{
            PRODUCT_INSTANCE_TYPE, PRODUCT_SERVICE_CODE, LINE_ITEM_OPERATION, LINE_ITEM_PRODUCT_CODE, USAGE_AMOUNT
    };

    AbstractBoaviztaAws() {
        // The class is AWS-specific by definition; default the provider so callers that bypass
        // the provider-aware init still get correct behaviour.
        this.provider = Provider.AWS;
    }

    @Override
    public final Column[] columnsNeeded() {
        return COLUMNS_NEEDED;
    }

    @Override
    protected final String extractInstanceType(Row row) {
        String instanceType = PRODUCT_INSTANCE_TYPE.getString(row);
        if (instanceType == null) {
            return null;
        }

        final String serviceCode = PRODUCT_SERVICE_CODE.getString(row);
        final String operation = LINE_ITEM_OPERATION.getString(row);
        final String productCode = LINE_ITEM_PRODUCT_CODE.getString(row);

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
