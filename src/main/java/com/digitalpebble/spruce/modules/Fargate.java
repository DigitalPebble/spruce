// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.modules.ccf.Storage;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.CURColumn.PRICING_UNIT;
import static com.digitalpebble.spruce.CURColumn.PRODUCT_SERVICE_CODE;
import static com.digitalpebble.spruce.SpruceColumn.*;
import static com.digitalpebble.spruce.Utils.loadJSONResources;

/**
 *  Estimates the energy usage for CPU and memory of Fargate
 *  Based on the TailPipe methodology
 *  https://tailpipe.ai/methodology/serverless-explained/
 *  as of 08/10/2025
 **/
public class Fargate implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(Fargate.class);

    // https://tailpipe.ai/methodology/serverless-explained/
    // in kWh per GB
    Double memory_coefficient_kwh = 0.0000598d;

    // https://tailpipe.ai/methodology/serverless-explained/
    // in kWh per vCPU
    //  (150 * 0.815) /64 / 1000
    Double arm_cpu_coefficient_kwh = 0.00191015625;

    // https://tailpipe.ai/methodology/serverless-explained/
    // in kWh per vCPU
    //  (173 * 0.815) /16 / 1000
    Double x86_cpu_coefficient_kwh = 0.00220304687;

    @Override
    public void init(Map<String, Object> params) {
        Double coef = (Double) params.get("memory_coefficient_kwh");
        if (coef != null) {
            memory_coefficient_kwh = coef;
        }
        coef = (Double) params.get("x86_cpu_coefficient_kwh");
        if (coef != null) {
            x86_cpu_coefficient_kwh = coef;
        }
        coef = (Double) params.get("arm_cpu_coefficient_kwh");
        if (coef != null) {
            arm_cpu_coefficient_kwh = coef;
        }
        log.info("memory_coefficient: {}", memory_coefficient_kwh);
        log.info("x86_cpu_coefficient: {}", x86_cpu_coefficient_kwh);
        log.info("arm_cpu_coefficient: {}", arm_cpu_coefficient_kwh);
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{LINE_ITEM_OPERATION, USAGE_AMOUNT, LINE_ITEM_USAGE_TYPE};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public Row process(Row row) {
        String operation = LINE_ITEM_OPERATION.getString(row);
        if (!"FargateTask".equals(operation)) {
            return row;
        }

        String usage_type = LINE_ITEM_USAGE_TYPE.getString(row);
        if (usage_type == null) {
            return row;
        }

        // memory
        if (usage_type.endsWith("-GB-Hours")) {
            double amount_gb = USAGE_AMOUNT.getDouble(row);
            double energy = amount_gb * memory_coefficient_kwh;
            return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, energy);
        }

        // cpu
        if (usage_type.endsWith("-vCPU-Hours:perCPU")) {
            double amount_vcpu = USAGE_AMOUNT.getDouble(row);
            boolean isARM = usage_type.contains("-ARM-");
            double coefficient = isARM? arm_cpu_coefficient_kwh : x86_cpu_coefficient_kwh;
            double energy = amount_vcpu * coefficient;
            return EnrichmentModule.withUpdatedValue(row, ENERGY_USED, energy);
        }

        return row;
    }
}
