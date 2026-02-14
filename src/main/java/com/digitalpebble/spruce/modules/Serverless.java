// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 *  Estimates the energy usage for CPU and memory of serverless services
 *  such as Fargate or EMR
 *  Based on the TailPipe methodology
 *  https://tailpipe.ai/methodology/serverless-explained/
 *  as of 08/10/2025
 **/
public class Serverless implements EnrichmentModule {

    private static final Logger log = LoggerFactory.getLogger(Serverless.class);

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
    Double x86_cpu_coefficient_kwh = 0.0088121875;

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
    public void enrich(Row inputRow, Map<Column, Object> enrichedValues) {
        String usage_type = LINE_ITEM_USAGE_TYPE.getString(inputRow);
        if (usage_type == null) {
            return;
        }

        String operation = LINE_ITEM_OPERATION.getString(inputRow);
        if ("FargateTask".equals(operation)) {
            // memory
            if (usage_type.endsWith("-GB-Hours")) {
                double amount_gb = USAGE_AMOUNT.getDouble(inputRow);
                double energy = amount_gb * memory_coefficient_kwh;
                enrichedValues.put(ENERGY_USED, energy);
                return;
            }

            // cpu
            if (usage_type.endsWith("-vCPU-Hours:perCPU")) {
                double amount_vcpu = USAGE_AMOUNT.getDouble(inputRow);
                boolean isARM = usage_type.contains("-ARM-");
                double coefficient = isARM ? arm_cpu_coefficient_kwh : x86_cpu_coefficient_kwh;
                double energy = amount_vcpu * coefficient;
                enrichedValues.put(ENERGY_USED, energy);
                return;
            }
        }
        else if (usage_type.contains("EMR-SERVERLESS")) {
            if (usage_type.endsWith("MemoryGBHours")) {
                double amount_gb = USAGE_AMOUNT.getDouble(inputRow);
                double energy = amount_gb * memory_coefficient_kwh;
                enrichedValues.put(ENERGY_USED, energy);
                return;
            }

            // cpu
            if (usage_type.endsWith("-vCPUHours")) {
                double amount_vcpu = USAGE_AMOUNT.getDouble(inputRow);
                boolean isARM = usage_type.contains("-ARM-");
                double coefficient = isARM ? arm_cpu_coefficient_kwh : x86_cpu_coefficient_kwh;
                double energy = amount_vcpu * coefficient;
                enrichedValues.put(ENERGY_USED, energy);
                return;
            }
        }
    }
}
