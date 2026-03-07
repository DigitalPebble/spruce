// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Enrichment module estimating energy consumption and embodied emissions
 * for LLM inference on AWS Bedrock.
 * <p>
 * It extracts the model name from the CUR {@code product} map, determines token
 * types (input/output) via {@code line_item_usage_type}, and retrieves coefficients
 * using {@link EcoLogits}. If the usage type is ambiguous, it falls back to
 * a configurable {@code input_token_ratio} (default 0.5).
 * <p>
 * Usage amounts are normalized to individual tokens based on the {@code pricing_unit}
 * before applying the per-1K-token coefficients.
 */
public class BedrockEcoLogits implements EnrichmentModule {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BedrockEcoLogits.class);

    private EcoLogits impacts;

    private double inputTokenRatio = 0.5;

    @Override
    public void init(Map<String, Object> params) {
        impacts = new EcoLogits();
        impacts.load();

        if (params != null) {
            Number ratio = (Number) params.get("input_token_ratio");
            if (ratio != null) {
                inputTokenRatio = ratio.doubleValue();
            }
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{LINE_ITEM_PRODUCT_CODE, PRODUCT, USAGE_AMOUNT, PRICING_UNIT, LINE_ITEM_USAGE_TYPE};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED, EMBODIED_EMISSIONS};
    }

    @Override
    @SuppressWarnings("unchecked")
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String productCode = LINE_ITEM_PRODUCT_CODE.getString(row);
        if (!"AmazonBedrock".equals(productCode)) {
            return;
        }

        int productIndex = PRODUCT.resolveIndex(row);
        if (row.isNullAt(productIndex)) {
            return;
        }

        // extract the model from Scala Map
        Object productObj = row.get(productIndex);
        Map<String, String> productMap;
        if (productObj instanceof scala.collection.Map) {
            productMap = JavaConverters.mapAsJavaMapConverter((scala.collection.Map<String, String>) productObj).asJava();
        } else if (productObj instanceof Map) {
            productMap = (Map<String, String>) productObj;
        } else  {
            return;
        }

        String modelId = productMap.get("modelId");
        if(modelId == null || modelId.isEmpty()) return;

        EcoLogits.ModelImpacts modelImpacts = impacts.getImpacts(modelId);
        if (modelImpacts == null) {
            return;
        }

        if (USAGE_AMOUNT.isNullAt(row)) {
            return;
        }
        double usageAmount = USAGE_AMOUNT.getDouble(row);
        if (usageAmount <= 0) {
            return;
        }

        double tokenMultiplier = parseTokenMultiplier(PRICING_UNIT.getString(row));
        double totalTokens= usageAmount * tokenMultiplier;

        // split into input/output tokens
        double inputTokens = totalTokens * inputTokenRatio;
        double outputTokens = totalTokens * (1.0 - inputTokenRatio);

        double energyKwh = (inputTokens / 1_000.0) * modelImpacts.getEnergyKwhPer1kInputTokens()
                + (outputTokens / 1_000.0) * modelImpacts.getEnergyKwhPer1kOutputTokens();

        // calculate embodied emissions
        double embodiedEmissions = (totalTokens / 1_000.0) * modelImpacts.getEmbodiedCo2eGPer1kTokens();

        enrichedValues.put(ENERGY_USED, energyKwh);
        enrichedValues.put(EMBODIED_EMISSIONS, embodiedEmissions);

        LOG.debug("Bedrock model={} tokens={} energy_kwh={} embodied_g={}", modelId, totalTokens, energyKwh, embodiedEmissions);
    }

    /**
     * Parses the CUR {@code pricing_unit} to determine how many individual tokens
     * one usage-amount unit represents. (e.g. "1M tokens" → 1_000_000)
     */
    static double parseTokenMultiplier(String pricingUnit) {
        if (pricingUnit == null || pricingUnit.isBlank()) {
            return 1.0;
        }
        String lower = pricingUnit.trim().toLowerCase();
        if (!lower.contains("token")) {
            return 1.0;
        }
        if (lower.startsWith("1m")) {
            return 1_000_000.0;
        }
        if (lower.startsWith("1k")) {
            return 1_000.0;
        }
        return 1.0;
    }

}