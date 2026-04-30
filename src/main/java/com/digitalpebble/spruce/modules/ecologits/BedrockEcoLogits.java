// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Enrichment module estimating energy consumption and embodied emissions
 * for LLM inference on AWS Bedrock.
 * <p>
 * It extracts the model name from the CUR {@code product} map, looks up the
 * matching {@link EcoLogits} coefficients (per-1k-output-token), and applies
 * them to the token count derived from {@code usage_amount × pricing_unit}.
 * <p>
 * Coefficients only describe output tokens; this module decides how to score
 * input-token usage rows. By default it treats them as zero (the EcoLogits
 * methodology attributes ~all generation cost to output tokens). The
 * {@code input_to_output_energy_ratio} config key (default {@code 0.0}) lets
 * an operator override that — e.g. set to {@code 1.0} for a worst-case bound.
 * <p>
 * For ambiguous {@code line_item_usage_type} values (neither "input" nor
 * "output"), the existing {@code input_token_ratio} (default {@code 0.5})
 * splits the row before applying the input-to-output ratio.
 */
public class BedrockEcoLogits implements EnrichmentModule {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BedrockEcoLogits.class);

    private EcoLogits impacts;

    private double inputTokenRatio = 0.5;
    private double inputToOutputEnergyRatio = 0.0;

    @Override
    public void init(Map<String, Object> params) {
        if (impacts == null) {
            impacts = new EcoLogits();
            impacts.load();
        }

        if (params != null) {
            Number ratio = (Number) params.get("input_token_ratio");
            if (ratio != null) {
                inputTokenRatio = ratio.doubleValue();
            }
            Number i2o = (Number) params.get("input_to_output_energy_ratio");
            if (i2o != null) {
                inputToOutputEnergyRatio = i2o.doubleValue();
            }
        }
    }

    /** Test hook: inject a pre-built EcoLogits instance before {@link #init(Map)}. */
    void setEcoLogits(EcoLogits impacts) {
        this.impacts = impacts;
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
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String productCode = LINE_ITEM_PRODUCT_CODE.getString(row);
        if (!"AmazonBedrock".equals(productCode)) {
            return;
        }

        String modelId = Utils.getStringFromProductMap(row, "model", null);
        if (modelId == null || modelId.isEmpty()) {
            LOG.warn("BedrockEcoLogits: model key missing or empty in product map");
            return;
        }

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
        double totalTokens = usageAmount * tokenMultiplier;

        // Translate the row's tokens into an "effective output token" count.
        // EcoLogits attributes ~all generation cost to output tokens; input cost ≈ 0.
        boolean isInput = false;
        boolean isOutput = false;
        String usageType = LINE_ITEM_USAGE_TYPE.getString(row);
        if (usageType != null) {
            String lower = usageType.toLowerCase();
            isInput = lower.contains("input");
            isOutput = lower.contains("output");
        }

        double effectiveOutputTokens;
        if (isOutput && !isInput) {
            effectiveOutputTokens = totalTokens;
        } else if (isInput && !isOutput) {
            effectiveOutputTokens = totalTokens * inputToOutputEnergyRatio;
        } else {
            // Ambiguous: split the row, scale the input portion by the i2o ratio.
            double inputPortion = totalTokens * inputTokenRatio;
            double outputPortion = totalTokens * (1.0 - inputTokenRatio);
            effectiveOutputTokens = outputPortion + inputPortion * inputToOutputEnergyRatio;
        }

        double per1k = effectiveOutputTokens / 1_000.0;
        double energyKwh = per1k * modelImpacts.getEnergyKwhPer1kOutputTokens();
        double embodiedEmissions = per1k * modelImpacts.getGwpEmbodiedGPer1kOutputTokens();

        enrichedValues.put(ENERGY_USED, energyKwh);
        enrichedValues.put(EMBODIED_EMISSIONS, embodiedEmissions);

        LOG.debug("Bedrock model={} tokens={} effectiveOutput={} energy_kwh={} embodied_g={}",
                modelId, totalTokens, effectiveOutputTokens, energyKwh, embodiedEmissions);
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
