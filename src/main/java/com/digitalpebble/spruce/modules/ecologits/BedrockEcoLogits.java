// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.*;
import static com.digitalpebble.spruce.SpruceColumn.*;

/**
 * Enrichment module estimating energy consumption and embodied emissions
 * for LLM inference on AWS Bedrock.
 * <p>
 * It extracts the model key and token type from {@code line_item_usage_type}
 * (format: {@code {REGION}-{ModelKey}-{input|output}-tokens[-batch]}), looks
 * up the matching {@link EcoLogits} coefficients (per-1k-output-token), and
 * applies them to the token count derived from {@code usage_amount × pricing_unit}.
 * <p>
 * Coefficients only describe output tokens; input-token rows are ignored
 * (the EcoLogits methodology attributes ~all generation cost to output tokens).
 */
public class BedrockEcoLogits implements EnrichmentModule {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BedrockEcoLogits.class);

    private EcoLogits impacts;

    @Override
    public void init(Map<String, Object> params) {
        if (impacts == null) {
            impacts = new EcoLogits();
            impacts.load();
        }
    }

    /** Test hook: inject a pre-built EcoLogits instance before {@link #init(Map)}. */
    void setEcoLogits(EcoLogits impacts) {
        this.impacts = impacts;
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{LINE_ITEM_PRODUCT_CODE, USAGE_AMOUNT, PRICING_UNIT, LINE_ITEM_USAGE_TYPE};
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

        String usageType = LINE_ITEM_USAGE_TYPE.getString(row);
        String[] parsed = parseUsageType(usageType);
        if (parsed == null || "input".equals(parsed[1])) {
            return;
        }

        String modelKey = parsed[0];

        EcoLogits.ModelImpacts modelImpacts = impacts.getImpacts(modelKey);
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

        double per1k = totalTokens / 1_000.0;
        double energyKwh = per1k * modelImpacts.getEnergyKwhPer1kOutputTokens();
        double embodiedEmissions = per1k * modelImpacts.getGwpEmbodiedGPer1kOutputTokens();

        enrichedValues.put(ENERGY_USED, energyKwh);
        enrichedValues.put(EMBODIED_EMISSIONS, embodiedEmissions);

        LOG.debug("Bedrock modelKey={} outputTokens={} energy_kwh={} embodied_g={}",
                modelKey, totalTokens, energyKwh, embodiedEmissions);
    }

    /**
     * Parses {@code line_item_usage_type} with format
     * {@code {REGION_PREFIX}-{ModelKey}-{input|output}-tokens[-batch]}.
     *
     * @return [modelKey, "input"|"output"], or {@code null} if the format is unrecognized
     */
    static String[] parseUsageType(String usageType) {
        if (usageType == null) {
            return null;
        }
        int firstDash = usageType.indexOf('-');
        if (firstDash < 0) {
            return null;
        }
        String withoutRegion = usageType.substring(firstDash + 1);
        String lower = withoutRegion.toLowerCase();

        if (lower.endsWith("-output-tokens-batch")) {
            return new String[]{withoutRegion.substring(0, withoutRegion.length() - "-output-tokens-batch".length()), "output"};
        } else if (lower.endsWith("-input-tokens-batch")) {
            return new String[]{withoutRegion.substring(0, withoutRegion.length() - "-input-tokens-batch".length()), "input"};
        } else if (lower.endsWith("-output-tokens")) {
            return new String[]{withoutRegion.substring(0, withoutRegion.length() - "-output-tokens".length()), "output"};
        } else if (lower.endsWith("-input-tokens")) {
            return new String[]{withoutRegion.substring(0, withoutRegion.length() - "-input-tokens".length()), "input"};
        }
        return null;
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
