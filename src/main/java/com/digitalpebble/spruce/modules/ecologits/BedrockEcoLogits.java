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
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        // TODO
    }

}