// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.ecologits;

import com.digitalpebble.spruce.AzureColumn;
import com.digitalpebble.spruce.AzureFOCUSColumn;
import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.ReportFormat;
import com.digitalpebble.spruce.RowColumn;
import org.apache.spark.sql.Row;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.digitalpebble.spruce.SpruceColumn.EMBODIED_EMISSIONS;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;

/**
 * Enrichment module estimating energy consumption and embodied emissions
 * for LLM inference billed through Azure AI Foundry token meters.
 * <p>
 * Token usage is billed through meters under the {@code Azure OpenAI} (or, for
 * newer meters, {@code Foundry Models}) category. The module extracts the model
 * label and the token direction from {@code MeterName} (e.g.
 * {@code "GPT 5 outpt Glbl 1M Tokens"}), maps the label to the matching
 * {@link EcoLogits} coefficients via {@code ecologits/mapping.csv}, and applies
 * them to the consumed token count: {@code Quantity} in native cost details
 * exports, {@code ConsumedQuantity} in FOCUS exports. Both hold the number of
 * tokens consumed — the 1K/1M unit in {@code UnitOfMeasure} only describes the
 * pricing block ({@code ContractedCost = UnitPrice × Quantity / x_PricingBlockSize}
 * in Microsoft's FOCUS conversion rules) and must not scale the quantity.
 * <p>
 * Coefficients only describe output tokens; input-token rows are ignored
 * (the EcoLogits methodology attributes ~all generation cost to output tokens).
 * Provisioned throughput (PTU), hourly hosting and fine-tuning meters are not
 * token-based and are therefore not covered. Other Foundry model families
 * (Mistral, Cohere, Llama, ...) bill through the same kind of token meters and
 * only need entries in {@code mapping.csv} verified against real exports —
 * initially the module ships with Azure OpenAI mappings. Reasoning and
 * embedding token meters carry no input/output marker and are skipped too,
 * which slightly underestimates the impacts of reasoning models.
 */
public class AzureFoundryTokenEcoLogits implements EnrichmentModule {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(AzureFoundryTokenEcoLogits.class);

    private static final Set<String> METER_CATEGORIES = Set.of("Azure OpenAI", "Foundry Models");

    // Direction markers observed in Azure OpenAI meter names ("5.1 codex opt Gl 1M Tokens"
    // pairs with "5.1 codex inp Gl 1M Tokens", so "opt" is an output marker).
    private static final Set<String> INPUT_MARKERS = Set.of("inp", "inpt", "input");
    private static final Set<String> OUTPUT_MARKERS = Set.of("outp", "outpt", "out", "opt", "output");

    // Deployment/pricing qualifiers that may precede the direction marker and are not
    // part of the model label (batch pricing, cached tokens, short/long context, ...).
    private static final Set<String> QUALIFIERS = Set.of("batch", "cchd", "cd", "wr", "pp", "shortco", "longco");

    private EcoLogits impacts;

    protected RowColumn meterCategory = AzureColumn.METER_CATEGORY;
    protected RowColumn meterName = AzureColumn.METER_NAME;
    protected RowColumn quantity = AzureColumn.QUANTITY;

    @Override
    public void bindReportFormat(ReportFormat reportFormat) {
        if (reportFormat == ReportFormat.FOCUS) {
            meterCategory = AzureFOCUSColumn.X_SKU_METER_CATEGORY;
            meterName = AzureFOCUSColumn.X_SKU_METER_NAME;
            quantity = FOCUSColumn.CONSUMED_QUANTITY;
        } else {
            meterCategory = AzureColumn.METER_CATEGORY;
            meterName = AzureColumn.METER_NAME;
            quantity = AzureColumn.QUANTITY;
        }
    }

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
        return new Column[]{meterCategory, meterName, quantity};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED, EMBODIED_EMISSIONS};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String category = this.meterCategory.getString(row);
        if (category == null || !METER_CATEGORIES.contains(category)) {
            return;
        }

        String[] parsed = parseMeterName(this.meterName.getString(row));
        if (parsed == null || "input".equals(parsed[1])) {
            return;
        }

        EcoLogits.ModelImpacts modelImpacts = impacts.getImpacts(parsed[0]);
        if (modelImpacts == null) {
            return;
        }

        if (this.quantity.isNullAt(row)) {
            return;
        }
        double totalTokens = this.quantity.getDouble(row);
        if (totalTokens <= 0) {
            return;
        }

        double per1k = totalTokens / 1_000.0;
        double energyKwh = per1k * modelImpacts.getEnergyKwhPer1kOutputTokens();
        double embodiedEmissions = per1k * modelImpacts.getGwpEmbodiedGPer1kOutputTokens();

        enrichedValues.put(ENERGY_USED, energyKwh);
        enrichedValues.put(EMBODIED_EMISSIONS, embodiedEmissions);

        LOG.debug("Azure OpenAI model={} outputTokens={} energy_kwh={} embodied_g={}",
                parsed[0], totalTokens, energyKwh, embodiedEmissions);
    }

    /**
     * Parses an Azure OpenAI token meter name such as {@code "GPT 5 outpt Glbl 1M Tokens"}
     * or {@code "5 mini pp Inp Gl 1M Tokens"}: the words before the first input/output
     * marker form the model label (minus pricing qualifiers like {@code Batch} or
     * {@code cchd}), lowercased so it can be looked up in {@code mapping.csv}.
     *
     * @return [modelLabel, "input"|"output"], or {@code null} if the meter is not a
     * recognisable token meter
     */
    static String[] parseMeterName(String meterName) {
        if (meterName == null || meterName.isBlank()) {
            return null;
        }
        String[] tokens = meterName.trim().split("\\s+");
        if (!"tokens".equalsIgnoreCase(tokens[tokens.length - 1])) {
            return null;
        }

        StringBuilder label = new StringBuilder();
        for (String token : tokens) {
            String lower = token.toLowerCase(Locale.ROOT);
            if (INPUT_MARKERS.contains(lower) || OUTPUT_MARKERS.contains(lower)) {
                if (label.isEmpty()) {
                    return null;
                }
                String direction = INPUT_MARKERS.contains(lower) ? "input" : "output";
                return new String[]{label.toString(), direction};
            }
            if (!QUALIFIERS.contains(lower)) {
                if (!label.isEmpty()) {
                    label.append(' ');
                }
                label.append(lower);
            }
        }
        // no direction marker found
        return null;
    }
}
