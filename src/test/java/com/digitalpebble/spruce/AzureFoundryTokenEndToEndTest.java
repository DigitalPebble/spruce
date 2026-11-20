// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import com.digitalpebble.spruce.modules.ecologits.EcoLogits;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs minimal Azure OpenAI billing exports — one native, one FOCUS — through the real
 * default configurations and the {@link EnrichmentPipeline}, as {@link SparkJob} does.
 **/
public class AzureFoundryTokenEndToEndTest {

    private static SparkSession spark;

    private static final String ENERGY = SpruceColumn.ENERGY_USED.getLabel();

    private static EcoLogits impacts;

    @BeforeAll
    static void loadEcoLogits() {
        impacts = new EcoLogits();
        impacts.load();
    }

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("AzureFoundryTokenEndToEndTest")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    /** Replicates the SparkJob steps: read, normalise, add module columns, run the pipeline. */
    private List<Row> enrich(String resource, ReportFormat reportFormat) throws Exception {
        String path = getClass().getResource(resource).getPath();
        Dataset<Row> dataframe = spark.read().option("header", "true").option("inferSchema", "true")
                .option("quote", "\"")
                .option("escape", "\"").csv(path);
        dataframe = SparkJob.normalizeAzureColumns(dataframe, reportFormat);

        Config config = Config.loadDefault(Provider.AZURE, reportFormat);
        for (EnrichmentModule module : config.getModules()) {
            for (Column c : module.columnsNeeded()) {
                assertFalse(dataframe.schema().getFieldIndex(c.getLabel()).isEmpty(),
                        "Fixture " + resource + " misses column '" + c.getLabel()
                                + "' needed by " + module.getClass().getSimpleName());
            }
            for (Column c : module.columnsAdded()) {
                dataframe = dataframe.withColumn(c.getLabel(), lit(null).cast(c.getType()));
            }
        }

        Encoder<Row> encoder = RowEncoder.encoderFor(dataframe.schema());
        return dataframe.mapPartitions(new EnrichmentPipeline(config), encoder).collectAsList();
    }

    private static Double energy(Row row) {
        int index = row.fieldIndex(ENERGY);
        return row.isNullAt(index) ? null : row.getDouble(index);
    }

    @Test
    void ecologitsRunsBeforeFactorAndImpactModules() throws Exception {
        for (ReportFormat format : new ReportFormat[]{ReportFormat.NATIVE, ReportFormat.FOCUS}) {
            List<String> names = Config.loadDefault(Provider.AZURE, format).getModules().stream()
                    .map(m -> m.getClass().getSimpleName()).toList();
            int ecologits = names.indexOf("AzureFoundryTokenEcoLogits");
            assertTrue(ecologits >= 0, "AzureFoundryTokenEcoLogits missing from " + format + " config");
            for (String downstream : new String[]{"PWUE", "Water", "OperationalEmissions"}) {
                assertTrue(ecologits < names.indexOf(downstream),
                        "AzureFoundryTokenEcoLogits must run before " + downstream + " (" + format + ")");
            }
        }
    }

    @Test
    void enrichesNativeExport() throws Exception {
        List<Row> rows = enrich("/azure/native-openai.csv", ReportFormat.NATIVE);
        assertEquals(6, rows.size());

        // 1,000,000 output tokens of gpt-5 (Quantity 1 with UnitOfMeasure "1M"): energy
        // matches the bundled coefficients
        double expected = 1_000.0 * impacts.getImpacts("gpt 5").getEnergyKwhPer1kOutputTokens();
        assertEquals(expected, energy(rows.get(0)), 1e-12);

        assertNull(energy(rows.get(1)), "input tokens must not be estimated");
        assertNull(energy(rows.get(2)), "unmapped model must not be estimated");
        assertNull(energy(rows.get(3)), "non-token meter must not be estimated");
        // 2,000 output tokens of gpt-4.1 (Quantity 2 with UnitOfMeasure "1K")
        double expected41 = 2.0 * impacts.getImpacts("gpt 4.1").getEnergyKwhPer1kOutputTokens();
        assertEquals(expected41, energy(rows.get(4)), 1e-12);
        assertNull(energy(rows.get(5)), "non-usage charge must not be enriched");
    }

    @Test
    void enrichesFocusExport() throws Exception {
        List<Row> focusRows = enrich("/azure/focus-openai.csv", ReportFormat.FOCUS);
        assertEquals(2, focusRows.size());

        // ConsumedQuantity 1,000,000 is read as a token count
        double expected = 1_000.0 * impacts.getImpacts("gpt 5").getEnergyKwhPer1kOutputTokens();
        assertEquals(expected, energy(focusRows.get(0)), 1e-12);
        assertNull(energy(focusRows.get(1)), "input tokens must not be estimated");
    }

    @Test
    void readsNumericUnitOfMeasureAsString() throws Exception {
        // with UnitOfMeasure "1" on every row, inferSchema reads the column as integers
        List<Row> rows = enrich("/azure/native-numeric-unit.csv", ReportFormat.NATIVE);
        assertEquals(1, rows.size());

        assertNull(energy(rows.get(0)));
        Row row = rows.get(0);
        String gap = row.getString(row.fieldIndex(SpruceColumn.COVERAGE_GAP.getLabel()));
        assertTrue(gap.contains("token_unit=1"), gap);
    }
}
