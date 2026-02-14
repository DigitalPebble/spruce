// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Benchmarks the current row-copy-per-module enrichment approach against a
 * map-based approach where modules accumulate updates in a {@code Map<Column, Object>}
 * and a single Row is materialised at the end of the pipeline.
 *
 * <h3>Current approach</h3>
 * Every call to {@link EnrichmentModule#withUpdatedValue} allocates a new
 * {@code Object[]}, copies <em>all</em> column values from the previous row,
 * sets the target value, and wraps it in a new {@link GenericRowWithSchema}.
 * With 6 modules and a 108-column schema this means 6 full array copies and
 * 6 Row allocations <em>per input row</em>.
 *
 * <h3>Map approach</h3>
 * Modules read CUR (input) columns from the immutable original Row and
 * read/write Spruce (enrichment) columns via a shared {@code Map<Column, Object>}.
 * One {@link GenericRowWithSchema} is created at the very end — reducing
 * N array copies to 1.
 *
 * <p>The simulated pipeline mirrors the real module chain:
 * <ol>
 *   <li>RegionExtraction → writes REGION</li>
 *   <li>BoaviztAPIstatic → writes ENERGY_USED, EMBODIED_EMISSIONS, EMBODIED_ADP</li>
 *   <li>Accelerators    → adds to ENERGY_USED</li>
 *   <li>PUE             → reads ENERGY_USED, writes PUE</li>
 *   <li>CarbonIntensity → reads REGION, writes CARBON_INTENSITY</li>
 *   <li>OperationalEmissions → reads ENERGY_USED, PUE, CARBON_INTENSITY → writes OPERATIONAL_EMISSIONS</li>
 * </ol>
 */
public class EnrichmentStrategyBenchmark {

    /** CUR input columns — realistic width for an AWS CUR report. */
    private static final int CUR_WIDTH = 100;

    private static final Column[] SPRUCE_COLUMNS = {
            REGION, ENERGY_USED, EMBODIED_EMISSIONS, EMBODIED_ADP,
            PUE, CARBON_INTENSITY, OPERATIONAL_EMISSIONS, CPU_LOAD
    };

    private static StructType schema;
    private static Row templateRow;
    private static int totalWidth;

    @BeforeAll
    static void setup() {
        StructField[] fields = new StructField[CUR_WIDTH + SPRUCE_COLUMNS.length];
        for (int i = 0; i < CUR_WIDTH; i++) {
            fields[i] = DataTypes.createStructField("cur_col_" + i, DataTypes.StringType, true);
        }
        for (int i = 0; i < SPRUCE_COLUMNS.length; i++) {
            fields[CUR_WIDTH + i] = DataTypes.createStructField(
                    SPRUCE_COLUMNS[i].getLabel(), SPRUCE_COLUMNS[i].getType(), true);
        }
        schema = new StructType(fields);
        totalWidth = schema.length();

        Object[] values = new Object[totalWidth];
        for (int i = 0; i < CUR_WIDTH; i++) {
            values[i] = "cur_value_" + i;
        }
        // Spruce columns start as null — same as the real pipeline
        templateRow = new GenericRowWithSchema(values, schema);
    }

    // ==================================================================
    //  CURRENT APPROACH — one Row copy per module update
    // ==================================================================

    /** Mirrors the old per-module row-copy approach. */
    private static Row copyAndSet(Row row, Column column, Object newValue) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
        }
        values[column.resolveIndex(row)] = newValue;
        return new GenericRowWithSchema(values, row.schema());
    }

    /** Mirrors the old bulk row-copy approach. */
    private static Row copyAndSetBulk(Row row, Map<Column, Object> updates) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
        }
        for (Map.Entry<Column, Object> e : updates.entrySet()) {
            values[e.getKey().resolveIndex(row)] = e.getValue();
        }
        return new GenericRowWithSchema(values, row.schema());
    }

    /**
     * 6-module pipeline using the current row-copy strategy.
     * Each module produces a new Row; subsequent modules read from it.
     */
    static Row currentApproach(Row row) {
        // 1. RegionExtraction → REGION
        row = copyAndSet(row, REGION, "us-east-1");

        // 2. BoaviztAPIstatic → ENERGY_USED, EMBODIED_EMISSIONS, EMBODIED_ADP
        Map<Column, Object> boavizta = new HashMap<>(4);
        boavizta.put(ENERGY_USED, 0.042);
        boavizta.put(EMBODIED_EMISSIONS, 0.15);
        boavizta.put(EMBODIED_ADP, 0.003);
        row = copyAndSetBulk(row, boavizta);

        // 3. Accelerators → adds to ENERGY_USED (withUpdatedValue with add=true)
        double existing = row.getDouble(ENERGY_USED.resolveIndex(row));
        row = copyAndSet(row, ENERGY_USED, existing + 0.008);

        // 4. PUE → reads ENERGY_USED, writes PUE
        @SuppressWarnings("unused")
        double energy4 = row.getDouble(ENERGY_USED.resolveIndex(row));
        row = copyAndSet(row, PUE, 1.15);

        // 5. AverageCarbonIntensity → reads REGION, writes CARBON_INTENSITY
        @SuppressWarnings("unused")
        String region = row.getString(REGION.resolveIndex(row));
        row = copyAndSet(row, CARBON_INTENSITY, 450.0);

        // 6. OperationalEmissions → reads ENERGY_USED, PUE, CARBON_INTENSITY → OPERATIONAL_EMISSIONS
        double energy = row.getDouble(ENERGY_USED.resolveIndex(row));
        double pue = row.getDouble(PUE.resolveIndex(row));
        double ci = row.getDouble(CARBON_INTENSITY.resolveIndex(row));
        row = copyAndSet(row, OPERATIONAL_EMISSIONS, energy * pue * ci);

        return row;
    }

    // ==================================================================
    //  MAP APPROACH — modules enrich a shared Map, one Row at the end
    // ==================================================================

    /**
     * Same 6-module pipeline but enrichment values accumulate in a Map.
     * CUR columns are read from the immutable original row.
     * Spruce columns written by earlier modules are read from the map.
     * A single Row is materialised at the very end.
     */
    static Row mapApproach(Row originalRow) {
        Map<Column, Object> enriched = new HashMap<>();

        // 1. RegionExtraction
        enriched.put(REGION, "us-east-1");

        // 2. BoaviztAPIstatic
        enriched.put(ENERGY_USED, 0.042);
        enriched.put(EMBODIED_EMISSIONS, 0.15);
        enriched.put(EMBODIED_ADP, 0.003);

        // 3. Accelerators — adds to ENERGY_USED (reads previous value from map)
        Double existingEnergy = (Double) enriched.get(ENERGY_USED);
        enriched.put(ENERGY_USED, (existingEnergy != null ? existingEnergy : 0.0) + 0.008);

        // 4. PUE — reads ENERGY_USED from map
        @SuppressWarnings("unused")
        double energy4 = (Double) enriched.get(ENERGY_USED);
        enriched.put(PUE, 1.15);

        // 5. AverageCarbonIntensity — reads REGION from map
        @SuppressWarnings("unused")
        String region = (String) enriched.get(REGION);
        enriched.put(CARBON_INTENSITY, 450.0);

        // 6. OperationalEmissions — reads from map
        double energy = (Double) enriched.get(ENERGY_USED);
        double pue = (Double) enriched.get(PUE);
        double ci = (Double) enriched.get(CARBON_INTENSITY);
        enriched.put(OPERATIONAL_EMISSIONS, energy * pue * ci);

        // --- Single row materialisation ---
        Object[] values = new Object[originalRow.size()];
        for (int i = 0; i < originalRow.size(); i++) {
            values[i] = originalRow.get(i);
        }
        for (Map.Entry<Column, Object> e : enriched.entrySet()) {
            values[e.getKey().resolveIndex(originalRow)] = e.getValue();
        }
        return new GenericRowWithSchema(values, originalRow.schema());
    }

    // ==================================================================
    //  Benchmark
    // ==================================================================

    @Test
    void benchmarkMapVsRowCopy() {
        final int warmup = 100_000;
        final int iterations = 1_000_000;

        // ---- Warmup: let HotSpot JIT compile both paths ----
        for (int i = 0; i < warmup; i++) {
            currentApproach(templateRow);
            mapApproach(templateRow);
        }

        // ---- Measure current approach ----
        long t0 = System.nanoTime();
        Row resultCurrent = null;
        for (int i = 0; i < iterations; i++) {
            resultCurrent = currentApproach(templateRow);
        }
        long currentNs = System.nanoTime() - t0;

        // ---- Measure map approach ----
        t0 = System.nanoTime();
        Row resultMap = null;
        for (int i = 0; i < iterations; i++) {
            resultMap = mapApproach(templateRow);
        }
        long mapNs = System.nanoTime() - t0;

        // ---- Verify both approaches produce identical results ----
        for (int i = 0; i < totalWidth; i++) {
            Object a = resultCurrent.get(i);
            Object b = resultMap.get(i);
            if (a instanceof Double da && b instanceof Double db) {
                assertEquals(da, db, 1e-12, "Mismatch at column " + i);
            } else {
                assertEquals(a, b, "Mismatch at column " + i);
            }
        }

        // ---- Report ----
        long currentMs = currentNs / 1_000_000;
        long mapMs = mapNs / 1_000_000;
        double speedup = (double) currentNs / mapNs;

        System.out.println();
        System.out.println("=== Enrichment Strategy Benchmark ===");
        System.out.printf("Schema width:      %d columns (%d CUR + %d Spruce)%n",
                totalWidth, CUR_WIDTH, SPRUCE_COLUMNS.length);
        System.out.printf("Pipeline:          6 modules (mirrors real chain)%n");
        System.out.printf("Rows processed:    %,d%n%n", iterations);
        System.out.printf("Current (row-copy per module): %,d ms%n", currentMs);
        System.out.printf("Map (single row creation):     %,d ms%n", mapMs);
        System.out.printf("Speedup:                       %.2fx%n", speedup);
    }
}
