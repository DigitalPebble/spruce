// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimedModuleTest {

    /** Module whose enrich() does enough work to take a measurable amount of time. */
    static class Spinning implements EnrichmentModule {
        int calls = 0;

        @Override
        public Column[] columnsNeeded() {
            return new Column[]{CURColumn.USAGE_AMOUNT};
        }

        @Override
        public Column[] columnsAdded() {
            return new Column[]{SpruceColumn.ENERGY_USED};
        }

        @Override
        public void enrich(Row row, Map<Column, Object> enrichedValues) {
            calls++;
            long until = System.nanoTime() + 200_000;
            while (System.nanoTime() < until) {
                // spin
            }
        }
    }

    /** Module that gives up part way through, as most of the real ones do. */
    static class Failing implements EnrichmentModule {
        @Override
        public Column[] columnsNeeded() {
            return new Column[0];
        }

        @Override
        public Column[] columnsAdded() {
            return new Column[0];
        }

        @Override
        public void enrich(Row row, Map<Column, Object> enrichedValues) {
            throw new IllegalStateException("boom");
        }
    }

    @Test
    public void testRecordsTimeAtItsOwnIndex() {
        Spinning module = new Spinning();
        long[] nanos = new long[3];
        TimedModule timed = new TimedModule(module, nanos, 1);

        timed.enrich(null, null);
        timed.enrich(null, null);

        assertEquals(2, module.calls, "the delegate should be called every time");
        assertTrue(nanos[1] > 0, "elapsed time should be recorded");
        assertEquals(0, nanos[0], "other modules' timings should be untouched");
        assertEquals(0, nanos[2], "other modules' timings should be untouched");
    }

    @Test
    public void testAccumulatesAcrossCalls() {
        Spinning module = new Spinning();
        long[] nanos = new long[1];
        TimedModule timed = new TimedModule(module, nanos, 0);

        timed.enrich(null, null);
        long afterFirst = nanos[0];
        timed.enrich(null, null);

        assertTrue(nanos[0] > afterFirst, "timings should add up rather than be replaced");
    }

    @Test
    public void testRecordsTimeWhenTheModuleThrows() {
        long[] nanos = new long[1];
        TimedModule timed = new TimedModule(new Failing(), nanos, 0);

        assertThrows(IllegalStateException.class, () -> timed.enrich(null, null));
        assertTrue(nanos[0] >= 0, "the finally block should still have recorded the call");
    }

    @Test
    public void testDelegatesEverythingElse() {
        Spinning module = new Spinning();
        TimedModule timed = new TimedModule(module, new long[1], 0);

        assertArrayEqualsByIdentity(module.columnsNeeded(), timed.columnsNeeded());
        assertArrayEqualsByIdentity(module.columnsAdded(), timed.columnsAdded());
        assertSame(module, timed.getDelegate());
    }

    @Test
    public void testLabelsDisambiguateRepeatedClassNames() {
        List<EnrichmentModule> modules = List.of(new Spinning(), new Failing(), new Spinning());
        assertEquals(List.of("Spinning", "Failing", "Spinning#2"), ModuleTimings.labels(modules));
    }

    private static void assertArrayEqualsByIdentity(Column[] expected, Column[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertSame(expected[i], actual[i]);
        }
    }
}
