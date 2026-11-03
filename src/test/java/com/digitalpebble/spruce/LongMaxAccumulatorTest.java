// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LongMaxAccumulatorTest {

    @Test
    public void testKeepsTheLargestValue() {
        LongMaxAccumulator accumulator = new LongMaxAccumulator();
        accumulator.add(5L);
        accumulator.add(90L);
        accumulator.add(12L);

        assertEquals(90L, accumulator.value());
    }

    @Test
    public void testStartsAndResetsToZero() {
        LongMaxAccumulator accumulator = new LongMaxAccumulator();
        assertTrue(accumulator.isZero());
        assertEquals(0L, accumulator.value());

        accumulator.add(7L);
        assertFalse(accumulator.isZero());

        accumulator.reset();
        assertTrue(accumulator.isZero());
        assertEquals(0L, accumulator.value());
    }

    @Test
    public void testCopyIsIndependent() {
        LongMaxAccumulator accumulator = new LongMaxAccumulator();
        accumulator.add(42L);

        AccumulatorV2<Long, Long> copy = accumulator.copy();
        assertEquals(42L, copy.value());

        copy.add(100L);
        assertEquals(100L, copy.value());
        assertEquals(42L, accumulator.value(), "the original should not move");
    }

    @Test
    public void testCopyAndResetGivesAZeroAccumulator() {
        LongMaxAccumulator accumulator = new LongMaxAccumulator();
        accumulator.add(42L);

        assertTrue(accumulator.copyAndReset().isZero(), "Spark requires this to be zero");
    }

    @Test
    public void testMergeTakesTheLargerOfTheTwo() {
        LongMaxAccumulator one = new LongMaxAccumulator();
        one.add(30L);
        LongMaxAccumulator two = new LongMaxAccumulator();
        two.add(80L);

        one.merge(two);
        assertEquals(80L, one.value());

        two.merge(one);
        assertEquals(80L, two.value(), "merging a smaller value should not lower it");
    }

    @Test
    public void testMergeRejectsOtherAccumulatorTypes() {
        LongMaxAccumulator accumulator = new LongMaxAccumulator();
        assertThrows(UnsupportedOperationException.class,
                () -> accumulator.merge(new LongAccumulator()));
    }

    @Test
    public void testNullIsIgnored() {
        LongMaxAccumulator accumulator = new LongMaxAccumulator();
        accumulator.add(9L);
        accumulator.add(null);

        assertEquals(9L, accumulator.value());
    }
}
