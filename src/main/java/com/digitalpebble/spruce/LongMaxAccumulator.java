// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import org.apache.spark.util.AccumulatorV2;

/**
 * Accumulator keeping the largest value it was given, rather than the sum. Spark only ships a
 * summing {@link org.apache.spark.util.LongAccumulator}, and a sum cannot tell a cost every task
 * pays from a cost one task pays once for the whole executor: 90 ms spread over four tasks and
 * 90 ms charged to a single one look identical. The largest value of a set of per-task totals
 * separates the two.
 *
 * <p>Values are expected to be non-negative, which they are for elapsed nanoseconds. That lets
 * zero stand in as the identity, so a fresh or reset instance reports itself as zero and Spark
 * leaves it out of the updates it sends back.
 */
public class LongMaxAccumulator extends AccumulatorV2<Long, Long> {

    private long max = 0L;

    @Override
    public boolean isZero() {
        return max == 0L;
    }

    @Override
    public AccumulatorV2<Long, Long> copy() {
        LongMaxAccumulator copy = new LongMaxAccumulator();
        copy.max = max;
        return copy;
    }

    @Override
    public void reset() {
        max = 0L;
    }

    @Override
    public void add(Long value) {
        if (value != null && value > max) {
            max = value;
        }
    }

    @Override
    public void merge(AccumulatorV2<Long, Long> other) {
        if (!(other instanceof LongMaxAccumulator)) {
            throw new UnsupportedOperationException(
                    "Cannot merge " + getClass().getName() + " with " + other.getClass().getName());
        }
        add(((LongMaxAccumulator) other).max);
    }

    @Override
    public Long value() {
        return max;
    }
}
