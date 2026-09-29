//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

/**
 * Shared pieces of {@link AggregationIncrementalBenchmark}, for a build that predates reclaiming aggregation states.
 */
final class AggregationStateBenchSupport {
    private AggregationStateBenchSupport() {}

    /**
     * Select how refreshing aggregations reclaim the states of removed keys. This build predates reclaiming, so only
     * {@code none} is supported.
     *
     * @param reclaim the reclaim mode
     */
    static void setReclaimMode(final String reclaim) {
        if (!reclaim.equals("none")) {
            throw new IllegalArgumentException("This build does not reclaim states: reclaim=" + reclaim);
        }
    }

    /**
     * Set the fraction free at which blocks of output positions are collapsed. This build never collapses, so only 1 is
     * supported.
     *
     * @param collapseFreeFraction the fraction free at which a block is collapsed
     */
    static void setCollapseFreeFraction(final double collapseFreeFraction) {
        if (collapseFreeFraction < 1) {
            throw new IllegalArgumentException("This build does not collapse blocks: collapse=" + collapseFreeFraction);
        }
    }

    /**
     * @return -1, since this build does not count rehashes
     */
    static long rehashCount() {
        return -1;
    }

    /**
     * A long column whose value is a pure function of the row key, so that adding or removing rows costs nothing in the
     * column itself. The value at {@code rowKey} is {@code rowKey / rowsPerValue}, reduced modulo {@code valueSpace}
     * when {@code valueSpace} is positive.
     */
    static final class ComputedLongSource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long rowsPerValue;
        private final long valueSpace;

        ComputedLongSource(final long rowsPerValue, final long valueSpace) {
            super(long.class);
            this.rowsPerValue = rowsPerValue;
            this.valueSpace = valueSpace;
        }

        @Override
        public long getLong(final long rowKey) {
            final long value = rowKey / rowsPerValue;
            return valueSpace > 0 ? value % valueSpace : value;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return getLong(rowKey);
        }

        @Override
        public boolean isImmutable() {
            return false;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }
}
