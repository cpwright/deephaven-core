//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchPairFactory;

/**
 * Gives benchmarks an as-of join with a chosen right chunk size, which sizes the chunks that an update of the right
 * table is processed in, and a chosen {@link JoinControl#restampBudgetFactor() restamp budget factor}.
 */
public class AsOfJoinBenchmarkAccess {
    private AsOfJoinBenchmarkAccess() {}

    /**
     * Join left to right on the stamp column T, and on the key column K when keyed, adding the column V.
     *
     * @param rightChunkSize the right chunk size of the join
     * @param restampBudgetFactor the restamp budget factor of the join
     * @param left the left table
     * @param right the right table
     * @param reverse true for raj (left T &lt;= right T), false for aj (left T &gt;= right T)
     * @param keyed true to match the K columns as well as the stamps
     * @return the joined table
     */
    public static Table asOfJoin(final int rightChunkSize, final int restampBudgetFactor, final QueryTable left,
            final QueryTable right, final boolean reverse, final boolean keyed) {
        final JoinControl control = new JoinControl() {
            @Override
            public int rightChunkSize() {
                return rightChunkSize;
            }

            @Override
            public int restampBudgetFactor() {
                return restampBudgetFactor;
            }
        };
        return AsOfJoinHelper.asOfJoin(control, left, reverse ? (QueryTable) right.reverse() : right,
                keyed ? MatchPairFactory.getExpressions("K", "T") : MatchPairFactory.getExpressions("T"),
                MatchPairFactory.getExpressions("V"), reverse ? SortingOrder.Descending : SortingOrder.Ascending,
                false);
    }
}
