//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ShortReverseSortCheck and run "./gradlew replicateSortCheck" to regenerate
//
// @formatter:off

package io.deephaven.engine.table.impl.sortcheck;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;

public class ShortReverseSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new ShortReverseSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asShortChunk());
    }

    private int sortCheck(ShortChunk<? extends Values> valuesToCheck) {
        if (valuesToCheck.size() == 0) {
            return -1;
        }
        short last = valuesToCheck.get(0);
        for (int ii = 1; ii < valuesToCheck.size(); ++ii) {
            final short current = valuesToCheck.get(ii);
            if (!leq(last, current)) {
                return ii - 1;
            }
            last = current;
        }
        return -1;
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static boolean leq(short lhs, short rhs) {
        return ShortComparisons.geq(lhs, rhs);
    }
    // endregion comparison functions
}
