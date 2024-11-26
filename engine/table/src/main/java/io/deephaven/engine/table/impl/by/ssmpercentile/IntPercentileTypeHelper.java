//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPercentileTypeHelper and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.util.NullNanHelper;
import io.deephaven.util.compare.IntComparisons;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.engine.table.impl.ssms.IntSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.mutable.MutableInt;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class IntPercentileTypeHelper implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final IntegerArraySource resultColumn;

    IntPercentileTypeHelper(double percentile, WritableColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (IntegerArraySource) resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, NULL_INT);
        } else {
            final long targetLo = Math.round((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            if (NullNanHelper.intHasNans()) {
                // if we have only a low value, then there is by definition only one thing that is a NaN if we need
                // to poison it so we can just check the high values for poison
                final IntSegmentedSortedMultiset typedHi = (IntSegmentedSortedMultiset) ssmHi;
                if (ssmHi.size() > 0 && NullNanHelper.isNaN(typedHi.getMaxInt())) {
                    return setResult(destination, typedHi.getMaxInt());
                }
            }
            return setResult(destination, ((IntSegmentedSortedMultiset) ssmLo).getMaxInt());
        }
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_INT);
    }

    private boolean setResult(long destination, int newResult) {
        final int oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy,
            IntChunk<ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers) {
        final IntChunk<? extends Values> asIntChunk = valueCopy.asIntChunk();
        final IntSegmentedSortedMultiset ssmLo = (IntSegmentedSortedMultiset) segmentedSortedMultiSet;
        final int hiValue = ssmLo.getMaxInt();

        final int result = upperBound(asIntChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && IntComparisons.eq(asIntChunk.get(result - 1), hiValue)
                && counts.get(result - 1) > hiCount) {
            leftOvers.set((int) (counts.get(result - 1) - hiCount));
        } else {
            leftOvers.set(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy,
            IntChunk<ChunkLengths> counts, int startPosition, int runLength) {
        final IntChunk<? extends Values> asIntChunk = valueCopy.asIntChunk();
        final IntSegmentedSortedMultiset ssmLo = (IntSegmentedSortedMultiset) segmentedSortedMultiSet;
        final int hiValue = ssmLo.getMaxInt();

        final int result = upperBound(asIntChunk, startPosition, startPosition + runLength, hiValue);

        return result - startPosition;
    }

    /**
     * Return the highest index in valuesToSearch leq searchValue.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first index to search for
     * @param hi one past the last index to search in
     * @param searchValue the value to find
     * @return the highest index that is less than or equal to valuesToSearch
     */
    private static int upperBound(IntChunk<? extends Values> valuesToSearch, int lo, int hi, int searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final int testValue = valuesToSearch.get(mid);
            final boolean moveHi = IntComparisons.gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }
}
