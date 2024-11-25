//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.util.NullNanHelper;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.util.mutable.MutableInt;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative add only min max operator.
 */
class CharChunkedAddOnlyMinMaxOperator implements IterativeChunkedAggregationOperator {
    private final CharacterArraySource resultColumn;
    // region actualResult
    // endregion actualResult
    private final boolean minimum;
    private final String name;

    CharChunkedAddOnlyMinMaxOperator(
            // region extra constructor params
            // endregion extra constructor params
            boolean minimum, String name) {
        this.minimum = minimum;
        this.name = name;
        // region resultColumn initialization
        this.resultColumn = new CharacterArraySource();
        // endregion resultColumn initialization
    }

    private static char min(CharChunk<?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull = 0;
        char value = QueryConstants.NULL_CHAR;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final char candidate = values.get(ii);
            if (NullNanHelper.isNull(candidate)) {
                continue;
            }
            if (nonNull++ == 0) {
                value = candidate;
            } else if (NullNanHelper.isNaN(candidate)) {
                value = candidate;
                // the nonNull count is not accurate in this short circuit case, but it will be non-zero which is
                // enough for the caller within this class to function properly.
                break;
            } else if (CharComparisons.lt(candidate, value)) {
                value = candidate;
            }
        }
        chunkNonNull.set(nonNull);
        return value;
    }

    private static char max(CharChunk<?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull = 0;
        char value = QueryConstants.NULL_CHAR;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final char candidate = values.get(ii);
            if (NullNanHelper.isNull(candidate)) {
                continue;
            }
            if (nonNull++ == 0) {
                value = candidate;
            } else if (NullNanHelper.isNaN(candidate)) {
                value = candidate;
                // the nonNull count is not accurate in this short circuit case, but it will be non-zero which is
                // enough for the caller within this class to function properly.
                break;
            } else if (CharComparisons.gt(candidate, value)) {
                value = candidate;
            }
        }
        chunkNonNull.set(nonNull);
        return value;
    }

    private static char min(char a, char b) {
        if (NullNanHelper.isNaN(a)) {
            return a;
        }
        if (NullNanHelper.isNaN(b)) {
            return b;
        }
        return CharComparisons.leq(a, b) ? a : b;
    }

    private static char max(char a, char b) {
        if (NullNanHelper.isNaN(a)) {
            return a;
        }
        if (NullNanHelper.isNaN(b)) {
            return b;
        }
        return CharComparisons.geq(a, b) ? a : b;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final CharChunk<? extends Values> asCharChunk = values.asCharChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asCharChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asCharChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        throw new UnsupportedOperationException();
    }

    private boolean addChunk(CharChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        if (chunkSize == 0) {
            return false;
        }
        final char oldValue = resultColumn.getUnsafe(destination);
        if (NullNanHelper.isNaN(oldValue)) {
            return false;
        }

        final MutableInt chunkNonNull = new MutableInt(0);
        final int chunkEnd = chunkStart + chunkSize;
        final char chunkValue = minimum ? min(values, chunkNonNull, chunkStart, chunkEnd)
                : max(values, chunkNonNull, chunkStart, chunkEnd);
        if (chunkNonNull.get() == 0) {
            return false;
        }

        final char result;
        if (NullNanHelper.isNull(oldValue)) {
            // we exclude nulls (and NaNs) from the min/max calculation, therefore if the value in our min/max is null
            // or NaN we know that it is in fact empty and we should use the value from the chunk
            result = chunkValue;
        } else {
            result = minimum ? min(oldValue, chunkValue) : max(oldValue, chunkValue);
        }
        if (!CharComparisons.eq(result, oldValue)) {
            resultColumn.set(destination, result);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        // region getResultColumns
        return Collections.<String, ColumnSource<?>>singletonMap(name, resultColumn);
        // endregion getResultColumns
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }
}
