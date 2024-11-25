//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkedAddOnlyMinMaxOperator and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.util.NullNanHelper;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.util.mutable.MutableInt;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative add only min max operator.
 */
class DoubleChunkedAddOnlyMinMaxOperator implements IterativeChunkedAggregationOperator {
    private final DoubleArraySource resultColumn;
    // region actualResult
    // endregion actualResult
    private final boolean minimum;
    private final String name;

    DoubleChunkedAddOnlyMinMaxOperator(
            // region extra constructor params
            // endregion extra constructor params
            boolean minimum, String name) {
        this.minimum = minimum;
        this.name = name;
        // region resultColumn initialization
        this.resultColumn = new DoubleArraySource();
        // endregion resultColumn initialization
    }

    private static double min(DoubleChunk<?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull = 0;
        double value = QueryConstants.NULL_DOUBLE;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final double candidate = values.get(ii);
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
            } else if (DoubleComparisons.lt(candidate, value)) {
                value = candidate;
            }
        }
        chunkNonNull.set(nonNull);
        return value;
    }

    private static double max(DoubleChunk<?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull = 0;
        double value = QueryConstants.NULL_DOUBLE;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final double candidate = values.get(ii);
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
            } else if (DoubleComparisons.gt(candidate, value)) {
                value = candidate;
            }
        }
        chunkNonNull.set(nonNull);
        return value;
    }

    private static double min(double a, double b) {
        if (NullNanHelper.isNaN(a)) {
            return a;
        }
        if (NullNanHelper.isNaN(b)) {
            return b;
        }
        return DoubleComparisons.leq(a, b) ? a : b;
    }

    private static double max(double a, double b) {
        if (NullNanHelper.isNaN(a)) {
            return a;
        }
        if (NullNanHelper.isNaN(b)) {
            return b;
        }
        return DoubleComparisons.geq(a, b) ? a : b;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final DoubleChunk<? extends Values> asDoubleChunk = values.asDoubleChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asDoubleChunk, destination, startPosition, length.get(ii)));
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
        return addChunk(values.asDoubleChunk(), destination, 0, values.size());
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

    private boolean addChunk(DoubleChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        if (chunkSize == 0) {
            return false;
        }
        final double oldValue = resultColumn.getUnsafe(destination);
        if (NullNanHelper.isNaN(oldValue)) {
            return false;
        }

        final MutableInt chunkNonNull = new MutableInt(0);
        final int chunkEnd = chunkStart + chunkSize;
        final double chunkValue = minimum ? min(values, chunkNonNull, chunkStart, chunkEnd)
                : max(values, chunkNonNull, chunkStart, chunkEnd);
        if (chunkNonNull.get() == 0) {
            return false;
        }

        final double result;
        if (NullNanHelper.isNull(oldValue)) {
            // we exclude nulls (and NaNs) from the min/max calculation, therefore if the value in our min/max is null
            // or NaN we know that it is in fact empty and we should use the value from the chunk
            result = chunkValue;
        } else {
            result = minimum ? min(oldValue, chunkValue) : max(oldValue, chunkValue);
        }
        if (!DoubleComparisons.eq(result, oldValue)) {
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
