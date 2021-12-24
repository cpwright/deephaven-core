/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FlatCharArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.sources.flat;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_LONG;
// endregion boxing imports

/**
 * Simple flat array source that supports filling for initial creation.
 */
public class FlatLongArraySource extends AbstractColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong, WritableColumnSource<Long>, FillUnordered, InMemoryColumnSource {
    private final long[] data;

    // region constructor
    public FlatLongArraySource(long size) {
        super(long.class);
        this.data = new long[Math.toIntExact(size)];
    }
    // endregion constructor

    @Override
    public final long getLong(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_LONG;
        }

        return getUnsafe(index);
    }

    public final long getUnsafe(long index) {
        return data[(int)index];
    }

    @Override
    public final void set(long key, long value) {
        data[Math.toIntExact(key)] = value;
    }

    @Override
    public void copy(ColumnSource<? extends Long> sourceColumn, long sourceKey, long destKey) {
        set(destKey, sourceColumn.getLong(sourceKey));
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        if (capacity > data.length) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillChunkByRanges(destination, rowSequence);
        } else {
            fillChunkByKeys(destination, rowSequence);
        }
    }

    private void fillChunkByRanges(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableLongChunk<? super Values> asLongChunk = destination.asWritableLongChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = Math.toIntExact(end - start + 1);
            asLongChunk.copyFromTypedArray(data, Math.toIntExact(start), srcPos.getAndAdd(rangeLength), rangeLength);
        });
    }

    private void fillChunkByKeys(WritableChunk<? super Values> destination, RowSequence rowSequence) {
        final WritableLongChunk<? super Values> asLongChunk = destination.asWritableLongChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> asLongChunk.set(srcPos.getAndIncrement(), getUnsafe(key)));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return super.getChunk(contextWithResettable.inner, rowSequence);
    }

    private class GetContextWithResettable implements GetContext {
        final ResettableLongChunk<? extends Values> resettableLongChunk = ResettableLongChunk.makeResettableChunk();
        final GetContext inner;

        private GetContextWithResettable(GetContext inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            resettableLongChunk.close();
            inner.close();
        }
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new GetContextWithResettable(super.makeGetContext(chunkCapacity, sharedContext));
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int len = Math.toIntExact(lastKey - firstKey + 1);
        final GetContextWithResettable contextWithResettable = (GetContextWithResettable) context;
        return contextWithResettable.resettableLongChunk.resetFromTypedArray(data, Math.toIntExact(firstKey), len);
    }

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillFromChunkByRanges(src, rowSequence);
        } else {
            fillFromChunkByKeys(src, rowSequence);
        }
    }

    private void fillFromChunkByKeys(Chunk<? extends Values> src, RowSequence rowSequence) {
        final LongChunk<? extends Values> asLongChunk = src.asLongChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> set(key, asLongChunk.get(srcPos.getAndIncrement())));
    }

    private void fillFromChunkByRanges(Chunk<? extends Values> src, RowSequence rowSequence) {
        final LongChunk<? extends Values> asLongChunk = src.asLongChunk();
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            final int rangeLength = Math.toIntExact(end - start + 1);
            asLongChunk.copyToTypedArray(srcPos.getAndAdd(rangeLength), data, Math.toIntExact(start), rangeLength);
        });
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<RowKeys> keys) {
        final LongChunk<? extends Values> asLongChunk = src.asLongChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            set(keys.get(ii), asLongChunk.get(ii));
        }
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            final int key = Math.toIntExact(keys.get(ii));
            longDest.set(ii, getUnsafe(key));
        }
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, dest, keys);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return getChunk(context, firstKey, lastKey);
    }
}
