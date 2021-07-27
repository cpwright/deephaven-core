/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunkcolumnsource;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

/**
 * A column source backed by {@link ShortChunk ShortChunks}.
 * <p>
 * The address space of the column source is dense, with each chunk backing a contiguous set of indices.  The
 * {@link #getChunk(GetContext, OrderedKeys)}
 * call will return the backing chunk or a slice of the backing chunk if possible.
 */
public class ShortChunkColumnSource extends AbstractColumnSource<Short> implements ImmutableColumnSourceGetDefaults.ForShort, ChunkColumnSource<Short> {
    private final ArrayList<WritableShortChunk<? extends Attributes.Values>> data = new ArrayList<>();
    private final TLongArrayList firstOffsetForData = new TLongArrayList();
    private long totalSize = 0;

    // region constructor
    protected ShortChunkColumnSource() {
        super(Short.class);
    }
    // endregion constructor

    @Override
    public short getShort(final long index) {
        if (index < 0 || index >= totalSize) {
            return QueryConstants.NULL_SHORT;
        }

        final int chunkIndex = getChunkIndex(index);
        final long offset = firstOffsetForData.getQuick(chunkIndex);
        return data.get(chunkIndex).get((int) (index - offset));
    }

    private final static class ChunkGetContext<ATTR extends Attributes.Any> extends DefaultGetContext<ATTR> {
        private final ResettableShortChunk resettableShortChunk = ResettableShortChunk.makeResettableChunk();

        public ChunkGetContext(final ChunkSource<ATTR> chunkSource, final int chunkCapacity, final SharedContext sharedContext) {
            super(chunkSource, chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            resettableShortChunk.close();
            super.close();
        }
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ChunkGetContext(this, chunkCapacity, sharedContext);
    }

    @Override
    public Chunk<? extends Attributes.Values> getChunk(@NotNull final GetContext context, @NotNull final OrderedKeys orderedKeys) {
        // if we can slice part of one of our backing chunks, then we will return that instead
        if (orderedKeys.isContiguous()) {
            final long firstKey = orderedKeys.firstKey();
            final int firstChunk = getChunkIndex(firstKey);
            final int lastChunk = getChunkIndex(orderedKeys.lastKey(), firstChunk);
            if (firstChunk == lastChunk) {
                final int offset = (int) (firstKey - firstOffsetForData.get(firstChunk));
                final int length = orderedKeys.intSize();
                final ShortChunk<? extends Attributes.Values> shortChunk = data.get(firstChunk);
                if (offset == 0 && length == shortChunk.size()) {
                    return shortChunk;
                }
                return ((ChunkGetContext) context).resettableShortChunk.resetFromChunk(shortChunk, offset, length);
            }
        }
        return getChunkByFilling(context, orderedKeys);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Attributes.Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final MutableInt searchStartChunkIndex = new MutableInt(0);
        final MutableInt destinationOffset = new MutableInt(0);
        orderedKeys.forAllLongRanges((s, e) -> {
            while (s <= e) {
                final int chunkIndex = getChunkIndex(s, searchStartChunkIndex.intValue());
                final int offsetWithinChunk = (int) (s - firstOffsetForData.get(chunkIndex));
                Assert.geqZero(offsetWithinChunk, "offsetWithinChunk");
                final ShortChunk<? extends Attributes.Values> shortChunk = data.get(chunkIndex);
                final int chunkSize = shortChunk.size();
                final long rangeLength = e - s + 1;
                final int chunkRemaining = chunkSize - offsetWithinChunk;
                final int length = rangeLength > chunkRemaining ? chunkRemaining : (int) rangeLength;
                Assert.gtZero(length, "length");
                destination.copyFromChunk(shortChunk, offsetWithinChunk, destinationOffset.intValue(), length);
                destinationOffset.add(length);
                searchStartChunkIndex.setValue(chunkIndex + 1);
                s += length;
            }
        });
        destination.setSize(destinationOffset.intValue());
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Attributes.Values> destination, @NotNull final OrderedKeys orderedKeys) {
        // immutable, so we can delegate to fill
        fillChunk(context, destination, orderedKeys);
    }

    /**
     * Given an index within this column's address space; return the chunk that contains the index.
     *
     * @param start the data index to find the corresponding chunk for
     * @return the chunk index within data and offsets
     */
    private int getChunkIndex(final long start) {
        return getChunkIndex(start, 0);
    }

    /**
     * Given an index within this column's address space; return the chunk that contains the index.
     *
     * @param start      the data index to find the corresponding chunk for
     * @param startChunk the first chunk that may possibly contain start
     * @return the chunk index within data and offsets
     */

    private int getChunkIndex(final long start, final int startChunk) {
        int index = firstOffsetForData.binarySearch(start, startChunk, firstOffsetForData.size());
        if (index < 0) {
            index = -index - 2;
        }
        return index;
    }

    private void addChunk(@NotNull final WritableShortChunk<? extends Attributes.Values> chunk) {
        data.add(chunk);
        firstOffsetForData.add(totalSize);
        totalSize += chunk.size();
    }

    @Override
    public void addChunk(@NotNull final WritableChunk<? extends Attributes.Values> chunk) {
        addChunk(chunk.asWritableShortChunk());
    }

    @Override
    public void clear() {
        totalSize = 0;
        data.forEach(SafeCloseable::close);
        data.clear();
        firstOffsetForData.resetQuick();
    }
}
