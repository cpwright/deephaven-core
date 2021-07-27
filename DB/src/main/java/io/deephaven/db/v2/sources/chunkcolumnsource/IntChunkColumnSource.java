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
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

public class IntChunkColumnSource extends AbstractColumnSource<Integer> implements ImmutableColumnSourceGetDefaults.ForInt, ChunkColumnSource<Integer> {
    private final ArrayList<IntChunk<? extends Attributes.Values>> data = new ArrayList<>();
    private final TLongArrayList offsets = new TLongArrayList();
    private long totalSize = 0;

    // region constructor
    protected IntChunkColumnSource() {
        super(Integer.class);
    }
    // endregion constructor

    @Override
    public int getInt(long index) {
        if (index < 0 || index >= totalSize) {
            return QueryConstants.NULL_INT;
        }

        final int chunkIndex = getChunkIndex(index);
        final long offset = offsets.getQuick(chunkIndex);
        return data.get(chunkIndex).get((int)(index - offset));
    }

    private final static class ChunkGetContext<ATTR extends Attributes.Any> extends DefaultGetContext<ATTR> {
        private final ResettableIntChunk resettableIntChunk = ResettableIntChunk.makeResettableChunk();

        public ChunkGetContext(ChunkSource<ATTR> chunkSource, int chunkCapacity, SharedContext sharedContext) {
            super(chunkSource, chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            resettableIntChunk.close();
        }
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new ChunkGetContext(this, chunkCapacity, sharedContext);
    }

    @Override
    public Chunk<? extends Attributes.Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        // if we can slice part of one of our backing chunks, then we will return that instead
        if (orderedKeys.isContiguous()) {
            final long firstKey = orderedKeys.firstKey();
            final int firstChunk = getChunkIndex(firstKey);
            final int lastChunk = getChunkIndex(orderedKeys.lastKey(), firstChunk);
            if (firstChunk == lastChunk) {
                final int offset = (int)(firstKey - offsets.get(firstChunk));
                final int length = orderedKeys.intSize();
                final IntChunk<? extends Attributes.Values> intChunk = data.get(firstChunk);
                if (offset == 0 && length == intChunk.size()) {
                    return intChunk;
                }
                return ((ChunkGetContext)context).resettableIntChunk.resetFromChunk(intChunk, offset, length);
            }
        }
        return getChunkByFilling(context, orderedKeys);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
        final MutableInt startChunkIndex = new MutableInt(0);
        final MutableInt destinationOffset = new MutableInt(0);
        orderedKeys.forAllLongRanges((s, e) -> {
            while (s <= e) {
                final int chunkIndex = getChunkIndex(s, startChunkIndex.intValue());
                final int offsetWithinChunk = (int)(s - offsets.get(chunkIndex));
                Assert.geqZero(offsetWithinChunk, "offsetWithinChunk");
                final IntChunk<? extends Attributes.Values> intChunk = data.get(chunkIndex);
                final int chunkSize = intChunk.size();
                final long rangeLength = e - s + 1;
                final int chunkRemaining = chunkSize - offsetWithinChunk;
                final int length = rangeLength > chunkRemaining ? chunkRemaining : (int) rangeLength;
                Assert.gtZero(length, "length");
                destination.copyFromChunk(intChunk, offsetWithinChunk, destinationOffset.intValue(), length);
                destinationOffset.add(length);
                startChunkIndex.setValue(chunkIndex + 1);
                s += length;
            }
        });
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
        // immutable, so we can delegate to fill
        fillChunk(context, destination, orderedKeys);
    }

    private int getChunkIndex(final long start) {
        return getChunkIndex(start, 0);
    }

    private int getChunkIndex(final long start, final int startIndex) {
        int index = offsets.binarySearch(start, startIndex, offsets.size());
        if (index < 0) {
            index = -index - 2;
        }
        return index;
    }

    public void addChunk(final IntChunk<? extends Attributes.Values> chunk) {
        data.add(chunk);
        offsets.add(totalSize);
        totalSize += chunk.size();
    }

    @Override
    public void addChunk(final Chunk<? extends Attributes.Values> chunk) {
        addChunk(chunk.asIntChunk());
    }

    @Override
    public void clear() {
        totalSize = 0;
        data.clear();
        offsets.clear();
    }
}
