//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

public interface SegmentedSortedArray extends LongSizedDataStructure {
    boolean SEGMENTED_SORTED_ARRAY_VALIDATION =
            Configuration.getInstance().getBooleanWithDefault("SegmentedSortedArray.validation", false);

    /**
     * Make a SegmentedSortedArray for values of the given type.
     *
     * @param chunkType the chunk type of the values
     * @param equalsConsistent true when values of the data type compare equal exactly when they are equal (see
     *        {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}), which selects the
     *        EqualsConsistentObject SSA that tests Object equality with {@code equals}; when false, Object equality is
     *        tested with {@link ObjectComparisons#compareEquals(Object, Object)}. Other chunk types ignore it. An
     *        operation reads the registry once and passes the same decision to every kernel it creates, so its kernels
     *        come from one family.
     * @param reverse true for a descending SSA
     * @param nodeSize the leaf size of the SSA
     * @return a new SegmentedSortedArray
     */
    static SegmentedSortedArray make(ChunkType chunkType, boolean equalsConsistent, boolean reverse, int nodeSize) {
        return makeFactory(chunkType, equalsConsistent, reverse, nodeSize).get();
    }

    /**
     * Make a factory for SegmentedSortedArrays of values of the given type, choosing the implementation once.
     *
     * @param chunkType the chunk type of the values
     * @param equalsConsistent true when values of the data type compare equal exactly when they are equal (see
     *        {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}), which selects the
     *        EqualsConsistentObject SSA that tests Object equality with {@code equals}; when false, Object equality is
     *        tested with {@link ObjectComparisons#compareEquals(Object, Object)}. Other chunk types ignore it. An
     *        operation reads the registry once and passes the same decision to every kernel it creates, so its kernels
     *        come from one family.
     * @param reverse true for a descending SSA
     * @param nodeSize the leaf size of the SSA
     * @return a factory for new SegmentedSortedArrays
     */
    static Supplier<SegmentedSortedArray> makeFactory(ChunkType chunkType, boolean equalsConsistent, boolean reverse,
            int nodeSize) {
        switch (chunkType) {
            case Char:
                return reverse ? () -> new CharReverseSegmentedSortedArray(nodeSize)
                        : () -> new CharSegmentedSortedArray(nodeSize);
            case Byte:
                return reverse ? () -> new ByteReverseSegmentedSortedArray(nodeSize)
                        : () -> new ByteSegmentedSortedArray(nodeSize);
            case Short:
                return reverse ? () -> new ShortReverseSegmentedSortedArray(nodeSize)
                        : () -> new ShortSegmentedSortedArray(nodeSize);
            case Int:
                return reverse ? () -> new IntReverseSegmentedSortedArray(nodeSize)
                        : () -> new IntSegmentedSortedArray(nodeSize);
            case Long:
                return reverse ? () -> new LongReverseSegmentedSortedArray(nodeSize)
                        : () -> new LongSegmentedSortedArray(nodeSize);
            case Float:
                return reverse ? () -> new FloatReverseSegmentedSortedArray(nodeSize)
                        : () -> new FloatSegmentedSortedArray(nodeSize);
            case Double:
                return reverse ? () -> new DoubleReverseSegmentedSortedArray(nodeSize)
                        : () -> new DoubleSegmentedSortedArray(nodeSize);
            case Object:
                if (equalsConsistent) {
                    return reverse ? () -> new EqualsConsistentObjectReverseSegmentedSortedArray(nodeSize)
                            : () -> new EqualsConsistentObjectSegmentedSortedArray(nodeSize);
                }
                return reverse ? () -> new ObjectReverseSegmentedSortedArray(nodeSize)
                        : () -> new ObjectSegmentedSortedArray(nodeSize);
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Insert new valuesToInsert into this SSA. The valuesToInsert to insert must be sorted.
     * 
     * @param valuesToInsert the valuesToInsert to insert
     * @param indicesToInsert the corresponding indicesToInsert
     */
    void insert(Chunk<? extends Any> valuesToInsert, LongChunk<? extends RowKeys> indicesToInsert);

    /**
     * Remove valuesToRemove from this SSA. The valuesToRemove to remove must be sorted.
     * 
     * @param valuesToRemove the valuesToRemove to remove
     * @param indicesToRemove the corresponding indices
     */
    void remove(Chunk<? extends Any> valuesToRemove, LongChunk<? extends RowKeys> indicesToRemove);

    /**
     * Remove the values and indices referenced in stampChunk and indicesToRemove. Fill priorRedirections with the
     * redirection value immediately preceding the removed value.
     * 
     * @param stampChunk the values to remove
     * @param indicesToRemove the indices (parallel to the values)
     * @param priorRedirections the output prior redirections (parallel to valeus/indices)
     */
    void removeAndGetPrior(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> indicesToRemove,
            WritableLongChunk<? extends RowKeys> priorRedirections);

    /**
     * Insert valuesToInsert into this SSA, and fill nextValue with the value that follows each inserted value in this
     * SSA after the insertion. The valuesToInsert must be sorted, with ties broken by the row key.
     * <p>
     * Only the last inserted value can lack a next value, which happens when it becomes the last value of this SSA; its
     * position in nextValue is left unchanged.
     *
     * @param valuesToInsert the values to insert
     * @param indicesToInsert the corresponding row keys
     * @param nextValue the output next values, parallel to valuesToInsert
     * @return the number of leading positions of nextValue that were filled, which is the size of valuesToInsert, or
     *         one less when the last inserted value has no next value
     */
    <T extends Any> int insertAndGetNextValue(Chunk<T> valuesToInsert, LongChunk<? extends RowKeys> indicesToInsert,
            WritableChunk<T> nextValue);

    /**
     * Fill nextValue with the value that follows each of the given values in this SSA. Every given value and row key
     * must be present in this SSA, sorted with ties broken by the row key, which makes the result the next value that
     * {@link #insertAndGetNextValue} reports for a value inserted into an SSA that already holds the values that follow
     * it.
     * <p>
     * Only the last given value can lack a next value, which happens when it is the last value of this SSA; its
     * position in nextValue is left unchanged.
     *
     * @param presentValues the values to look up, which must be present in this SSA
     * @param presentRowKeys the corresponding row keys
     * @param nextValue the output next values, parallel to presentValues
     * @return the number of leading positions of nextValue that were filled, which is the size of presentValues, or one
     *         less when the last value has no next value
     */
    <T extends Any> int findNextValues(Chunk<T> presentValues, LongChunk<? extends RowKeys> presentRowKeys,
            WritableChunk<T> nextValue);

    /**
     * Fill priorRowKeys with the row key that precedes each of the given values in this SSA, or
     * {@link io.deephaven.engine.rowset.RowSequence#NULL_ROW_KEY} when no value precedes it. No given value and row key
     * may be present in this SSA, and they must be sorted with ties broken by the row key, which makes the result the
     * prior that {@link #removeAndGetPrior} reports for a value removed from an SSA that holds only the values that
     * survive it.
     *
     * @param absentValues the values to look up, which must not be present in this SSA
     * @param absentRowKeys the corresponding row keys
     * @param priorRowKeys the output prior row keys, parallel to absentValues
     */
    void findPriorRowKeys(Chunk<? extends Any> absentValues, LongChunk<? extends RowKeys> absentRowKeys,
            WritableLongChunk<? extends RowKeys> priorRowKeys);

    void applyShift(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> keyChunk, long shiftDelta);

    void applyShiftReverse(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> keyChunk,
            long shiftDelta);

    int getNodeSize();

    /**
     * Call the longConsumer for each of the long row keys in this SegmentedSortedArray.
     *
     * @param longConsumer the long consumer to call
     */
    void forAllKeys(LongConsumer longConsumer);

    boolean isReversed();

    /**
     * @return the first row key in this SSA, RowSet.NULL_ROW_KEY when empty.
     */
    long getFirst();

    /**
     * @return the last row key in this SSA, RowSet.NULL_ROW_KEY when empty.
     */
    long getLast();
}
