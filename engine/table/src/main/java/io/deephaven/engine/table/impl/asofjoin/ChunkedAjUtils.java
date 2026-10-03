//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableLongChunk;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.Nullable;

public class ChunkedAjUtils {
    static void bothIncrementalLeftSsaShift(RowSetShiftData shiftData, SegmentedSortedArray leftSsa,
            RowSet restampRemovals, QueryTable table,
            int nodeSize, ColumnSource<?> stampSource) {
        final ChunkType stampChunkType = stampSource.getChunkType();
        final SortingOrder sortOrder = leftSsa.isReversed() ? SortingOrder.Descending : SortingOrder.Ascending;

        final RowSet prevRowSet = table.getRowSet().prev();
        try (final RowSet relevantShiftedRows = relevantShiftedRows(shiftData, prevRowSet, restampRemovals);
                final SizedSafeCloseable<ColumnSource.FillContext> shiftFillContext =
                        new SizedSafeCloseable<>(stampSource::makeFillContext);
                final SizedSafeCloseable<LongSortKernel<Values, RowKeys>> shiftSortContext =
                        new SizedSafeCloseable<>(
                                size -> LongSortKernel.makeContext(stampChunkType, sortOrder, size, true));
                final SizedLongChunk<RowKeys> stampKeys = new SizedLongChunk<>();
                final SizedChunk<Values> stampValues = new SizedChunk<>(stampChunkType)) {
            final RowSetShiftData.Iterator sit = shiftData.applyIterator();
            while (sit.hasNext()) {
                sit.next();
                try (final RowSet rowSetToShift =
                        relevantShiftedRows.subSetByKeyRange(sit.beginRange(), sit.endRange())) {
                    if (rowSetToShift.isNonempty()) {
                        applyOneShift(leftSsa, nodeSize, stampSource, shiftFillContext, shiftSortContext, stampKeys,
                                stampValues, sit, rowSetToShift);
                    }
                }
            }
        }
    }

    static void applyOneShift(SegmentedSortedArray leftSsa, int nodeSize, ColumnSource<?> stampSource,
            SizedSafeCloseable<ChunkSource.FillContext> shiftFillContext,
            SizedSafeCloseable<LongSortKernel<Values, RowKeys>> shiftSortContext,
            SizedLongChunk<RowKeys> stampKeys, SizedChunk<Values> stampValues, RowSetShiftData.Iterator sit,
            RowSet rowSetToShift) {
        final long rowsToShift = rowSetToShift.size();
        final int chunkSize = (int) Math.min(nodeSize, rowsToShift);
        shiftFillContext.ensureCapacity(chunkSize);
        shiftSortContext.ensureCapacity(chunkSize);
        stampValues.ensureCapacity(chunkSize);
        stampKeys.ensureCapacity(chunkSize);
        if (sit.polarityReversed()) {
            // a positive shift moves the highest row keys first, so no row is shifted onto a key that has yet to be
            // shifted
            for (long endPosition = rowsToShift; endPosition > 0; endPosition -= chunkSize) {
                try (final RowSet chunkOk =
                        rowSetToShift.subSetByPositionRange(Math.max(0, endPosition - chunkSize), endPosition)) {
                    stampSource.fillPrevChunk(shiftFillContext.get(), stampValues.get(), chunkOk);
                    chunkOk.fillRowKeyChunk(stampKeys.get());
                    shiftSortContext.get().sort(stampKeys.get(), stampValues.get());
                    leftSsa.applyShiftReverse(stampValues.get(), stampKeys.get(), sit.shiftDelta());
                }
            }
        } else {
            try (final RowSequence.Iterator shiftIt = rowSetToShift.getRowSequenceIterator()) {
                while (shiftIt.hasMore()) {
                    final RowSequence chunkOk = shiftIt.getNextRowSequenceWithLength(chunkSize);
                    stampSource.fillPrevChunk(shiftFillContext.get(), stampValues.get(), chunkOk);
                    chunkOk.fillRowKeyChunk(stampKeys.get());
                    shiftSortContext.get().sort(stampKeys.get(), stampValues.get());
                    leftSsa.applyShift(stampValues.get(), stampKeys.get(), sit.shiftDelta());
                }
            }
        }
    }

    /**
     * Returns the rows of {@code prevRowSet} that fall within a range of {@code shifted}, excluding
     * {@code restampRemovals}. The result is proportional to the shifted rows and the removals rather than to the whole
     * table.
     *
     * @param shifted the upstream shift data
     * @param prevRowSet the table's row set in the previous (pre-shift) key space
     * @param restampRemovals rows already removed from the SSA this cycle, in the previous key space
     * @return the rows that must be shifted within the SSA
     */
    public static WritableRowSet relevantShiftedRows(RowSetShiftData shifted, RowSet prevRowSet,
            RowSet restampRemovals) {
        final RowSetBuilderSequential relevantShiftKeys = RowSetFactory.builderSequential();

        try (final RowSet.RangeIterator it = prevRowSet.rangeIterator()) {
            for (int ii = 0; ii < shifted.size(); ++ii) {
                final long beginRange = shifted.getBeginRange(ii);
                final long endRange = shifted.getEndRange(ii);
                if (!it.advance(beginRange)) {
                    break;
                }
                if (it.currentRangeStart() > endRange) {
                    continue;
                }

                while (true) {
                    final long startOfNewRange = Math.max(it.currentRangeStart(), beginRange);
                    final long endOfNewRange = Math.min(it.currentRangeEnd(), endRange);
                    relevantShiftKeys.appendRange(startOfNewRange, endOfNewRange);
                    if (it.currentRangeEnd() < endRange) {
                        if (!it.hasNext())
                            break;
                        it.next();
                        if (it.currentRangeStart() > endRange) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        final WritableRowSet relevantShiftedRows = relevantShiftKeys.build();
        relevantShiftedRows.remove(restampRemovals);
        return relevantShiftedRows;
    }

    /**
     * Returns the current right row keys that may hold a different right row than the same key held in the previous
     * cycle: the added keys and every key in the destination of a shift. Any other key holds the same right row as in
     * the previous cycle.
     *
     * @param added the right side's added rows
     * @param shifted the right side's shifts
     * @return the right row keys whose row may have been replaced
     */
    public static WritableRowSet replacedRightKeys(RowSet added, RowSetShiftData shifted) {
        final RowSetBuilderSequential shiftDestinations = RowSetFactory.builderSequential();
        for (int ii = 0; ii < shifted.size(); ++ii) {
            final long shiftDelta = shifted.getShiftDelta(ii);
            shiftDestinations.appendRange(shifted.getBeginRange(ii) + shiftDelta,
                    shifted.getEndRange(ii) + shiftDelta);
        }
        final WritableRowSet replaced = shiftDestinations.build();
        replaced.insert(added);
        return replaced;
    }

    /**
     * Returns the rows of {@code currentRows} whose right values may differ from those of the previous cycle. The two
     * sequences correspond by position. A row whose current and previous redirections differ, or are equal but name a
     * key in {@code replacedRightKeys}, may have a different value in every right column, and sets
     * {@code redirectionChanged}. A row whose redirection is unchanged and names a key in {@code modifiedRightKeys}
     * differs only in the right columns modified upstream, and sets {@code addedColumnsModified}.
     *
     * @param currentRows result rows in the current key space
     * @param previousRows the same result rows, in the previous key space
     * @param rowRedirection the result's row redirection, which tracks previous values
     * @param replacedRightKeys the keys from {@link #replacedRightKeys}, or null when the right side did not change
     * @param modifiedRightKeys the right side's modified rows when an added column was modified, otherwise null
     * @param maxChunkSize the largest number of rows to read at once
     * @param firstOnly whether to return as soon as one row sets {@code redirectionChanged}
     * @param redirectionChanged set when a reported row may differ in every right column
     * @param addedColumnsModified set when a reported row differs only in the right columns modified upstream
     * @return the rows whose right values may have changed; when {@code firstOnly} is set, only rows up to the first
     *         that sets {@code redirectionChanged}
     */
    public static WritableRowSet changedRedirections(RowSequence currentRows, RowSequence previousRows,
            RowRedirection rowRedirection, @Nullable RowSet replacedRightKeys, @Nullable RowSet modifiedRightKeys,
            int maxChunkSize, boolean firstOnly, MutableBoolean redirectionChanged,
            MutableBoolean addedColumnsModified) {
        final long size = currentRows.size();
        Assert.eq(previousRows.size(), "previousRows.size()", size, "currentRows.size()");
        if (size == 0) {
            return RowSetFactory.empty();
        }
        // a key outside [first, last] of a set is not in it; NULL_ROW_KEY is below any row key
        final boolean anyReplaced = replacedRightKeys != null && replacedRightKeys.isNonempty();
        final long replacedFirst = anyReplaced ? replacedRightKeys.firstRowKey() : Long.MAX_VALUE;
        final long replacedLast = anyReplaced ? replacedRightKeys.lastRowKey() : Long.MIN_VALUE;
        final boolean anyModified = modifiedRightKeys != null && modifiedRightKeys.isNonempty();
        final long modifiedFirst = anyModified ? modifiedRightKeys.firstRowKey() : Long.MAX_VALUE;
        final long modifiedLast = anyModified ? modifiedRightKeys.lastRowKey() : Long.MIN_VALUE;

        final int chunkSize = (int) Math.min(maxChunkSize, size);
        final RowSetBuilderSequential changedBuilder = RowSetFactory.builderSequential();
        try (final ChunkSource.FillContext currentContext = rowRedirection.makeFillContext(chunkSize, null);
                final ChunkSource.FillContext previousContext = rowRedirection.makeFillContext(chunkSize, null);
                final WritableLongChunk<RowKeys> currentRedirections = WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<RowKeys> previousRedirections =
                        WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<RowKeys> currentKeys = WritableLongChunk.makeWritableChunk(chunkSize);
                final RowSequence.Iterator currentIt = currentRows.getRowSequenceIterator();
                final RowSequence.Iterator previousIt = previousRows.getRowSequenceIterator()) {
            while (currentIt.hasMore()) {
                final RowSequence currentChunk = currentIt.getNextRowSequenceWithLength(chunkSize);
                final RowSequence previousChunk = previousIt.getNextRowSequenceWithLength(chunkSize);
                rowRedirection.fillChunk(currentContext, currentRedirections, currentChunk);
                rowRedirection.fillPrevChunk(previousContext, previousRedirections, previousChunk);
                currentChunk.fillRowKeyChunk(currentKeys);
                for (int ii = 0; ii < currentKeys.size(); ++ii) {
                    final long currentRedirection = currentRedirections.get(ii);
                    if (currentRedirection != previousRedirections.get(ii)
                            || (currentRedirection >= replacedFirst && currentRedirection <= replacedLast
                                    && replacedRightKeys.find(currentRedirection) >= 0)) {
                        changedBuilder.appendKey(currentKeys.get(ii));
                        redirectionChanged.setTrue();
                        if (firstOnly) {
                            return changedBuilder.build();
                        }
                    } else if (currentRedirection >= modifiedFirst && currentRedirection <= modifiedLast
                            && modifiedRightKeys.find(currentRedirection) >= 0) {
                        changedBuilder.appendKey(currentKeys.get(ii));
                        addedColumnsModified.setTrue();
                    }
                }
            }
        }
        return changedBuilder.build();
    }
}
