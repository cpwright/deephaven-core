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
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

public class ChunkedAjUtils {
    /**
     * Fill stamps and rowKeys with one chunk of the rows to add, sorted in the order of the SSA. The chunks are
     * processed from the highest positions down, so processing index 0 holds the last chunkSize positions of rows.
     */
    public static void fillSortedAdditionChunk(RowSet rows, long processingIndex, long chunks, int chunkSize,
            ColumnSource<?> stampSource, ChunkSource.FillContext fillContext, WritableChunk<Values> stamps,
            WritableLongChunk<RowKeys> rowKeys, LongSortKernel<Values, RowKeys> sortKernel) {
        final long chunkStart = (chunks - processingIndex - 1) * chunkSize;
        try (final RowSet chunkRows = rows.subSetByPositionRange(chunkStart, chunkStart + chunkSize)) {
            stampSource.fillChunk(fillContext, stamps, chunkRows);
            rowKeys.setSize(chunkRows.intSize());
            chunkRows.fillRowKeyChunk(rowKeys);
        }
        sortKernel.sort(rowKeys, stamps);
    }

    /**
     * Insert the chunks of rows from firstProcessingIndex on into the SSA, with stamps and rowKeys as scratch space.
     */
    public static void insertAdditionChunks(SegmentedSortedArray ssa, RowSet rows, long firstProcessingIndex,
            long chunks, int chunkSize, ColumnSource<?> stampSource, ChunkSource.FillContext fillContext,
            WritableChunk<Values> stamps, WritableLongChunk<RowKeys> rowKeys,
            LongSortKernel<Values, RowKeys> sortKernel) {
        for (long ii = firstProcessingIndex; ii < chunks; ++ii) {
            fillSortedAdditionChunk(rows, ii, chunks, chunkSize, stampSource, fillContext, stamps, rowKeys,
                    sortKernel);
            ssa.insert(stamps, rowKeys);
        }
    }

    /**
     * Remove rows from the SSA, in chunks of chunkSize, with stamps and rowKeys as scratch space. The stamps are the
     * previous values of stampSource.
     */
    public static void removeRemovalChunks(SegmentedSortedArray ssa, RowSequence rows, int chunkSize,
            ColumnSource<?> stampSource, ChunkSource.FillContext fillContext, WritableChunk<Values> stamps,
            WritableLongChunk<RowKeys> rowKeys, LongSortKernel<Values, RowKeys> sortKernel) {
        try (final RowSequence.Iterator removeIt = rows.getRowSequenceIterator()) {
            while (removeIt.hasMore()) {
                final RowSequence chunkOk = removeIt.getNextRowSequenceWithLength(chunkSize);
                stampSource.fillPrevChunk(fillContext, stamps, chunkOk);
                chunkOk.fillRowKeyChunk(rowKeys);
                sortKernel.sort(rowKeys, stamps);
                ssa.remove(stamps, rowKeys);
            }
        }
    }

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
}
