//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.rsp.RspIterator;
import io.deephaven.engine.rowset.impl.rsp.RspRangeBatchIterator;
import io.deephaven.engine.rowset.impl.singlerange.SingleRangeMixin;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Coverage-focused tests for RowSequence forEach abort semantics across every RowSequence implementation flavor, and
 * for iterator resume/truncation behavior (RowSequence.Iterator, RspRangeBatchIterator, RspIterator).
 */
public class RowSequenceAbortAndResumeTest {

    private static final long BLK = RspArray.BLOCK_SIZE;

    private static long[] keysOf(final RowSequence rs) {
        final long[] out = new long[(int) rs.size()];
        final int[] i = {0};
        assertTrue(rs.forEachRowKey(v -> {
            out[i[0]++] = v;
            return true;
        }));
        assertEquals(out.length, i[0]);
        return out;
    }

    private static long[][] rangesOf(final long[] keys) {
        final List<long[]> ranges = new ArrayList<>();
        int i = 0;
        while (i < keys.length) {
            int j = i;
            while (j + 1 < keys.length && keys[j + 1] == keys[j] + 1) {
                ++j;
            }
            ranges.add(new long[] {keys[i], keys[j]});
            i = j + 1;
        }
        return ranges.toArray(new long[0][]);
    }

    /**
     * Which k values (1-based abort points) to test for a sequence of {@code n} callbacks. Every k when n is small;
     * otherwise a dense prefix plus interesting interior/terminal points.
     */
    private static int[] abortPoints(final int n, final long[][] ranges) {
        if (n <= 512) {
            final int[] ks = new int[n];
            for (int i = 0; i < n; ++i) {
                ks[i] = i + 1;
            }
            return ks;
        }
        final TreeSet<Integer> ks = new TreeSet<>();
        for (int k = 1; k <= 64; ++k) {
            ks.add(k);
        }
        // Straddle every range (span) boundary.
        int consumed = 0;
        for (final long[] r : ranges) {
            consumed += (int) (r[1] - r[0] + 1);
            for (final int k : new int[] {consumed - 1, consumed, consumed + 1}) {
                if (k >= 1 && k <= n) {
                    ks.add(k);
                }
            }
        }
        ks.add(n / 2);
        ks.add(n - 1);
        ks.add(n);
        return ks.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Verify that forEachRowKey and forEachRowKeyRange invoke their consumer exactly k times when the consumer aborts
     * on its k-th invocation, for a comprehensive set of k values, and that full traversals visit the expected keys.
     */
    private static void checkAbortSemantics(final String tag, final RowSequence rs, final long[] expected) {
        assertEquals(tag, expected.length, rs.size());
        assertArrayEquals(tag, expected, keysOf(rs));

        final long[][] expectedRanges = rangesOf(expected);
        {
            final int[] count = {0};
            assertTrue(tag, rs.forEachRowKeyRange((s, e) -> {
                assertEquals(tag, expectedRanges[count[0]][0], s);
                assertEquals(tag, expectedRanges[count[0]][1], e);
                ++count[0];
                return true;
            }));
            assertEquals(tag, expectedRanges.length, count[0]);
        }

        // Key-wise aborts.
        for (final int k : abortPoints(expected.length, expectedRanges)) {
            final int[] count = {0};
            final boolean completed = rs.forEachRowKey(v -> {
                assertEquals(tag + ", k=" + k, expected[count[0]], v);
                return ++count[0] < k;
            });
            assertFalse(tag + ", k=" + k, completed);
            assertEquals(tag + ", k=" + k, k, count[0]);
        }

        // Range-wise aborts: every k.
        for (int k = 1; k <= expectedRanges.length; ++k) {
            final int kf = k;
            final int[] count = {0};
            final boolean completed = rs.forEachRowKeyRange((s, e) -> {
                assertEquals(tag + ", rk=" + kf, expectedRanges[count[0]][0], s);
                assertEquals(tag + ", rk=" + kf, expectedRanges[count[0]][1], e);
                return ++count[0] < kf;
            });
            assertFalse(tag + ", rk=" + kf, completed);
            assertEquals(tag + ", rk=" + kf, kf, count[0]);
        }
    }

    private static RspBitmap makeRspWithMixedSpans(final TreeSet<Long> model) {
        RspBitmap rb = RspBitmap.makeEmpty();
        // Isolated singleton block.
        rb = rb.addUnsafe(2 * BLK + 42);
        model.add(2 * BLK + 42);
        // A full 65536-key block span.
        rb = rb.addRangeUnsafe(4 * BLK, 5 * BLK - 1);
        for (long v = 4 * BLK; v < 5 * BLK; ++v) {
            model.add(v);
        }
        // A sparse container block.
        for (final long off : new long[] {1, 5, 6, 7, 100, 1000, 65000}) {
            rb = rb.addUnsafe(7 * BLK + off);
            model.add(7 * BLK + off);
        }
        rb.finishMutations();
        return rb;
    }

    private static long[] toArray(final TreeSet<Long> model) {
        return model.stream().mapToLong(Long::longValue).toArray();
    }

    @Test
    public void testAbortSemanticsRspBacked() {
        final TreeSet<Long> model = new TreeSet<>();
        final RspBitmap rb = makeRspWithMixedSpans(model);
        try (final WritableRowSetImpl rowSet = new WritableRowSetImpl(rb)) {
            checkAbortSemantics("rsp", rowSet, toArray(model));
        }
    }

    @Test
    public void testAbortSemanticsSortedRangesBacked() {
        SortedRanges sr = SortedRanges.makeSingleRange(10, 20);
        sr = sr.addRange(30, 40);
        sr = sr.add(45);
        sr = sr.addRange(50, 60);
        final TreeSet<Long> model = new TreeSet<>();
        for (long v = 10; v <= 20; ++v) {
            model.add(v);
        }
        for (long v = 30; v <= 40; ++v) {
            model.add(v);
        }
        model.add(45L);
        for (long v = 50; v <= 60; ++v) {
            model.add(v);
        }
        try (final WritableRowSetImpl rowSet = new WritableRowSetImpl(sr)) {
            checkAbortSemantics("sortedRanges", rowSet, toArray(model));
            // Also exercise the SortedRangesRowSequence view surface.
            try (final RowSequence view = rowSet.getRowSequenceByPosition(3, 20)) {
                final long[] all = toArray(model);
                checkAbortSemantics("sortedRangesView", view, Arrays.copyOfRange(all, 3, 23));
            }
        }
    }

    @Test
    public void testAbortSemanticsSingleRange() {
        final long[] expected = new long[21];
        for (int i = 0; i < 21; ++i) {
            expected[i] = 100 + i;
        }
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(100, 120)) {
            checkAbortSemantics("singleRange", rowSet, expected);
        }
    }

    @Test
    public void testAbortSemanticsSlicedSingleRange() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(10, 1000);
                final RowSequence sliced = rowSet.getRowSequenceByPosition(5, 100)) {
            // Slice is [15, 114].
            final long[] expected = new long[100];
            for (int i = 0; i < 100; ++i) {
                expected[i] = 15 + i;
            }
            checkAbortSemantics("slicedSingleRange", sliced, expected);

            // Exercise the rest of the SingleRangeMixin surface on the sliced sequence.
            assertTrue(sliced instanceof SingleRangeMixin);
            assertTrue(sliced.isContiguous());
            assertEquals(100, sliced.getAverageRunLengthEstimate());
            assertEquals(1, ((SingleRangeMixin) sliced).rangesCountUpperBound());

            try (final RowSequence sub = sliced.getRowSequenceByPosition(2, 10)) {
                assertEquals(17, sub.firstRowKey());
                assertEquals(26, sub.lastRowKey());
                assertEquals(10, sub.size());
            }
            try (final RowSequence sub = sliced.getRowSequenceByPosition(10, 1000)) {
                assertEquals(25, sub.firstRowKey());
                assertEquals(114, sub.lastRowKey()); // truncated at the slice end.
            }
            try (final RowSequence sub = sliced.getRowSequenceByPosition(200, 5)) {
                assertTrue(sub.isEmpty());
            }
            try (final RowSequence sub = sliced.getRowSequenceByPosition(0, 0)) {
                assertTrue(sub.isEmpty());
            }
            try (final RowSequence sub = sliced.getRowSequenceByKeyRange(0, 20)) {
                assertEquals(15, sub.firstRowKey());
                assertEquals(20, sub.lastRowKey());
            }
            try (final RowSequence sub = sliced.getRowSequenceByKeyRange(500, 600)) {
                assertTrue(sub.isEmpty());
            }
            try (final RowSequence sub = sliced.getRowSequenceByKeyRange(20, 10)) {
                assertTrue(sub.isEmpty());
            }

            try (final RowSequence.Iterator it = sliced.getRowSequenceIterator()) {
                assertTrue(it.hasMore());
                assertEquals(15, it.peekNextKey());
                final RowSequence rs1 = it.getNextRowSequenceWithLength(30);
                assertEquals(15, rs1.firstRowKey());
                assertEquals(44, rs1.lastRowKey());
                assertEquals(45, it.peekNextKey());
                final RowSequence rs2 = it.getNextRowSequenceThrough(114 + 100);
                assertEquals(45, rs2.firstRowKey());
                assertEquals(114, rs2.lastRowKey());
                assertFalse(it.hasMore());
            }
        }
    }

    @Test
    public void testAbortSemanticsKeyRangesChunkBacked() {
        try (final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(6)) {
            chunk.setSize(0);
            chunk.add(100);
            chunk.add(105);
            chunk.add(200);
            chunk.add(200);
            chunk.add(300);
            chunk.add(310);
            final RowSequence rs = RowSequenceFactory.wrapKeyRangesChunkAsRowSequence(chunk);
            final TreeSet<Long> model = new TreeSet<>();
            for (long v = 100; v <= 105; ++v) {
                model.add(v);
            }
            model.add(200L);
            for (long v = 300; v <= 310; ++v) {
                model.add(v);
            }
            checkAbortSemantics("keyRangesChunk", rs, toArray(model));
            rs.close();
        }
    }

    @Test
    public void testSortedRangesSubViewIteratorThroughClamps() {
        SortedRanges sr = SortedRanges.makeSingleRange(10, 20);
        sr = sr.addRange(30, 40);
        sr = sr.addRange(50, 60);
        try (final WritableRowSetImpl rowSet = new WritableRowSetImpl(sr);
                final RowSequence view = rowSet.getRowSequenceByPosition(3, 20)) {
            // View is positions 3..22: keys 13..20, 30..40, 50.
            assertEquals(13, view.firstRowKey());
            assertEquals(50, view.lastRowKey());
            try (final RowSequence.Iterator it = view.getRowSequenceIterator()) {
                final RowSequence rs1 = it.getNextRowSequenceThrough(35);
                final long[] expected1 = new long[] {13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 33, 34, 35};
                assertArrayEquals(expected1, keysOf(rs1));
                assertTrue(it.hasMore());
                // maxKey far past the truncated view's end: must clamp to the view, not the whole rowset.
                final RowSequence rs2 = it.getNextRowSequenceThrough(1_000_000);
                final long[] expected2 = new long[] {36, 37, 38, 39, 40, 50};
                assertArrayEquals(expected2, keysOf(rs2));
                assertFalse(it.hasMore());
            }
        }
    }

    @Test
    public void testRspIteratorGetNextWithLengthThenPeek() {
        final TreeSet<Long> modelSet = new TreeSet<>();
        RspBitmap rb = RspBitmap.makeEmpty();
        for (final long v : new long[] {5, 6, 7, 100}) {
            rb = rb.addUnsafe(v);
            modelSet.add(v);
        }
        rb = rb.addRangeUnsafe(2 * BLK, 3 * BLK - 1);
        for (long v = 2 * BLK; v < 3 * BLK; ++v) {
            modelSet.add(v);
        }
        rb = rb.addUnsafe(5 * BLK + 3);
        modelSet.add(5 * BLK + 3);
        rb.finishMutations();
        final long[] model = toArray(modelSet);

        try (final RowSequence.Iterator it = rb.getRowSequenceIterator()) {
            int consumed = 0;
            final int step = 7; // smaller than every span.
            while (it.hasMore()) {
                assertEquals(model[consumed], it.peekNextKey());
                final RowSequence rs = it.getNextRowSequenceWithLength(step);
                final int expectedCount = Math.min(step, model.length - consumed);
                assertEquals(expectedCount, rs.size());
                assertEquals(model[consumed], rs.firstRowKey());
                assertEquals(model[consumed + expectedCount - 1], rs.lastRowKey());
                consumed += expectedCount;
            }
            assertEquals(model.length, consumed);
            assertEquals(RowSequence.NULL_ROW_KEY, it.peekNextKey());
        }
    }

    @Test
    public void testSingleRangeIteratorSecondThroughAndAdvance() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(100, 200);
                final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            final RowSequence rs1 = it.getNextRowSequenceThrough(150);
            assertEquals(100, rs1.firstRowKey());
            assertEquals(150, rs1.lastRowKey());
            // A second call with the same maxKey must return the empty sequence.
            final RowSequence rs2 = it.getNextRowSequenceThrough(150);
            assertTrue(rs2.isEmpty());
            final RowSequence rs3 = it.getNextRowSequenceThrough(160);
            assertEquals(151, rs3.firstRowKey());
            assertEquals(160, rs3.lastRowKey());
            // advance past the range end exhausts the iterator.
            assertFalse(it.advance(200 + 10));
            assertFalse(it.hasMore());
            assertTrue(it.getNextRowSequenceThrough(1000).isEmpty());
        }
    }

    @Test
    public void testRspRangeBatchIteratorResumeAcrossSmallChunks() {
        final TreeSet<Long> modelSet = new TreeSet<>();
        RspBitmap rb = RspBitmap.makeEmpty();
        // Container whose last range ends exactly at block-last...
        rb = rb.addRangeUnsafe(3 * BLK + 10, 3 * BLK + 12);
        rb = rb.addRangeUnsafe(4 * BLK - 5, 4 * BLK - 1);
        // ... followed immediately by a full-block span (blocks 4 and 5).
        rb = rb.addRangeUnsafe(4 * BLK, 6 * BLK - 1);
        // And a trailing singleton.
        rb = rb.addUnsafe(7 * BLK + 5);
        rb.finishMutations();
        for (long v = 3 * BLK + 10; v <= 3 * BLK + 12; ++v) {
            modelSet.add(v);
        }
        for (long v = 4 * BLK - 5; v < 6 * BLK; ++v) {
            modelSet.add(v);
        }
        modelSet.add(7 * BLK + 5);
        final long[] model = toArray(modelSet);
        assertEquals(model.length, rb.getCardinality());

        final long maxCount = rb.getCardinality() - 10;
        try (final RspRangeBatchIterator rit = rb.getRangeBatchIterator(0, maxCount);
                final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(4)) {
            long produced = 0;
            long lastEnd = -2;
            while (rit.hasNext()) {
                final int nRanges = rit.fillRangeChunk(chunk, 0);
                assertTrue(nRanges >= 0);
                if (nRanges == 0) {
                    assertFalse(rit.hasNext());
                    break;
                }
                for (int r = 0; r < nRanges; ++r) {
                    final long s = chunk.get(2 * r);
                    final long e = chunk.get(2 * r + 1);
                    assertTrue("range must move forward", s > lastEnd);
                    for (long v = s; v <= e; ++v) {
                        assertEquals(model[(int) produced], v);
                        ++produced;
                    }
                    lastEnd = e;
                }
            }
            assertEquals(maxCount, produced);
        }

        // A batch iterator whose initial seek lands inside the leading full-block span.
        RspBitmap rb2 = RspBitmap.makeEmpty();
        rb2 = rb2.addRangeUnsafe(2 * BLK, 4 * BLK - 1); // full blocks 2-3.
        rb2 = rb2.addUnsafe(6 * BLK + 1);
        rb2.finishMutations();
        try (final RspRangeBatchIterator rit = rb2.getRangeBatchIterator(10, 50);
                final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(4)) {
            final int nRanges = rit.fillRangeChunk(chunk, 0);
            assertEquals(1, nRanges);
            assertEquals(2 * BLK + 10, chunk.get(0));
            assertEquals(2 * BLK + 59, chunk.get(1));
            assertFalse(rit.hasNext());
        }
    }

    @Test
    public void testRspIteratorForEachLongAndNextLong() {
        final TreeSet<Long> modelSet = new TreeSet<>();
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addUnsafe(BLK + 42); // singleton span.
        modelSet.add(BLK + 42);
        rb = rb.addRangeUnsafe(3 * BLK, 4 * BLK - 1); // full block span.
        for (long v = 3 * BLK; v < 4 * BLK; ++v) {
            modelSet.add(v);
        }
        for (final long off : new long[] {1, 7, 9, 500}) { // container span.
            rb = rb.addUnsafe(6 * BLK + off);
            modelSet.add(6 * BLK + off);
        }
        rb.finishMutations();
        final long[] model = toArray(modelSet);

        // Full forEachLong traversal.
        try (final RspIterator it = rb.getIterator()) {
            final int[] count = {0};
            final boolean completed = it.forEachLong(v -> {
                assertEquals(model[count[0]], v);
                ++count[0];
                return true;
            });
            assertTrue(completed);
            assertEquals(model.length, count[0]);
        }

        // Aborting forEachLong: in the singleton span, inside the full block span, and in the container span.
        for (final int k : new int[] {1, 2, 100, BLOCK_ABORT, model.length - 2, model.length}) {
            try (final RspIterator it = rb.getIterator()) {
                final int[] count = {0};
                final boolean completed = it.forEachLong(v -> {
                    assertEquals(model[count[0]], v);
                    return ++count[0] < k;
                });
                assertFalse("k=" + k, completed);
                assertEquals("k=" + k, k, count[0]);
            }
        }

        // hasNext/nextLong drain.
        try (final RspIterator it = rb.getIterator()) {
            int i = 0;
            while (it.hasNext()) {
                assertEquals(model[i], it.nextLong());
                ++i;
            }
            assertEquals(model.length, i);
        }
    }

    private static final int BLOCK_ABORT = 1 + (int) BLK / 2; // abort point inside the full block span.
}
