//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSequence;
import org.junit.Test;

import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Coverage-focused tests for RspArray navigation: rank/position lookups with and without the cardinality accumulator
 * cache ("acc"), full-block-span merge and collapse shapes, containment fast paths, and RspRowSequence views.
 */
public class RspArrayNavigationTest {

    private static final long BLK = BLOCK_SIZE;

    // Simple growing long buffer for building expected key models in increasing order.
    private static final class LongBuf {
        private long[] a = new long[1 << 16];
        private int n;

        void add(final long v) {
            if (n == a.length) {
                a = Arrays.copyOf(a, 2 * a.length);
            }
            a[n++] = v;
        }

        void addRange(final long first, final long last) {
            for (long v = first; v <= last; ++v) {
                add(v);
            }
        }

        long[] done() {
            return Arrays.copyOf(a, n);
        }
    }

    private static long[] keysOf(final RowSequence rs) {
        final long[] out = new long[(int) rs.size()];
        final int[] i = {0};
        final boolean completed = rs.forEachRowKey(v -> {
            out[i[0]++] = v;
            return true;
        });
        assertTrue(completed);
        assertEquals(out.length, i[0]);
        return out;
    }

    private static long[] keysOfByRanges(final RowSequence rs) {
        final LongBuf buf = new LongBuf();
        final boolean completed = rs.forEachRowKeyRange((first, last) -> {
            assertTrue(first <= last);
            buf.addRange(first, last);
            return true;
        });
        assertTrue(completed);
        return buf.done();
    }

    private static long[] slice(final long[] model, final long start, final long len) {
        final int from = (int) Math.min(start, model.length);
        final int to = (int) Math.min(start + len, model.length);
        return Arrays.copyOfRange(model, from, to);
    }

    private static PrimitiveIterator.OfLong positions(final long... positions) {
        return Arrays.stream(positions).iterator();
    }

    /**
     * Build a bitmap with 24 spans in distinct (non-adjacent) blocks, mixing singleton spans, small containers, full
     * block spans, and multi-range containers. Returns the bitmap; fills {@code modelBuf} with the expected keys in
     * increasing order.
     */
    private static RspBitmap buildMultiSpan(final LongBuf modelBuf) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int b = 0; b < 24; ++b) {
            final long base = (2L * b + 1) * BLK; // distinct, non-adjacent blocks.
            switch (b % 4) {
                case 0: // singleton span
                    rb = rb.addUnsafe(base + 7);
                    modelBuf.add(base + 7);
                    break;
                case 1: // small container
                    rb = rb.addUnsafe(base + 1);
                    rb = rb.addUnsafe(base + 9);
                    rb = rb.addRangeUnsafe(base + 300, base + 310);
                    modelBuf.add(base + 1);
                    modelBuf.add(base + 9);
                    modelBuf.addRange(base + 300, base + 310);
                    break;
                case 2: // full block span
                    rb = rb.addRangeUnsafe(base, base + BLOCK_LAST);
                    modelBuf.addRange(base, base + BLOCK_LAST);
                    break;
                default: // multi-range container
                    rb = rb.addRangeUnsafe(base + 10, base + 50);
                    rb = rb.addRangeUnsafe(base + 1000, base + 1010);
                    modelBuf.addRange(base + 10, base + 50);
                    modelBuf.addRange(base + 1000, base + 1010);
                    break;
            }
        }
        rb.finishMutations();
        return rb;
    }

    @Test
    public void testGetRowSequenceByPositionWithAcc() {
        final LongBuf modelBuf = new LongBuf();
        final RspBitmap rb = buildMultiSpan(modelBuf);
        final long[] model = modelBuf.done();
        final long card = rb.getCardinality();
        assertEquals(model.length, card);
        assertTrue("expected enough spans to populate the acc cache", rb.size > 8);

        final long[][] queries = {
                {0, 5}, // start of first span
                {0, card}, // everything
                {card / 2, 1}, // single key in the middle
                {card - 1, 100}, // last key, length overflowing past the end
                {3, card}, // start mid-first-span, length overflowing
                {17, 70000}, // crosses multiple spans including a full block span
                {card / 3, card / 3}, // interior slab
        };
        for (final long[] q : queries) {
            final String m = "start=" + q[0] + ", len=" + q[1];
            try (final RowSequence rs = rb.getRowSequenceByPosition(q[0], q[1])) {
                assertArrayEquals(m, slice(model, q[0], q[1]), keysOf(rs));
            }
        }

        // Start position at or past cardinality yields the empty sequence.
        try (final RowSequence rs = rb.getRowSequenceByPosition(card, 10)) {
            assertTrue(rs.isEmpty());
        }
        try (final RowSequence rs = rb.getRowSequenceByPosition(card + 5, 10)) {
            assertTrue(rs.isEmpty());
        }
    }

    @Test
    public void testGetRowSequenceByPositionSmallNoAcc() {
        // A small (<= 8 spans) bitmap keeps acc == null even after finishMutations.
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addUnsafe(10);
        rb = rb.addUnsafe(BLK + 5);
        rb = rb.addUnsafe(BLK + 6);
        rb = rb.addUnsafe(3 * BLK + 2);

        // Before finishing mutations the cardinality is not cached; a start position past the actual cardinality
        // must walk all spans and return the empty sequence.
        try (final RowSequence rs = rb.getRowSequenceByPosition(100, 5)) {
            assertTrue(rs.isEmpty());
        }
        // A query landing at the very last position while the cardinality cache is dirty.
        try (final RowSequence rs = rb.getRowSequenceByPosition(3, 100)) {
            assertArrayEquals(new long[] {3 * BLK + 2}, keysOf(rs));
        }

        rb.finishMutations();
        assertEquals(4, rb.getCardinality());
        try (final RowSequence rs = rb.getRowSequenceByPosition(100, 5)) {
            assertTrue(rs.isEmpty());
        }
        try (final RowSequence rs = rb.getRowSequenceByPosition(1, 2)) {
            assertArrayEquals(new long[] {BLK + 5, BLK + 6}, keysOf(rs));
        }
    }

    @Test
    public void testGetKeysForPositions() {
        final LongBuf modelBuf = new LongBuf();
        final RspBitmap rb = buildMultiSpan(modelBuf);
        final long[] model = modelBuf.done();
        final long card = rb.getCardinality();

        // A negative position fills NULL_ROW_KEY (-1) for it and everything after it.
        assertArrayEquals(
                new long[] {-1, -1, -1},
                getKeys(rb, -1, 5, 10));

        // Valid positions resolve through the acc-based rank lookup.
        assertArrayEquals(
                new long[] {model[0], model[(int) (card / 2)], model[(int) (card - 1)]},
                getKeys(rb, 0, card / 2, card - 1));

        // Positions at or past cardinality fill NULL_ROW_KEY for the position and everything after it.
        assertArrayEquals(
                new long[] {model[2], -1, -1},
                getKeys(rb, 2, card, card + 5));

        // Small bitmap with a dirty cardinality cache: the no-acc lookup walks off the end of the spans array.
        RspBitmap small = RspBitmap.makeEmpty();
        small = small.addUnsafe(4);
        small = small.addUnsafe(2 * BLK + 1);
        assertArrayEquals(
                new long[] {4, 2 * BLK + 1, -1, -1},
                getKeys(small, 0, 1, 50, 60));
    }

    private static long[] getKeys(final RspBitmap rb, final long... poss) {
        final long[] out = new long[poss.length];
        final int[] i = {0};
        final LongConsumer c = v -> out[i[0]++] = v;
        rb.getKeysForPositions(positions(poss), c);
        assertEquals(poss.length, i[0]);
        return out;
    }

    @Test
    public void testFullBlockSpanRightMerge() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(5 * BLK, 7 * BLK - 1); // full block span over blocks 5-6.
        rb = rb.addRange(4 * BLK, 4 * BLK + 99); // partial block 4.
        assertEquals(2, rb.size);
        // Completing block 4 creates a new full block span that must right-merge with the existing one.
        rb = rb.addRange(4 * BLK + 100, 5 * BLK - 1);
        assertEquals(1, rb.size);
        assertEquals(3 * BLK, rb.getCardinality());
        assertTrue(rb.containsRange(4 * BLK, 7 * BLK - 1));
        rb.validate("testFullBlockSpanRightMerge");
    }

    @Test
    public void testFullBlockSpanLeftMerge() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(4 * BLK, 6 * BLK - 1); // full block span over blocks 4-5.
        rb = rb.addRange(6 * BLK, 6 * BLK + 99); // partial block 6.
        assertEquals(2, rb.size);
        // Completing block 6 must merge left into the existing full block span.
        rb = rb.addRange(6 * BLK + 100, 7 * BLK - 1);
        assertEquals(1, rb.size);
        assertEquals(3 * BLK, rb.getCardinality());
        assertTrue(rb.containsRange(4 * BLK, 7 * BLK - 1));
        rb.validate("testFullBlockSpanLeftMerge");
    }

    @Test
    public void testFullBlockSpanMergeBothSides() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(4 * BLK, 6 * BLK - 1); // full blocks 4-5.
        rb = rb.addRange(7 * BLK, 9 * BLK - 1); // full blocks 7-8.
        assertEquals(2, rb.size);
        // Filling block 6 merges everything into a single full block span over blocks 4-8.
        rb = rb.addRange(6 * BLK, 7 * BLK - 1);
        assertEquals(1, rb.size);
        assertEquals(5 * BLK, rb.getCardinality());
        assertTrue(rb.containsRange(4 * BLK, 9 * BLK - 1));
        rb.validate("testFullBlockSpanMergeBothSides");
    }

    @Test
    public void testRemoveRangeAtBlockBoundaryInFullSpan() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(10 * BLK, 15 * BLK - 1); // full blocks 10-14.
        assertEquals(1, rb.size);
        // Remove a small range starting exactly at a block boundary in the middle of the span.
        rb = rb.removeRange(12 * BLK, 12 * BLK + 9);
        assertEquals(5 * BLK - 10, rb.getCardinality());
        assertTrue(rb.containsRange(10 * BLK, 12 * BLK - 1));
        assertFalse(rb.contains(12 * BLK));
        assertFalse(rb.contains(12 * BLK + 9));
        assertFalse(rb.containsRange(12 * BLK, 12 * BLK + 9));
        assertTrue(rb.containsRange(12 * BLK + 10, 15 * BLK - 1));
        rb.validate("testRemoveRangeAtBlockBoundaryInFullSpan");

        // Remove whole blocks starting exactly at a block boundary.
        RspBitmap rb2 = RspBitmap.makeEmpty();
        rb2 = rb2.addRange(10 * BLK, 15 * BLK - 1);
        rb2 = rb2.removeRange(11 * BLK, 13 * BLK - 1);
        assertEquals(3 * BLK, rb2.getCardinality());
        assertTrue(rb2.containsRange(10 * BLK, 11 * BLK - 1));
        assertFalse(rb2.overlapsRange(11 * BLK, 13 * BLK - 1));
        assertTrue(rb2.containsRange(13 * BLK, 15 * BLK - 1));
        rb2.validate("testRemoveRangeAtBlockBoundaryInFullSpan-wholeBlocks");
    }

    private static RspBitmap build40SpanBitmap() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int b = 0; b < 40; ++b) {
            final long base = b * BLK;
            rb = rb.addUnsafe(base + 1);
            rb = rb.addUnsafe(base + 3);
        }
        return rb;
    }

    @Test
    public void testRemoveCollapsingManySpansWithAcc() {
        RspBitmap rb = build40SpanBitmap();
        rb.finishMutations();
        assertEquals(40, rb.size);
        assertEquals(80, rb.getCardinality()); // ensures the cardinality (and acc) cache is populated.
        // Remove the spans for blocks 5..34 (30 of 40 spans); 10 spans survive, so acc stays populated.
        rb = rb.removeRange(5 * BLK, 35 * BLK - 1);
        assertEquals(10, rb.size);
        assertEquals(20, rb.getCardinality());
        for (int b = 0; b < 40; ++b) {
            final long base = b * BLK;
            final boolean expected = b < 5 || b >= 35;
            assertEquals("block " + b, expected, rb.contains(base + 1));
            assertEquals("block " + b, expected, rb.contains(base + 3));
        }
        rb.validate("testRemoveCollapsingManySpansWithAcc");
    }

    @Test
    public void testRemoveCollapsingManySpansNoAcc() {
        // Same collapse but with a dirty cardinality cache (no acc), covering the acc-null shrink branch.
        RspBitmap rb = build40SpanBitmap();
        rb = rb.removeRangeUnsafe(3 * BLK, 38 * BLK - 1); // removes blocks 3..37 -> 5 spans left.
        rb.finishMutations();
        assertEquals(5, rb.size);
        assertEquals(10, rb.getCardinality());
        for (int b = 0; b < 40; ++b) {
            final boolean expected = b < 3 || b >= 38;
            assertEquals("block " + b, expected, rb.contains(b * BLK + 1));
        }
        rb.validate("testRemoveCollapsingManySpansNoAcc");
    }

    @Test
    public void testSubsetOfFalseBranches() {
        final RspBitmap empty = RspBitmap.makeEmpty();
        empty.finishMutations();

        RspBitmap fullLonger = RspBitmap.makeEmpty();
        fullLonger = fullLonger.addRange(4 * BLK, 7 * BLK - 1); // full blocks 4-6.
        RspBitmap fullShorter = RspBitmap.makeEmpty();
        fullShorter = fullShorter.addRange(4 * BLK, 6 * BLK - 1); // full blocks 4-5.

        // Receiver's full block span extends past the argument's.
        assertFalse(fullLonger.subsetOf(fullShorter));
        assertTrue(fullShorter.subsetOf(fullLonger));

        // Empty receiver / empty argument fast paths.
        assertTrue(empty.subsetOf(fullLonger));
        assertTrue(empty.subsetOf(empty));
        assertFalse(fullLonger.subsetOf(empty));

        // Receiver has a full block span where the argument only has a container.
        RspBitmap container = RspBitmap.makeEmpty();
        container = container.addRange(4 * BLK, 4 * BLK + 10);
        container = container.addRange(5 * BLK, 7 * BLK - 1);
        assertFalse(fullShorter.subsetOf(container));
    }

    @Test
    public void testContainsRangeAcrossMissingBlock() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(3 * BLK, 4 * BLK - 1); // full block 3.
        rb = rb.addRange(5 * BLK, 6 * BLK - 1); // full block 5; block 4 missing.
        assertFalse(rb.containsRange(3 * BLK, 6 * BLK - 1));
        assertFalse(rb.containsRange(3 * BLK + 10, 5 * BLK + 10));
        assertTrue(rb.containsRange(3 * BLK, 4 * BLK - 1));
        assertTrue(rb.containsRange(5 * BLK, 6 * BLK - 1));
        assertFalse(rb.containsRange(4 * BLK, 4 * BLK + 1));
    }

    @Test
    public void testOverlapsRangeAgainstSingleton() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(10 * BLK + 7);
        assertFalse(rb.overlapsRange(10 * BLK + 8, 11 * BLK));
        assertFalse(rb.overlapsRange(0, 10 * BLK + 6));
        assertTrue(rb.overlapsRange(10 * BLK, 10 * BLK + 7));
        assertTrue(rb.overlapsRange(10 * BLK + 7, 10 * BLK + 7));
    }

    @Test
    public void testRetainSingletonDemotion() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(BLK + 1);
        rb = rb.add(BLK + 10);
        rb = rb.add(BLK + 20);
        rb = rb.add(9 * BLK + 5);

        RspBitmap other = RspBitmap.makeEmpty();
        other = other.add(BLK + 10);
        other = other.add(BLK + 30);
        other = other.add(9 * BLK + 5);

        // The intersection leaves exactly one shared value in the first block, demoting the container span
        // to a singleton span.
        rb = rb.andEquals(other);
        assertEquals(2, rb.getCardinality());
        assertTrue(rb.contains(BLK + 10));
        assertTrue(rb.contains(9 * BLK + 5));
        assertFalse(rb.contains(BLK + 1));
        assertFalse(rb.contains(BLK + 20));
        rb.validate("testRetainSingletonDemotion");
    }

    @Test
    public void testRowSequenceViewByKeyRange() {
        final LongBuf modelBuf = new LongBuf();
        final RspBitmap rb = buildMultiSpan(modelBuf);
        final long[] model = modelBuf.done();
        final long card = rb.getCardinality();

        // Pick a view that starts and ends strictly inside spans; the full block spans in the model guarantee
        // there is plenty of room on both sides.
        final long viewStart = 20;
        final long viewLen = card - 40;
        try (final RowSequence view = rb.getRowSequenceByPosition(viewStart, viewLen)) {
            assertTrue(view instanceof RspRowSequence);
            final long[] viewModel = slice(model, viewStart, viewLen);
            assertEquals(viewModel[0], view.firstRowKey());
            assertEquals(viewModel[viewModel.length - 1], view.lastRowKey());

            // Key range wider than the view on both sides: the view boundary must bind at both ends.
            try (final RowSequence sub = view.getRowSequenceByKeyRange(
                    view.firstRowKey() - 3, view.lastRowKey() + 3)) {
                assertArrayEquals(viewModel, keysOf(sub));
            }
            // An unbounded key range: the whole view qualifies, even though the view starts inside a full
            // block span and key 0 falls in the gap before it.
            try (final RowSequence sub = view.getRowSequenceByKeyRange(0, Long.MAX_VALUE)) {
                assertArrayEquals(viewModel, keysOf(sub));
            }
            // Interior key range: the requested keys bind on both sides instead.
            final long a = viewModel[viewModel.length / 4];
            final long b = viewModel[3 * viewModel.length / 4];
            try (final RowSequence sub = view.getRowSequenceByKeyRange(a, b)) {
                final long[] expected = Arrays.stream(viewModel).filter(v -> a <= v && v <= b).toArray();
                assertArrayEquals(expected, keysOf(sub));
            }
            // One side binding only.
            try (final RowSequence sub = view.getRowSequenceByKeyRange(a, view.lastRowKey() + 100)) {
                final long[] expected = Arrays.stream(viewModel).filter(v -> v >= a).toArray();
                assertArrayEquals(expected, keysOf(sub));
            }
        }
    }

    /**
     * A requested start key may fall in a gap that precedes a full block span, in which case {@code findInSpan} must
     * report an insertion point at that span rather than a position derived from {@code startValue - spanKey}, which is
     * negative for such a key.
     */
    @Test
    public void testGetRowSequenceByKeyRangeStartKeyBeforeFullBlockSpan() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRangeUnsafe(5 * BLK, 6 * BLK - 1); // full block span, blocks 5-5.
        rb = rb.addUnsafe(9 * BLK + 3);
        rb.finishMutations();

        // Working cases: the start key is at or inside the full block span's first block.
        try (final RowSequence rs = rb.getRowSequenceByKeyRange(5 * BLK, Long.MAX_VALUE)) {
            assertEquals(5 * BLK, rs.firstRowKey());
            assertEquals(9 * BLK + 3, rs.lastRowKey());
            assertEquals(BLK + 1, rs.size());
        }
        try (final RowSequence rs = rb.getRowSequenceByKeyRange(5 * BLK + 10, Long.MAX_VALUE)) {
            assertEquals(5 * BLK + 10, rs.firstRowKey());
            assertEquals(BLK + 1 - 10, rs.size());
        }

        // A start key below the full block span's key finds the whole span.
        try (final RowSequence rs = rb.getRowSequenceByKeyRange(0, Long.MAX_VALUE)) {
            assertEquals(5 * BLK, rs.firstRowKey());
            assertEquals(BLK + 1, rs.size());
        }
        // The same shape narrowed to the full block span.
        try (final RowSequence rs = rb.getRowSequenceByKeyRange(0, 6 * BLK - 1)) {
            assertEquals(5 * BLK, rs.firstRowKey());
            assertEquals(BLK, rs.size());
        }

        RspBitmap rb2 = RspBitmap.makeEmpty();
        rb2 = rb2.addUnsafe(BLK + 1); // singleton span first...
        rb2 = rb2.addRangeUnsafe(5 * BLK, 6 * BLK - 1); // ... then a full block span, with a gap in between.
        rb2.finishMutations();
        // A container-backed first span behaves correctly for the same query shape.
        try (final RowSequence rs = rb2.getRowSequenceByKeyRange(0, Long.MAX_VALUE)) {
            assertEquals(BLK + 1, rs.firstRowKey());
            assertEquals(BLK + 1, rs.size());
        }
        // Starting from a key inside the gap between the two spans.
        try (final RowSequence rs = rb2.getRowSequenceByKeyRange(3 * BLK, Long.MAX_VALUE)) {
            assertEquals(5 * BLK, rs.firstRowKey());
            assertEquals(BLK, rs.size());
        }
    }

    @Test
    public void testRowSequenceViewByPosition() {
        final LongBuf modelBuf = new LongBuf();
        final RspBitmap rb = buildMultiSpan(modelBuf);
        final long[] model = modelBuf.done();
        final long card = rb.getCardinality();

        final long viewStart = 15;
        final long viewLen = card - 30;
        try (final RowSequence view = rb.getRowSequenceByPosition(viewStart, viewLen)) {
            final long[] viewModel = slice(model, viewStart, viewLen);
            // Interior sub-slice.
            try (final RowSequence sub = view.getRowSequenceByPosition(7, 66000)) {
                assertArrayEquals(slice(viewModel, 7, 66000), keysOf(sub));
            }
            // Length past the view's end must be truncated to the view.
            try (final RowSequence sub = view.getRowSequenceByPosition(viewLen - 5, 1000)) {
                assertArrayEquals(slice(viewModel, viewLen - 5, 1000), keysOf(sub));
            }
            // Start past the view's end.
            try (final RowSequence sub = view.getRowSequenceByPosition(viewLen + 10, 5)) {
                assertTrue(sub.isEmpty());
            }
        }
    }

    @Test
    public void testMultiSpanViewForEach() {
        final LongBuf modelBuf = new LongBuf();
        final RspBitmap rb = buildMultiSpan(modelBuf);
        final long[] model = modelBuf.done();
        final long card = rb.getCardinality();

        // A view crossing many (>= 4) spans, starting and ending mid-span.
        final long viewStart = 10;
        final long viewLen = card - 20;
        try (final RowSequence view = rb.getRowSequenceByPosition(viewStart, viewLen)) {
            final long[] viewModel = slice(model, viewStart, viewLen);
            assertArrayEquals(viewModel, keysOf(view));
            assertArrayEquals(viewModel, keysOfByRanges(view));

            // Abort semantics on the multi-span view: exactly k callbacks happen for early aborts.
            for (int k = 1; k <= 40; ++k) {
                final int kf = k;
                final int[] count = {0};
                final boolean completed = view.forEachRowKey(v -> {
                    assertEquals(viewModel[count[0]], v);
                    return ++count[0] < kf;
                });
                assertFalse(completed);
                assertEquals(kf, count[0]);
            }
            // Abort in a later span (inside / after the full block spans).
            for (final int k : new int[] {(int) viewLen / 2, (int) viewLen - 1}) {
                final int[] count = {0};
                final boolean completed = view.forEachRowKey(v -> ++count[0] < k);
                assertFalse(completed);
                assertEquals(k, count[0]);
            }
            // Range-wise abort.
            final int totalRanges = keyRangesCount(view);
            for (int k = 1; k <= totalRanges; ++k) {
                final int kf = k;
                final int[] count = {0};
                final boolean completed = view.forEachRowKeyRange((s, e) -> ++count[0] < kf);
                assertFalse(completed);
                assertEquals(kf, count[0]);
            }
        }
    }

    private static int keyRangesCount(final RowSequence rs) {
        final int[] count = {0};
        rs.forEachRowKeyRange((s, e) -> {
            ++count[0];
            return true;
        });
        return count[0];
    }
}
