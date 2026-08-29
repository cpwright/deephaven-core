//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import org.junit.Test;

import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.TreeSet;
import java.util.function.LongConsumer;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Coverage-focused tests for the paths that reshape the {@code RspArray} spans arrays while keeping (part of) the
 * cardinality accumulator cache ("acc"): the {@code orEqualsShifted} reallocation that copies the surviving acc prefix,
 * and the {@code collapseRange} shrink that reallocates to half the capacity and keeps the acc prefix. Both fail
 * silently if the prefix is copied wrongly: {@code get(pos)} and {@code find(key)} return wrong answers with no
 * exception, so every test here checks positions against an independent model.
 * <p>
 * Also covers position lookups performed while the cardinality cache is dirty (unsafe mutations not yet finished).
 */
public class RspArrayCardinalityCacheTest {

    private static final long BLK = BLOCK_SIZE;

    /**
     * An independent model of a sorted key set kept as coalesced ranges, so that tests can cover multi-million-key
     * bitmaps without materializing every key. Ranges must be added in increasing order.
     */
    private static final class RangeModel {
        private long[] starts = new long[8];
        private long[] ends = new long[8];
        private int n;
        private long[] prevCard;
        private long card = -1;

        void add(final long v) {
            addRange(v, v);
        }

        void addRange(final long first, final long last) {
            if (n > 0) {
                if (first < starts[n - 1]) {
                    throw new IllegalArgumentException("ranges must be added in increasing order");
                }
                if (first <= ends[n - 1] + 1) {
                    ends[n - 1] = Math.max(ends[n - 1], last);
                    return;
                }
            }
            if (n == starts.length) {
                starts = Arrays.copyOf(starts, 2 * n);
                ends = Arrays.copyOf(ends, 2 * n);
            }
            starts[n] = first;
            ends[n] = last;
            ++n;
        }

        RangeModel seal() {
            prevCard = new long[n];
            long c = 0;
            for (int i = 0; i < n; ++i) {
                prevCard[i] = c;
                c += ends[i] - starts[i] + 1;
            }
            card = c;
            return this;
        }

        long cardinality() {
            return card;
        }

        /** The key at position {@code pos}, or -1 if the position is out of range. */
        long get(final long pos) {
            if (pos < 0 || pos >= card) {
                return -1;
            }
            int lo = 0;
            int hi = n - 1;
            while (lo < hi) {
                final int mid = (lo + hi + 1) >>> 1;
                if (prevCard[mid] <= pos) {
                    lo = mid;
                } else {
                    hi = mid - 1;
                }
            }
            return starts[lo] + (pos - prevCard[lo]);
        }

        private int rangeForKey(final long key) {
            int lo = 0;
            int hi = n - 1;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                if (key < starts[mid]) {
                    hi = mid - 1;
                } else if (key > ends[mid]) {
                    lo = mid + 1;
                } else {
                    return mid;
                }
            }
            return -1;
        }

        boolean contains(final long key) {
            return rangeForKey(key) >= 0;
        }

        /** The position of {@code key}, or -1 if absent. */
        long find(final long key) {
            final int i = rangeForKey(key);
            return (i < 0) ? -1 : prevCard[i] + key - starts[i];
        }
    }

    private static long[] getKeysForPositions(final RspBitmap rb, final long... poss) {
        final long[] out = new long[poss.length];
        final int[] i = {0};
        final LongConsumer c = v -> out[i[0]++] = v;
        final PrimitiveIterator.OfLong it = Arrays.stream(poss).iterator();
        rb.getKeysForPositions(it, c);
        assertEquals(poss.length, i[0]);
        return out;
    }

    /**
     * {@code orEqualsShifted} with interior insertions that do not fit in the current capacity: the spans arrays are
     * reallocated ({@code !inPlace}) and the acc prefix that survives the first insertion point is copied over with
     * {@code System.arraycopy}.
     * <p>
     * The shape matters: 12 spans give a capacity of 16 and (12 &gt; accNullThreshold) an allocated acc; adding 5 spans
     * at interior keys makes newSize=17 exceed the capacity, and the first insertion lands at index 2 so that a
     * non-empty prefix is actually copied.
     */
    @Test
    public void testOrEqualsReallocCopiesAccPrefix() {
        final TreeSet<Long> model = new TreeSet<>();
        RspBitmap a = RspBitmap.makeEmpty();
        for (int b = 0; b < 12; ++b) {
            final long v = (2L * b) * BLK + 3; // even block keys.
            a = a.appendUnsafe(v);
            model.add(v);
        }
        a.finishMutations();
        assertEquals(12, a.size);
        assertEquals("capacity", 16, a.spans.length);
        assertNotNull("acc must be populated for the prefix copy to happen", a.acc);
        assertTrue(a.isCardinalityCached());
        assertEquals(12, a.getCardinality());

        RspBitmap b2 = RspBitmap.makeEmpty();
        for (int i = 0; i < 5; ++i) {
            final long v = (2L * i + 3) * BLK + 7; // interior odd block keys: 3, 5, 7, 9, 11.
            b2 = b2.appendUnsafe(v);
            model.add(v);
        }
        b2.finishMutations();
        assertEquals(5, b2.size);
        assertTrue("newSize must exceed the capacity to force a realloc", a.size + b2.size > a.spans.length);

        a = a.orEquals(b2);

        assertEquals(17, a.size);
        assertNotNull(a.acc);
        assertEquals(model.size(), a.getCardinality());
        a.validate("testOrEqualsReallocCopiesAccPrefix");

        final long[] expected = model.stream().mapToLong(Long::longValue).toArray();
        for (int i = 0; i < expected.length; ++i) {
            assertEquals("pos " + i, expected[i], a.get(i));
            assertEquals("key " + expected[i], i, a.find(expected[i]));
        }
        assertEquals(-1, a.get(expected.length));
        assertEquals(-1, a.get(expected.length + 5));
        assertEquals(-1, a.get(-1));
        assertArrayEqualsLong(expected, getKeysForPositions(a, positionsUpTo(expected.length)));

        // Absent keys in blocks that are present, on both sides of the stored value.
        assertTrue(a.find(2) < 0);
        assertTrue(a.find(4) < 0);
        assertTrue(a.find(3 * BLK + 6) < 0);
        assertTrue(a.find(3 * BLK + 8) < 0);
        for (final long v : expected) {
            assertTrue("contains " + v, a.contains(v));
        }
    }

    /**
     * The same shape, but with the insertions shifted so that the surviving prefix is empty (the first insertion lands
     * at index 0): the acc prefix copy is skipped and the new acc is filled entirely from scratch.
     */
    @Test
    public void testOrEqualsReallocWithEmptyAccPrefix() {
        final TreeSet<Long> model = new TreeSet<>();
        RspBitmap a = RspBitmap.makeEmpty();
        for (int b = 0; b < 12; ++b) {
            final long v = (2L * b + 2) * BLK + 3; // even block keys starting at block 2.
            a = a.appendUnsafe(v);
            model.add(v);
        }
        a.finishMutations();
        assertEquals(12, a.size);
        assertEquals(16, a.spans.length);
        assertNotNull(a.acc);
        assertEquals(12, a.getCardinality());

        RspBitmap b2 = RspBitmap.makeEmpty();
        for (int i = 0; i < 5; ++i) {
            final long v = (2L * i + 1) * BLK + 7; // odd block keys 1, 3, 5, 7, 9; the first one is before all of a's.
            b2 = b2.appendUnsafe(v);
            model.add(v);
        }
        b2.finishMutations();

        a = a.orEquals(b2);
        assertEquals(17, a.size);
        assertEquals(model.size(), a.getCardinality());
        a.validate("testOrEqualsReallocWithEmptyAccPrefix");

        final long[] expected = model.stream().mapToLong(Long::longValue).toArray();
        for (int i = 0; i < expected.length; ++i) {
            assertEquals("pos " + i, expected[i], a.get(i));
            assertEquals("key " + expected[i], i, a.find(expected[i]));
        }
        assertArrayEqualsLong(expected, getKeysForPositions(a, positionsUpTo(expected.length)));
    }

    /**
     * {@code collapseRange} shrinking enough to reallocate to half of the current capacity while keeping the acc array
     * (only its prefix up to the collapse point is still meaningful).
     * <p>
     * 1025 single-value spans give a capacity of 2048; a single {@code addRange} then collapses 1006 of them into one
     * full block span, leaving 19 spans, which is both greater than 2*INITIAL_CAPACITY and less than half the capacity.
     * The surviving size stays above accNullThreshold so that the reads afterwards go through acc.
     */
    @Test
    public void testCollapseRangeShrinkKeepsAccPrefix() {
        final int nBlocks = 1025;
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < nBlocks; ++i) {
            rb = rb.appendUnsafe(i * BLK + 1);
        }
        rb.finishMutations();
        assertEquals(nBlocks, rb.size);
        assertEquals("capacity", 2048, rb.spans.length);
        assertNotNull("acc must be populated for the shrink to keep it", rb.acc);
        assertEquals(nBlocks, rb.getCardinality());

        // Collapse blocks 3..1009 into a single full block span: 1007 spans become 1, so size goes 1025 -> 19,
        // which is < spans.length / 2 == 1024, taking the shrink-and-keep-acc path.
        // Use the unsafe variant so that the post-collapse state is observable before finishMutations().
        rb = rb.addRangeUnsafe(3 * BLK, 1010 * BLK - 1);
        assertEquals(19, rb.size);
        assertEquals("capacity after shrink", 1024, rb.spans.length);
        assertNotNull("acc must survive the shrink", rb.acc);
        rb.finishMutations();
        assertNotNull("acc must still be populated for a 19 span array", rb.acc);
        assertTrue(rb.isCardinalityCached());

        final RangeModel model = new RangeModel();
        model.add(1);
        model.add(BLK + 1);
        model.add(2 * BLK + 1);
        model.addRange(3 * BLK, 1010 * BLK - 1);
        for (int i = 1010; i < nBlocks; ++i) {
            model.add(i * BLK + 1);
        }
        model.seal();

        final long card = model.cardinality();
        assertEquals(3 + 1007L * BLK + 15, card);
        assertEquals(card, rb.getCardinality());
        rb.validate("testCollapseRangeShrinkKeepsAccPrefix");

        // Positions of interest: the copied acc prefix (the three leading single-value spans), both edges of the full
        // block span, and each of the trailing single-value spans.
        final long fullSpanFirstPos = 3;
        final long fullSpanLastPos = 3 + 1007L * BLK - 1;
        for (final long p : new long[] {
                0, 1, 2,
                fullSpanFirstPos, fullSpanFirstPos + 1, fullSpanFirstPos + BLK - 1, fullSpanFirstPos + BLK,
                fullSpanLastPos - 1, fullSpanLastPos, fullSpanLastPos + 1, fullSpanLastPos + 2,
                card - 2, card - 1}) {
            assertEquals("pos " + p, model.get(p), rb.get(p));
            assertEquals("find at pos " + p, p, rb.find(model.get(p)));
        }
        for (long p = card - 16; p < card; ++p) {
            assertEquals("pos " + p, model.get(p), rb.get(p));
        }
        // A strided sweep through the full block span.
        for (long p = 0; p < card; p += 1_000_003) {
            assertEquals("pos " + p, model.get(p), rb.get(p));
            assertEquals("find at pos " + p, p, rb.find(model.get(p)));
        }
        // Out of range positions.
        assertEquals(-1, rb.get(card));
        assertEquals(-1, rb.get(card + 1000));
        assertEquals(-1, rb.get(-1));

        // getKeysForPositions over the same shape, through the acc based rank lookup.
        final long[] poss = {0, 1, 2, fullSpanFirstPos, fullSpanLastPos, fullSpanLastPos + 1, card - 1};
        final long[] expected = new long[poss.length];
        for (int i = 0; i < poss.length; ++i) {
            expected[i] = model.get(poss[i]);
        }
        assertArrayEqualsLong(expected, getKeysForPositions(rb, poss));

        // Containment spot checks on both sides of every boundary.
        for (final long k : new long[] {
                1, BLK + 1, 2 * BLK + 1,
                3 * BLK, 3 * BLK + 1, 500 * BLK, 500 * BLK + 12345, 1010 * BLK - 1,
                1010 * BLK + 1, 1024 * BLK + 1}) {
            assertTrue("contains " + k, rb.contains(k));
            assertEquals(model.contains(k), rb.contains(k));
        }
        for (final long k : new long[] {
                0, 2, BLK, BLK + 2, 2 * BLK, 2 * BLK + 2,
                3 * BLK - 1, 1010 * BLK, 1010 * BLK + 2, 1024 * BLK, 1025 * BLK + 1}) {
            assertFalse("contains " + k, rb.contains(k));
            assertEquals(model.contains(k), rb.contains(k));
            assertTrue("find " + k, rb.find(k) < 0);
        }
        assertTrue(rb.containsRange(3 * BLK, 1010 * BLK - 1));
        assertFalse(rb.containsRange(2 * BLK + 1, 3 * BLK));
        assertFalse(rb.containsRange(1010 * BLK - 1, 1010 * BLK));
    }

    /**
     * {@code get(pos)} while the cardinality cache is dirty but acc is allocated (unsafe mutations after a previous
     * {@code finishMutations()}). This has to fall back to the accumulator-free rank walk, since acc only reflects the
     * spans before the first modified one.
     */
    @Test
    public void testGetWithDirtyCardinalityCacheAndAllocatedAcc() {
        final TreeSet<Long> model = new TreeSet<>();
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int b = 0; b < 12; ++b) {
            final long v = (2L * b) * BLK + 3;
            rb = rb.appendUnsafe(v);
            model.add(v);
        }
        rb.finishMutations();
        assertNotNull(rb.acc);
        assertEquals(12, rb.getCardinality());

        // Unsafe insertions in the middle (and at the end), without finishing mutations.
        for (int b = 0; b < 12; b += 2) {
            final long v = (2L * b + 1) * BLK + 5;
            rb = rb.addUnsafe(v);
            model.add(v);
        }
        rb = rb.addRangeUnsafe(30 * BLK, 30 * BLK + 9);
        for (long v = 30 * BLK; v <= 30 * BLK + 9; ++v) {
            model.add(v);
        }
        assertNotNull("acc is still allocated", rb.acc);
        assertFalse("the cardinality cache must be dirty", rb.isCardinalityCached());

        final long[] expected = model.stream().mapToLong(Long::longValue).toArray();
        for (int i = 0; i < expected.length; ++i) {
            assertEquals("pos " + i, expected[i], rb.get(i));
        }
        // At and past the cardinality, with the cache dirty, the rank walk runs off the end of the spans array.
        assertEquals(-1, rb.get(expected.length));
        assertEquals(-1, rb.get(expected.length + 1));
        assertEquals(-1, rb.get(expected.length + 1000));
        assertEquals(-1, rb.get(-1));

        // Everything still lines up once mutations are finished.
        rb.finishMutations();
        assertEquals(expected.length, rb.getCardinality());
        rb.validate("testGetWithDirtyCardinalityCacheAndAllocatedAcc");
        for (int i = 0; i < expected.length; ++i) {
            assertEquals("pos " + i, expected[i], rb.get(i));
            assertEquals("key " + expected[i], i, rb.find(expected[i]));
        }
        assertArrayEqualsLong(expected, getKeysForPositions(rb, positionsUpTo(expected.length)));
    }

    /**
     * {@code get(pos)} and {@code getKeysForPositions} on a many-span bitmap whose cardinality cache was never
     * populated (acc == null, cardData dirty), including positions at and past the cardinality, which must yield
     * {@code RowSequence.NULL_ROW_KEY} for that position and every position after it.
     */
    @Test
    public void testPositionLookupsWithNoAccAndDirtyCache() {
        final TreeSet<Long> model = new TreeSet<>();
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int b = 0; b < 12; ++b) {
            final long base = (2L * b) * BLK;
            rb = rb.addUnsafe(base + 3);
            rb = rb.addRangeUnsafe(base + 100, base + 104);
            model.add(base + 3);
            for (long v = base + 100; v <= base + 104; ++v) {
                model.add(v);
            }
        }
        assertEquals(12, rb.size);
        assertNull("acc is never allocated without finishMutations()", rb.acc);
        assertFalse(rb.isCardinalityCached());

        final long[] expected = model.stream().mapToLong(Long::longValue).toArray();
        final int card = expected.length;
        for (int i = 0; i < card; ++i) {
            assertEquals("pos " + i, expected[i], rb.get(i));
        }
        assertEquals(-1, rb.get(card));
        assertEquals(-1, rb.get(card + 7));

        // The -1 fill is sticky: once a position is out of range, every remaining position gets -1 too.
        assertArrayEqualsLong(
                new long[] {expected[0], expected[card - 1], -1, -1},
                getKeysForPositions(rb, 0, card - 1, card, card + 5));
        assertArrayEqualsLong(
                new long[] {-1, -1},
                getKeysForPositions(rb, card + 1, card + 2));
        assertArrayEqualsLong(expected, getKeysForPositions(rb, positionsUpTo(card)));

        rb.finishMutations();
        assertNotNull(rb.acc);
        assertEquals(card, rb.getCardinality());
        rb.validate("testPositionLookupsWithNoAccAndDirtyCache");
        assertArrayEqualsLong(expected, getKeysForPositions(rb, positionsUpTo(card)));
    }

    private static long[] positionsUpTo(final int n) {
        final long[] poss = new long[n];
        for (int i = 0; i < n; ++i) {
            poss[i] = i;
        }
        return poss;
    }

    private static void assertArrayEqualsLong(final long[] expected, final long[] actual) {
        assertEquals("length", expected.length, actual.length);
        for (int i = 0; i < expected.length; ++i) {
            assertEquals("element " + i, expected[i], actual[i]);
        }
    }
}
