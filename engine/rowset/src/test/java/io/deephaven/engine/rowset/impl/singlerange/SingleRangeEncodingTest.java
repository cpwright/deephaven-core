//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.singlerange;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Round-trip coverage for {@link SingleRange#make(long, long)}'s bit-packing selection ladder.
 *
 * <p>
 * {@code make} picks among seven concrete representations depending on whether the range's start and its delta
 * ({@code end - start}) fit in an unsigned short, an unsigned int, or need a full long. Picking the wrong variant would
 * silently truncate keys, so every combination below is round-tripped through the whole {@code OrderedLongSet} key/
 * position API, and the selected class is pinned explicitly.
 */
public class SingleRangeEncodingTest {

    private static final long MAX_UNSIGNED_SHORT = 0xFFFFL;
    private static final long MAX_UNSIGNED_INT = 0xFFFF_FFFFL;

    private static final long[] STARTS = {
            0L,
            1L,
            1L << 15,
            (1L << 15) + 1,
            MAX_UNSIGNED_SHORT,
            MAX_UNSIGNED_SHORT + 1,
            1L << 31,
            (1L << 31) + 1,
            MAX_UNSIGNED_INT,
            MAX_UNSIGNED_INT + 1,
            1L << 40,
            Long.MAX_VALUE - 10,
    };

    private static final long[] DELTAS = {
            0L,
            1L,
            1L << 15,
            MAX_UNSIGNED_SHORT,
            MAX_UNSIGNED_SHORT + 1,
            1L << 31,
            MAX_UNSIGNED_INT,
            MAX_UNSIGNED_INT + 1,
            1L << 40,
    };

    /**
     * An independent restatement of the selection ladder in {@link SingleRange#make(long, long)}: a single-key range is
     * stored as a bare start; otherwise the start and the delta are each packed as an unsigned short / unsigned int /
     * long, with the trailing case keeping both endpoints as longs.
     */
    private static Class<? extends SingleRange> expectedClass(final long start, final long delta) {
        if (delta == 0) {
            return start <= MAX_UNSIGNED_INT ? SingleIntSingleRange.class : SingleLongSingleRange.class;
        }
        if (start <= MAX_UNSIGNED_SHORT && delta <= MAX_UNSIGNED_SHORT) {
            return ShortStartShortDeltaSingleRange.class;
        }
        if (delta <= MAX_UNSIGNED_INT) {
            return start <= MAX_UNSIGNED_INT
                    ? IntStartIntDeltaSingleRange.class
                    : LongStartIntDeltaSingleRange.class;
        }
        return start <= MAX_UNSIGNED_INT
                ? IntStartLongDeltaSingleRange.class
                : LongStartLongEndSingleRange.class;
    }

    private static void checkOne(final long start, final long delta, final Set<Class<?>> seen) {
        final long end = start + delta;
        final String m = "start=" + start + ", end=" + end + ", delta=" + delta;
        final SingleRange sr = SingleRange.make(start, end);

        assertEquals(m + ": encoding variant", expectedClass(start, delta), sr.getClass());
        seen.add(sr.getClass());

        final long card = delta + 1;
        assertEquals(m + ": rangeStart", start, sr.rangeStart());
        assertEquals(m + ": rangeEnd", end, sr.rangeEnd());
        assertEquals(m + ": getCardinality", card, sr.getCardinality());
        assertEquals(m + ": ixFirstKey", start, sr.ixFirstKey());
        assertEquals(m + ": ixLastKey", end, sr.ixLastKey());
        assertEquals(m + ": ixCardinality", card, sr.ixCardinality());
        assertTrue(m + ": ixContainsRange(start, end)", sr.ixContainsRange(start, end));
        assertEquals(m + ": ixGet(0)", start, sr.ixGet(0));
        assertEquals(m + ": ixGet(card - 1)", end, sr.ixGet(card - 1));
        assertEquals(m + ": ixGet(card)", RowSequence.NULL_ROW_KEY, sr.ixGet(card));
        assertEquals(m + ": ixFind(start)", 0, sr.ixFind(start));
        assertEquals(m + ": ixFind(end)", card - 1, sr.ixFind(end));
        sr.ixValidate();
        sr.ixValidate(m);

        // Interior probes, when the range has one.
        if (card > 2) {
            final long mid = start + delta / 2;
            assertEquals(m + ": ixGet(delta / 2)", mid, sr.ixGet(delta / 2));
            assertEquals(m + ": ixFind(mid)", delta / 2, sr.ixFind(mid));
            assertTrue(m + ": ixContainsRange(mid, end)", sr.ixContainsRange(mid, end));
        }

        // Neighborhood checks that must not be confused by truncation of the packed fields.
        if (start > 0) {
            assertFalse(m + ": ixContainsRange(start - 1, end)", sr.ixContainsRange(start - 1, end));
            assertEquals(m + ": ixFind(start - 1)", ~0L, sr.ixFind(start - 1));
        }
        if (end < Long.MAX_VALUE) {
            assertFalse(m + ": ixContainsRange(start, end + 1)", sr.ixContainsRange(start, end + 1));
            assertEquals(m + ": ixFind(end + 1)", ~card, sr.ixFind(end + 1));
            assertTrue(m + ": ixOverlapsRange(end, end + 1)", sr.ixOverlapsRange(end, end + 1));
        }
        assertFalse(m + ": ixIsEmpty", sr.ixIsEmpty());

        // copy()/ixCowRef() must preserve both the variant and the payload.
        final SingleRange copy = sr.copy();
        assertEquals(m + ": copy() variant", sr.getClass(), copy.getClass());
        assertEquals(m + ": copy() rangeStart", start, copy.rangeStart());
        assertEquals(m + ": copy() rangeEnd", end, copy.rangeEnd());
        final SingleRange cowRef = sr.ixCowRef();
        assertEquals(m + ": ixCowRef() variant", sr.getClass(), cowRef.getClass());
        assertEquals(m + ": ixCowRef() rangeEnd", end, cowRef.rangeEnd());

        // A RowSet wrapping it must agree.
        try (final RowSet rowSet = new WritableRowSetImpl(sr.ixCowRef())) {
            assertEquals(m + ": rowSet size", card, rowSet.size());
            assertEquals(m + ": rowSet firstRowKey", start, rowSet.firstRowKey());
            assertEquals(m + ": rowSet lastRowKey", end, rowSet.lastRowKey());
            assertTrue(m + ": rowSet containsRange", rowSet.containsRange(start, end));
            assertEquals(m + ": rowSet get(0)", start, rowSet.get(0));
            assertEquals(m + ": rowSet get(card - 1)", end, rowSet.get(card - 1));
            assertEquals(m + ": rowSet find(end)", card - 1, rowSet.find(end));
        }
    }

    @Test
    public void testEncodingSelectionRoundTrip() {
        final Set<Class<?>> seen = new HashSet<>();
        int cases = 0;
        for (final long start : STARTS) {
            for (final long delta : DELTAS) {
                if (delta > Long.MAX_VALUE - start) {
                    // Would overflow past Long.MAX_VALUE.
                    continue;
                }
                checkOne(start, delta, seen);
                ++cases;
            }
        }
        assertTrue("expected many cases, got " + cases, cases > 50);

        // Every representation in the ladder must have been constructed at least once.
        final Set<Class<?>> expected = new HashSet<>();
        expected.add(SingleIntSingleRange.class);
        expected.add(SingleLongSingleRange.class);
        expected.add(ShortStartShortDeltaSingleRange.class);
        expected.add(IntStartIntDeltaSingleRange.class);
        expected.add(LongStartIntDeltaSingleRange.class);
        expected.add(IntStartLongDeltaSingleRange.class);
        expected.add(LongStartLongEndSingleRange.class);
        assertEquals("all encoding variants must be exercised", expected, seen);
    }

    /** Representative, explicitly named case per variant: this is what pins the selection thresholds. */
    @Test
    public void testVariantSelectionThresholds() {
        // Single key.
        assertEquals(SingleIntSingleRange.class, SingleRange.make(0, 0).getClass());
        assertEquals(SingleIntSingleRange.class, SingleRange.make(MAX_UNSIGNED_INT, MAX_UNSIGNED_INT).getClass());
        assertEquals(SingleLongSingleRange.class,
                SingleRange.make(MAX_UNSIGNED_INT + 1, MAX_UNSIGNED_INT + 1).getClass());
        assertEquals(SingleLongSingleRange.class, SingleRange.make(Long.MAX_VALUE, Long.MAX_VALUE).getClass());

        // Short start, short delta.
        assertEquals(ShortStartShortDeltaSingleRange.class, SingleRange.make(0, 1).getClass());
        assertEquals(ShortStartShortDeltaSingleRange.class,
                SingleRange.make(MAX_UNSIGNED_SHORT - 1, MAX_UNSIGNED_SHORT).getClass());
        assertEquals(ShortStartShortDeltaSingleRange.class,
                SingleRange.make(0, MAX_UNSIGNED_SHORT).getClass());

        // Int start, int delta: either the start or the delta exceeds an unsigned short.
        assertEquals(IntStartIntDeltaSingleRange.class,
                SingleRange.make(MAX_UNSIGNED_SHORT + 1, MAX_UNSIGNED_SHORT + 2).getClass());
        assertEquals(IntStartIntDeltaSingleRange.class,
                SingleRange.make(0, MAX_UNSIGNED_SHORT + 1).getClass());
        assertEquals(IntStartIntDeltaSingleRange.class,
                SingleRange.make(0, MAX_UNSIGNED_INT).getClass());
        assertEquals(IntStartIntDeltaSingleRange.class,
                SingleRange.make(MAX_UNSIGNED_INT - 1, MAX_UNSIGNED_INT).getClass());

        // Long start, int delta.
        assertEquals(LongStartIntDeltaSingleRange.class,
                SingleRange.make(MAX_UNSIGNED_INT + 1, MAX_UNSIGNED_INT + 2).getClass());
        assertEquals(LongStartIntDeltaSingleRange.class,
                SingleRange.make(1L << 40, (1L << 40) + MAX_UNSIGNED_INT).getClass());

        // Int start, long delta.
        assertEquals(IntStartLongDeltaSingleRange.class,
                SingleRange.make(0, MAX_UNSIGNED_INT + 1).getClass());
        assertEquals(IntStartLongDeltaSingleRange.class,
                SingleRange.make(MAX_UNSIGNED_INT, MAX_UNSIGNED_INT + (1L << 40)).getClass());

        // Long start, long end: neither the start nor the delta fits in an unsigned int.
        assertEquals(LongStartLongEndSingleRange.class,
                SingleRange.make(MAX_UNSIGNED_INT + 1, 2 * MAX_UNSIGNED_INT + 2).getClass());
        assertEquals(LongStartLongEndSingleRange.class,
                SingleRange.make(1L << 40, (1L << 40) + (1L << 40)).getClass());
        assertEquals(LongStartLongEndSingleRange.class,
                SingleRange.make(1L << 62, (1L << 62) + (1L << 40)).getClass());
    }

    /**
     * A long-start/long-delta range must survive the operations that re-enter {@code make} (subsetting, shifting,
     * insertion and removal at the edges) without truncating the high bits of either endpoint.
     */
    @Test
    public void testLongStartLongEndOperations() {
        final long start = 1L << 40;
        final long end = start + (1L << 40);
        final long card = end - start + 1;
        final SingleRange sr = SingleRange.make(start, end);
        assertEquals(LongStartLongEndSingleRange.class, sr.getClass());

        // Subset by key: the surviving sub-range still needs a long start.
        assertEquals(start + 10, sr.ixSubindexByKeyOnNew(start + 10, end).ixFirstKey());
        assertEquals(end, sr.ixSubindexByKeyOnNew(start + 10, end + 100).ixLastKey());
        assertEquals(card, sr.ixSubindexByKeyOnNew(start, end).ixCardinality());

        // Subset by position.
        assertEquals(start + 5, sr.ixSubindexByPosOnNew(5, 10).ixFirstKey());
        assertEquals(start + 9, sr.ixSubindexByPosOnNew(5, 10).ixLastKey());
        assertEquals(card, sr.ixSubindexByPosOnNew(0, card).ixCardinality());

        // Shift.
        assertEquals(start + 3, sr.ixShiftOnNew(3).ixFirstKey());
        assertEquals(end + 3, sr.ixShiftOnNew(3).ixLastKey());

        // Grow / shrink at the edges.
        assertEquals(start - 1, sr.ixInsert(start - 1).ixFirstKey());
        assertEquals(end + 1, sr.ixInsert(end + 1).ixLastKey());
        assertEquals(start + 1, sr.ixRemove(start).ixFirstKey());
        assertEquals(end - 1, sr.ixRemove(end).ixLastKey());
        assertEquals(card, sr.ixRemove(start - 1).ixCardinality());

        // Retain / remove ranges.
        assertEquals(card - 1, sr.ixRetainRange(start + 1, end + 100).ixCardinality());
        assertEquals(card - 1, sr.ixRemoveRange(start, start).ixCardinality());
        assertEquals(2, sr.ixRemoveRange(start + 1, end - 1).ixCardinality());

        // Iteration endpoints.
        try (final RowSet.RangeIterator it = sr.ixRangeIterator()) {
            assertTrue(it.hasNext());
            it.next();
            assertEquals(start, it.currentRangeStart());
            assertEquals(end, it.currentRangeEnd());
            assertFalse(it.hasNext());
        }
        try (final RowSet.SearchIterator it = sr.ixReverseIterator()) {
            assertTrue(it.hasNext());
            assertEquals(end, it.nextLong());
        }
    }
}
