//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import org.junit.Test;

import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the "negative answer" sides of {@link SortedRanges} containment predicates: {@code overlapsRange},
 * {@code subsetOf(RowSet.RangeIterator)} and {@code ixSubsetOf(OrderedLongSet)}. Each assertion is checked against a
 * {@link TreeSet} model of the same key sets.
 */
public class SortedRangesPredicateTest {

    // region helpers

    /**
     * Build a SortedRanges from a list of (start, end) pairs; fails the test on capacity overflow (all shapes used here
     * are tiny, so this should never happen).
     */
    private static SortedRanges sr(final long... startEndPairs) {
        assertEquals("startEndPairs must be pairs", 0, startEndPairs.length % 2);
        SortedRanges ans = SortedRanges.makeEmpty();
        for (int i = 0; i < startEndPairs.length; i += 2) {
            ans = ans.addRange(startEndPairs[i], startEndPairs[i + 1]);
            assertNotNull("unexpected capacity overflow at pair " + (i / 2), ans);
        }
        return ans;
    }

    private static RspBitmap rsp(final long... startEndPairs) {
        assertEquals("startEndPairs must be pairs", 0, startEndPairs.length % 2);
        RspBitmap ans = RspBitmap.makeEmpty();
        for (int i = 0; i < startEndPairs.length; i += 2) {
            ans = ans.addRange(startEndPairs[i], startEndPairs[i + 1]);
        }
        return ans;
    }

    private static NavigableSet<Long> model(final long... startEndPairs) {
        final TreeSet<Long> ans = new TreeSet<>();
        for (int i = 0; i < startEndPairs.length; i += 2) {
            for (long v = startEndPairs[i]; v <= startEndPairs[i + 1]; ++v) {
                ans.add(v);
            }
        }
        return ans;
    }

    private static boolean modelOverlapsRange(final NavigableSet<Long> m, final long start, final long end) {
        if (end < start) {
            return false;
        }
        return !m.subSet(start, true, end, true).isEmpty();
    }

    /**
     * Assert {@code sr.overlapsRange(start, end)} against the model, and cross check the public {@link RowSet} API
     * (which routes through {@code ixOverlapsRange}).
     */
    private static void checkOverlapsRange(
            final SortedRanges s, final NavigableSet<Long> m, final long start, final long end) {
        final boolean expected = modelOverlapsRange(m, start, end);
        final String msg = "start=" + start + " end=" + end;
        assertEquals(msg, expected, s.overlapsRange(start, end));
        assertEquals(msg, expected, s.ixOverlapsRange(start, end));
        try (final RowSet rs = new WritableRowSetImpl(s.ixCowRef())) {
            assertEquals(msg, expected, rs.overlapsRange(start, end));
        }
    }

    /**
     * Assert {@code s.ixSubsetOf(other)} against the model.
     */
    private static void checkSubsetOf(
            final SortedRanges s,
            final NavigableSet<Long> sModel,
            final OrderedLongSet other,
            final NavigableSet<Long> otherModel,
            final String msg) {
        final boolean expected = otherModel.containsAll(sModel);
        assertEquals(msg + " (model)", expected, s.ixSubsetOf(other));
    }

    // endregion helpers

    // region overlapsRange

    @Test
    public void testOverlapsRangeInvertedArgs() {
        final SortedRanges s = sr(10, 12, 20, 22);
        final NavigableSet<Long> m = model(10, 12, 20, 22);
        // end < start: always false, even when both endpoints are individually present.
        checkOverlapsRange(s, m, 12, 10);
        checkOverlapsRange(s, m, 22, 20);
        checkOverlapsRange(s, m, 11, 11 - 1);
        // end < start where the (inverted) span would have covered the whole set.
        checkOverlapsRange(s, m, 30, 1);
        assertFalse(s.overlapsRange(12, 10));
    }

    @Test
    public void testOverlapsRangeEntirelyBelow() {
        final SortedRanges s = sr(10, 12, 20, 22);
        final NavigableSet<Long> m = model(10, 12, 20, 22);
        // end < first()
        checkOverlapsRange(s, m, 0, 9);
        checkOverlapsRange(s, m, 9, 9);
        checkOverlapsRange(s, m, 0, 0);
        assertFalse(s.overlapsRange(0, 9));
    }

    @Test
    public void testOverlapsRangeEntirelyAbove() {
        final SortedRanges s = sr(10, 12, 20, 22);
        final NavigableSet<Long> m = model(10, 12, 20, 22);
        // last() < start
        checkOverlapsRange(s, m, 23, 100);
        checkOverlapsRange(s, m, 23, 23);
        assertFalse(s.overlapsRange(23, 100));
    }

    @Test
    public void testOverlapsRangeInsideGap() {
        final SortedRanges s = sr(10, 12, 20, 22);
        final NavigableSet<Long> m = model(10, 12, 20, 22);
        // Strictly inside the gap: bounds checks pass but the internal search must report no overlap.
        checkOverlapsRange(s, m, 13, 19);
        checkOverlapsRange(s, m, 14, 18);
        checkOverlapsRange(s, m, 13, 13);
        checkOverlapsRange(s, m, 19, 19);
        assertFalse(s.overlapsRange(14, 18));
    }

    @Test
    public void testOverlapsRangeEmptySortedRanges() {
        final SortedRanges s = SortedRanges.makeEmpty();
        assertTrue(s.isEmpty());
        assertFalse(s.overlapsRange(0, 0));
        assertFalse(s.overlapsRange(10, 20));
        assertFalse(s.overlapsRange(0, Long.MAX_VALUE - 1));
        assertFalse(s.ixOverlapsRange(10, 20));
        // Inverted args on an empty set, too.
        assertFalse(s.overlapsRange(20, 10));
    }

    @Test
    public void testOverlapsRangeTrueCases() {
        final SortedRanges s = sr(10, 12, 20, 22);
        final NavigableSet<Long> m = model(10, 12, 20, 22);
        // Touching the first key from below.
        checkOverlapsRange(s, m, 1, 10);
        // Exactly the first key.
        checkOverlapsRange(s, m, 10, 10);
        // Touching the last key from above.
        checkOverlapsRange(s, m, 22, 30);
        // Exactly the last key.
        checkOverlapsRange(s, m, 22, 22);
        // Spanning the gap, touching both ranges.
        checkOverlapsRange(s, m, 12, 20);
        checkOverlapsRange(s, m, 11, 21);
        // Covering everything and then some.
        checkOverlapsRange(s, m, 0, 100);
        // Strictly inside a range.
        checkOverlapsRange(s, m, 21, 21);
        checkOverlapsRange(s, m, 11, 11);
        // From inside the gap into the second range.
        checkOverlapsRange(s, m, 15, 20);
        // From inside the first range into the gap.
        checkOverlapsRange(s, m, 12, 15);
        assertTrue(s.overlapsRange(12, 20));
    }

    @Test
    public void testOverlapsRangeSingletonsAndGaps() {
        // A shape with singleton elements interleaved with ranges.
        final SortedRanges s = sr(10, 10, 20, 30, 50, 50);
        final NavigableSet<Long> m = model(10, 10, 20, 30, 50, 50);
        for (long start = 8; start <= 52; ++start) {
            for (long end = start - 2; end <= 52; ++end) {
                checkOverlapsRange(s, m, start, end);
            }
        }
    }

    @Test
    public void testIxOverlapsNegatives() {
        final SortedRanges s = sr(10, 12, 20, 22);
        // Empty argument.
        assertFalse(s.ixOverlaps(OrderedLongSet.EMPTY));
        // Empty receiver.
        assertFalse(SortedRanges.makeEmpty().ixOverlaps(SingleRange.make(10, 12)));
        // SingleRange argument entirely in the gap.
        assertFalse(s.ixOverlaps(SingleRange.make(14, 18)));
        // SingleRange argument entirely below / above.
        assertFalse(s.ixOverlaps(SingleRange.make(0, 9)));
        assertFalse(s.ixOverlaps(SingleRange.make(23, 100)));
        // Multi-range arguments that interleave only with our gaps.
        assertFalse(s.ixOverlaps(sr(13, 14, 16, 19)));
        assertFalse(s.ixOverlaps(rsp(13, 14, 16, 19)));
        // True cases for contrast.
        assertTrue(s.ixOverlaps(SingleRange.make(12, 20)));
        assertTrue(s.ixOverlaps(sr(13, 14, 16, 20)));
        assertTrue(s.ixOverlaps(rsp(0, 10)));
    }

    // endregion overlapsRange

    // region subsetOf

    @Test
    public void testSubsetOfOtherExhaustedFirstSortedRanges() {
        // We fit inside other's bounds and our cardinality is smaller, so the cheap pre-checks in ixSubsetOf pass;
        // the range iteration must then discover that other ran out before our last range.
        final SortedRanges s = sr(10, 12, 30, 32);
        final NavigableSet<Long> sm = model(10, 12, 30, 32);
        final SortedRanges other = sr(10, 12, 20, 28);
        final NavigableSet<Long> om = model(10, 12, 20, 28);
        assertTrue("pre-check must not short-circuit", s.getCardinality() <= other.getCardinality());
        assertTrue("bounds must overlap", s.last() >= other.ixFirstKey() && other.ixLastKey() >= s.first());
        checkSubsetOf(s, sm, other, om, "other exhausted before us (SortedRanges)");
        assertFalse(s.ixSubsetOf(other));
    }

    @Test
    public void testSubsetOfOtherExhaustedFirstRsp() {
        final SortedRanges s = sr(10, 12, 30, 32);
        final NavigableSet<Long> sm = model(10, 12, 30, 32);
        final RspBitmap other = rsp(10, 12, 20, 28);
        final NavigableSet<Long> om = model(10, 12, 20, 28);
        assertTrue(s.getCardinality() <= other.ixCardinality());
        checkSubsetOf(s, sm, other, om, "other exhausted before us (RspBitmap)");
        assertFalse(s.ixSubsetOf(other));
    }

    @Test
    public void testSubsetOfViaRangeIteratorDirectly() {
        // {10-12,20-22}.subsetOf({10-12}) exercises the !ritOther.advance() path directly, bypassing the
        // cardinality pre-check in ixSubsetOf.
        final SortedRanges s = sr(10, 12, 20, 22);
        final SortedRanges other = sr(10, 12);
        assertFalse(s.subsetOf(other.getRangeIterator()));
        // And the same via ixSubsetOf (which short-circuits on cardinality, same answer).
        assertFalse(s.ixSubsetOf(other));
        assertFalse(s.ixSubsetOf(rsp(10, 12)));
        assertFalse(s.ixSubsetOf(SingleRange.make(10, 12)));
        assertFalse(model(10, 12).containsAll(model(10, 12, 20, 22)));
    }

    @Test
    public void testSubsetOfDisjointBelow() {
        // {10-20}.ixSubsetOf({1-5}) -> false; other's last key is below our first.
        final SortedRanges s = sr(10, 20);
        final NavigableSet<Long> sm = model(10, 20);
        checkSubsetOf(s, sm, SingleRange.make(1, 5), model(1, 5), "disjoint below (SingleRange)");
        checkSubsetOf(s, sm, sr(1, 2, 4, 5), model(1, 2, 4, 5), "disjoint below (SortedRanges)");
        checkSubsetOf(s, sm, rsp(1, 2, 4, 5), model(1, 2, 4, 5), "disjoint below (RspBitmap)");
        assertFalse(s.ixSubsetOf(SingleRange.make(1, 5)));
    }

    @Test
    public void testSubsetOfDisjointAbove() {
        // Our last key is below other's first key.
        final SortedRanges s = sr(10, 20);
        final NavigableSet<Long> sm = model(10, 20);
        checkSubsetOf(s, sm, SingleRange.make(21, 25), model(21, 25), "disjoint above (SingleRange)");
        checkSubsetOf(s, sm, sr(21, 22, 24, 25), model(21, 22, 24, 25), "disjoint above (SortedRanges)");
        checkSubsetOf(s, sm, rsp(21, 22, 24, 25), model(21, 22, 24, 25), "disjoint above (RspBitmap)");
        assertFalse(s.ixSubsetOf(SingleRange.make(21, 25)));
    }

    @Test
    public void testSubsetOfSameCardinalityDifferentKeys() {
        final SortedRanges s = sr(10, 12);
        final NavigableSet<Long> sm = model(10, 12);
        // Shifted by one: same cardinality, overlapping bounds, but not a subset.
        checkSubsetOf(s, sm, sr(11, 13), model(11, 13), "same cardinality shifted up (SortedRanges)");
        checkSubsetOf(s, sm, rsp(11, 13), model(11, 13), "same cardinality shifted up (RspBitmap)");
        checkSubsetOf(s, sm, SingleRange.make(11, 13), model(11, 13), "same cardinality shifted up (SingleRange)");
        checkSubsetOf(s, sm, sr(9, 11), model(9, 11), "same cardinality shifted down (SortedRanges)");
        checkSubsetOf(s, sm, rsp(9, 11), model(9, 11), "same cardinality shifted down (RspBitmap)");
        // Same cardinality, split differently: other starts at our start but has a hole.
        final SortedRanges s2 = sr(10, 12, 20, 22);
        final NavigableSet<Long> s2m = model(10, 12, 20, 22);
        checkSubsetOf(s2, s2m, sr(10, 11, 13, 13, 20, 22), model(10, 11, 13, 13, 20, 22), "hole in other");
        checkSubsetOf(s2, s2m, rsp(10, 11, 13, 13, 20, 22), model(10, 11, 13, 13, 20, 22), "hole in other (rsp)");
        assertFalse(s.ixSubsetOf(sr(11, 13)));
    }

    @Test
    public void testSubsetOfOtherRangeStartsTooLateOrEndsTooEarly() {
        // otherStart > start
        final SortedRanges a = sr(10, 12);
        checkSubsetOf(a, model(10, 12), sr(11, 13, 20, 30), model(11, 13, 20, 30), "otherStart > start");
        // otherEnd < end
        final SortedRanges b = sr(10, 15);
        checkSubsetOf(b, model(10, 15), sr(10, 12, 20, 30), model(10, 12, 20, 30), "otherEnd < end");
        checkSubsetOf(b, model(10, 15), rsp(10, 12, 20, 30), model(10, 12, 20, 30), "otherEnd < end (rsp)");
        assertFalse(b.ixSubsetOf(sr(10, 12, 20, 30)));
    }

    @Test
    public void testSubsetOfEmptyCases() {
        // Empty receiver is a subset of everything, including the empty set.
        final SortedRanges empty = SortedRanges.makeEmpty();
        assertTrue(empty.ixSubsetOf(SingleRange.make(10, 12)));
        assertTrue(empty.ixSubsetOf(sr(10, 12, 20, 22)));
        assertTrue(empty.ixSubsetOf(rsp(10, 12, 20, 22)));
        assertTrue(empty.ixSubsetOf(OrderedLongSet.EMPTY));
        // A non-empty set is never a subset of the empty set.
        assertFalse(sr(10, 12).ixSubsetOf(OrderedLongSet.EMPTY));
    }

    @Test
    public void testSubsetOfTrueCases() {
        final SortedRanges s = sr(10, 12, 20, 22);
        final NavigableSet<Long> sm = model(10, 12, 20, 22);

        // Proper subset, SingleRange other spanning everything (including our gap).
        checkSubsetOf(s, sm, SingleRange.make(10, 22), model(10, 22), "proper subset (SingleRange, tight)");
        checkSubsetOf(s, sm, SingleRange.make(0, 100), model(0, 100), "proper subset (SingleRange, loose)");
        // Proper subset, multi-range other.
        checkSubsetOf(s, sm, sr(5, 15, 18, 25), model(5, 15, 18, 25), "proper subset (SortedRanges)");
        checkSubsetOf(s, sm, rsp(5, 15, 18, 25), model(5, 15, 18, 25), "proper subset (RspBitmap)");
        // Equal sets.
        checkSubsetOf(s, sm, sr(10, 12, 20, 22), sm, "equal (SortedRanges)");
        checkSubsetOf(s, sm, rsp(10, 12, 20, 22), sm, "equal (RspBitmap)");
        // Equal singleton-range sets, via SingleRange.
        final SortedRanges one = sr(10, 20);
        checkSubsetOf(one, model(10, 20), SingleRange.make(10, 20), model(10, 20), "equal (SingleRange)");
        assertTrue(s.ixSubsetOf(sr(10, 12, 20, 22)));
        assertTrue(s.subsetOf(sr(10, 12, 20, 22).getRangeIterator()));
    }

    @Test
    public void testSubsetOfViaPublicRowSetApi() {
        final SortedRanges s = sr(10, 12, 30, 32);
        final SortedRanges other = sr(10, 12, 20, 28);
        try (final RowSet a = new WritableRowSetImpl(s.ixCowRef());
                final RowSet b = new WritableRowSetImpl(other.ixCowRef())) {
            assertFalse(a.subsetOf(b));
            assertFalse(b.subsetOf(a));
            assertTrue(a.subsetOf(a));
        }
        try (final RowSet a = new WritableRowSetImpl(sr(10, 12, 20, 22).ixCowRef());
                final RowSet b = new WritableRowSetImpl(sr(5, 15, 18, 25).ixCowRef())) {
            assertTrue(a.subsetOf(b));
            assertFalse(b.subsetOf(a));
        }
    }

    @Test
    public void testSubsetOfExhaustiveSmallShapes() {
        // Exhaustive-ish model cross check over a handful of small shapes and all three argument implementations.
        final long[][] shapes = new long[][] {
                {10, 10},
                {10, 12},
                {10, 12, 20, 22},
                {10, 10, 20, 30, 50, 50},
                {9, 13},
                {11, 11},
                {20, 30},
                {10, 30},
                {5, 40},
        };
        for (final long[] mine : shapes) {
            final SortedRanges s = sr(mine);
            final NavigableSet<Long> sm = model(mine);
            for (final long[] theirs : shapes) {
                final NavigableSet<Long> om = model(theirs);
                final boolean expected = om.containsAll(sm);
                final String msg = "mine=" + s + " theirs=" + java.util.Arrays.toString(theirs);
                assertEquals(msg + " (SortedRanges)", expected, s.ixSubsetOf(sr(theirs)));
                assertEquals(msg + " (RspBitmap)", expected, s.ixSubsetOf(rsp(theirs)));
                if (theirs.length == 2) {
                    assertEquals(msg + " (SingleRange)", expected,
                            s.ixSubsetOf(SingleRange.make(theirs[0], theirs[1])));
                }
                // Also drive subsetOf(RangeIterator) directly, which has no cardinality pre-check.
                assertEquals(msg + " (RangeIterator)", expected, s.subsetOf(sr(theirs).getRangeIterator()));
            }
        }
    }

    // endregion subsetOf
}
