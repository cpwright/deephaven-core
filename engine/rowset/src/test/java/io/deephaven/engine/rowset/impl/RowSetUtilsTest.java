//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RowSetUtils#forAllInvertedLongRanges}, {@link RowSetUtils#rangeSearch}, and
 * {@link RowSetUtils#equals}.
 */
public class RowSetUtilsTest {

    private static List<long[]> invertedRanges(final RowSet source, final RowSet dest) {
        final List<long[]> result = new ArrayList<>();
        RowSetUtils.forAllInvertedLongRanges(source, dest, (start, end) -> result.add(new long[] {start, end}));
        return result;
    }

    private static void assertMatchesInvert(final RowSet source, final RowSet dest) {
        final List<long[]> actual = invertedRanges(source, dest);
        final List<long[]> expected = new ArrayList<>();
        try (final RowSet inverted = source.invert(dest)) {
            inverted.forAllRowKeyRanges((start, end) -> expected.add(new long[] {start, end}));
        }
        assertEquals(expected.size(), actual.size());
        for (int ii = 0; ii < expected.size(); ++ii) {
            assertEquals(expected.get(ii)[0], actual.get(ii)[0]);
            assertEquals(expected.get(ii)[1], actual.get(ii)[1]);
        }
    }

    @Test
    public void testForAllInvertedLongRangesNonCoalesced() {
        try (final WritableRowSet source = RowSetFactory.fromRange(10, 19);
                final RowSet dest = RowSetFactory.fromKeys(12, 35)) {
            source.insertRange(30, 39);

            final List<long[]> ranges = invertedRanges(source, dest);
            assertEquals(2, ranges.size());
            assertEquals(2, ranges.get(0)[0]);
            assertEquals(2, ranges.get(0)[1]);
            assertEquals(15, ranges.get(1)[0]);
            assertEquals(15, ranges.get(1)[1]);

            assertMatchesInvert(source, dest);
        }
    }

    @Test
    public void testForAllInvertedLongRangesCoalescedAcrossSourceGap() {
        // dest keys {18,19} and {30,31} are adjacent in source position space (positions 8-9 and 10-11) and must be
        // emitted as a single coalesced run.
        try (final WritableRowSet source = RowSetFactory.fromRange(10, 19);
                final WritableRowSet dest = RowSetFactory.fromRange(18, 19)) {
            source.insertRange(30, 39);
            dest.insertRange(30, 31);

            final List<long[]> ranges = invertedRanges(source, dest);
            assertEquals(1, ranges.size());
            assertEquals(8, ranges.get(0)[0]);
            assertEquals(11, ranges.get(0)[1]);

            assertMatchesInvert(source, dest);
        }
    }

    @Test
    public void testForAllInvertedLongRangesContiguousDest() {
        try (final WritableRowSet source = RowSetFactory.fromRange(10, 19);
                final RowSet dest = RowSetFactory.fromRange(12, 17)) {
            source.insertRange(30, 39);

            final List<long[]> ranges = invertedRanges(source, dest);
            assertEquals(1, ranges.size());
            assertEquals(2, ranges.get(0)[0]);
            assertEquals(7, ranges.get(0)[1]);

            assertMatchesInvert(source, dest);
        }
    }

    @Test
    public void testForAllInvertedLongRangesEmptyDest() {
        try (final WritableRowSet source = RowSetFactory.fromRange(10, 19);
                final RowSet dest = RowSetFactory.empty()) {
            source.insertRange(30, 39);
            assertTrue(invertedRanges(source, dest).isEmpty());
        }
    }

    @Test
    public void testRangeSearchFloorForTargetsBetweenProbeValues() {
        // The comparator never returns 0: it behaves as if the target is t + 0.5. rangeSearch must converge to t from
        // both sides for every strictly-interior target.
        final long begin = 0;
        final long end = 99;
        for (long t = begin; t <= end; ++t) {
            final long target = t;
            final long result = RowSetUtils.rangeSearch(begin, end, value -> value <= target ? 1 : -1);
            assertEquals("target between " + t + " and " + (t + 1), t, result);
        }
    }

    @Test
    public void testRangeSearchTargetBelowBegin() {
        // directionToTargetFrom(begin) <= 0: returns begin immediately.
        assertEquals(0, RowSetUtils.rangeSearch(0, 99, value -> -1));
    }

    @Test
    public void testRangeSearchTargetAtOrAboveEnd() {
        // directionToTargetFrom(end) >= 0: returns end immediately.
        assertEquals(99, RowSetUtils.rangeSearch(0, 99, value -> 1));
    }

    @Test
    public void testRangeSearchExactMatch() {
        for (long t = 0; t <= 99; ++t) {
            final long target = t;
            final long result = RowSetUtils.rangeSearch(0, 99, value -> Long.compare(target, value));
            assertEquals(t, result);
        }
    }

    @Test
    public void testEqualsNonRowSetObject() {
        try (final RowSet rowSet = RowSetFactory.fromRange(0, 9)) {
            assertFalse(RowSetUtils.equals(rowSet, "not a rowset"));
            assertFalse(RowSetUtils.equals(rowSet, null));
        }
    }

    @Test
    public void testEqualsDifferentSizes() {
        try (final RowSet a = RowSetFactory.fromRange(0, 9);
                final RowSet b = RowSetFactory.fromRange(0, 8)) {
            assertFalse(RowSetUtils.equals(a, b));
            assertFalse(RowSetUtils.equals(b, a));
        }
    }

    @Test
    public void testEqualsSameSizeDifferentKeys() {
        try (final RowSet a = RowSetFactory.fromRange(0, 3);
                final RowSet b = RowSetFactory.fromRange(10, 13)) {
            assertFalse(RowSetUtils.equals(a, b));
        }
    }

    @Test
    public void testEqualsSameSizeDifferentRangeBoundaries() {
        try (final RowSet a = RowSetFactory.fromRange(0, 5);
                final WritableRowSet b = RowSetFactory.fromRange(0, 3)) {
            b.insertRange(5, 6);
            assertEquals(a.size(), b.size());
            assertFalse(RowSetUtils.equals(a, b));
        }
    }

    @Test
    public void testEqualsEqualRowSets() {
        try (final WritableRowSet a = RowSetFactory.fromRange(0, 5);
                final WritableRowSet b = RowSetFactory.fromRange(0, 5)) {
            a.insertRange(10, 15);
            b.insertRange(10, 15);
            assertTrue(RowSetUtils.equals(a, b));
        }
    }

    @Test
    public void testRangeSearchHighKeysMidpointDoesNotOverflow() {
        // Row keys are legal up to Long.MAX_VALUE; a naive (begin + end) / 2 midpoint wraps for keys >= 2^62.
        final long begin = Long.MAX_VALUE / 2;
        final long end = Long.MAX_VALUE - 1;
        final long target = Long.MAX_VALUE - 5;
        final long result = RowSetUtils.rangeSearch(begin, end, value -> Long.compare(target, value));
        assertEquals(target, result);
    }
}
