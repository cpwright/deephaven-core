//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.SafeCloseablePair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.renderRanges;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A shift window may reach past either end of the key space: the shift is valid as long as no key actually lands there.
 * Every {@link RowSetShiftData} operation that works in post-shift keyspace clamps the window to the key space rather
 * than letting {@code end + delta} wrap, so the keys that are inside the window are moved, extracted, or reported.
 */
public class RowSetShiftDataWindowPastKeySpaceTest {

    private static final long MAX = Long.MAX_VALUE;

    private static WritableRowSet rowSetOf(final long... keys) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (final long k : keys) {
            b.appendKey(k);
        }
        return b.build();
    }

    private static RowSetShiftData shift(final long begin, final long end, final long delta) {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(begin, end, delta);
        final RowSetShiftData shiftData = b.build();
        shiftData.validate();
        return shiftData;
    }

    /** [MAX-10, MAX] moves by +5; keys at MAX-10 and MAX-9 land within the key space. */
    private static RowSetShiftData topWindow() {
        return shift(MAX - 10, MAX, 5);
    }

    /** [0, 10] moves by -5; keys at 5 and 6 land within the key space. */
    private static RowSetShiftData bottomWindow() {
        return shift(0, 10, -5);
    }

    private static String range(final long start, final long end) {
        return start + "-" + end + " ";
    }

    @Test
    public void testApplyRowSet() {
        try (final WritableRowSet rs = rowSetOf(MAX - 10, MAX - 9)) {
            topWindow().apply(rs);
            assertEquals(range(MAX - 5, MAX - 4), renderRanges(rs));
        }
        try (final WritableRowSet rs = rowSetOf(5, 6)) {
            bottomWindow().apply(rs);
            assertEquals(range(0, 1), renderRanges(rs));
        }
    }

    @Test
    public void testUnapplyRowSet() {
        try (final WritableRowSet rs = rowSetOf(MAX - 5, MAX - 4)) {
            topWindow().unapply(rs);
            assertEquals(range(MAX - 10, MAX - 9), renderRanges(rs));
        }
        try (final WritableRowSet rs = rowSetOf(0, 1)) {
            bottomWindow().unapply(rs);
            assertEquals(range(5, 6), renderRanges(rs));
        }
    }

    @Test
    public void testUnapplyRowSetWithOffset() {
        try (final WritableRowSet rs = rowSetOf(MAX - 3, MAX - 2)) {
            topWindow().unapply(rs, 2);
            assertEquals(range(MAX - 8, MAX - 7), renderRanges(rs));
        }
        try (final WritableRowSet rs = rowSetOf(1, 2)) {
            bottomWindow().unapply(rs, 1);
            assertEquals(range(6, 7), renderRanges(rs));
        }
    }

    @Test
    public void testExtractParallelShiftedRows() {
        try (final WritableRowSet post = rowSetOf(MAX - 5, MAX - 4);
                final SafeCloseablePair<RowSet, RowSet> pair =
                        topWindow().extractParallelShiftedRowsFromPostShiftRowSet(post)) {
            assertEquals("pre-shift keys", range(MAX - 10, MAX - 9), renderRanges(pair.first));
            assertEquals("post-shift keys", range(MAX - 5, MAX - 4), renderRanges(pair.second));
        }
        try (final WritableRowSet post = rowSetOf(0, 1);
                final SafeCloseablePair<RowSet, RowSet> pair =
                        bottomWindow().extractParallelShiftedRowsFromPostShiftRowSet(post)) {
            assertEquals("pre-shift keys", range(5, 6), renderRanges(pair.first));
            assertEquals("post-shift keys", range(0, 1), renderRanges(pair.second));
        }
    }

    @Test
    public void testStaticUnapplyShift() {
        try (final WritableRowSet rs = rowSetOf(MAX - 5, MAX - 4)) {
            assertTrue("keys were moved", RowSetShiftData.unapplyShift(rs, MAX - 10, MAX, 5));
            assertEquals(range(MAX - 10, MAX - 9), renderRanges(rs));
        }
        try (final WritableRowSet rs = rowSetOf(0, 1)) {
            assertTrue("keys were moved", RowSetShiftData.unapplyShift(rs, 0, 10, -5));
            assertEquals(range(5, 6), renderRanges(rs));
        }
    }

    @Test
    public void testUnapplyCallback() {
        final List<long[]> seen = new ArrayList<>();
        topWindow().unapply((begin, end, delta) -> seen.add(new long[] {begin, end, delta}));
        assertEquals(1, seen.size());
        assertEquals("begin", MAX - 5, seen.get(0)[0]);
        assertEquals("end is clamped to the top of the key space", MAX, seen.get(0)[1]);
        assertEquals("delta", -5L, seen.get(0)[2]);

        seen.clear();
        bottomWindow().unapply((begin, end, delta) -> seen.add(new long[] {begin, end, delta}));
        assertEquals(1, seen.size());
        assertEquals("begin is clamped to zero", 0L, seen.get(0)[0]);
        assertEquals("end", 5L, seen.get(0)[1]);
        assertEquals("delta", 5L, seen.get(0)[2]);
    }

    /**
     * A window lying wholly outside the key space is ordered against nothing, so the builder accepts it after or before
     * other shifts and the built data validates, where comparing the wrapped {@code end + delta} would reject it.
     */
    @Test
    public void testWindowWhollyOutsideKeySpaceBuildsAfterOtherShifts() {
        final RowSetShiftData.Builder top = new RowSetShiftData.Builder();
        top.shiftRange(0, 0, 1);
        top.shiftRange(MAX - 10, MAX - 5, 20);
        final RowSetShiftData topData = top.build();
        topData.validate();
        assertEquals(2, topData.size());
        try (final WritableRowSet rs = rowSetOf(0, MAX - 20, MAX - 3)) {
            topData.apply(rs);
            assertEquals(range(1, 1) + range(MAX - 20, MAX - 20) + range(MAX - 3, MAX - 3), renderRanges(rs));
            topData.unapply(rs);
            assertEquals(range(0, 0) + range(MAX - 20, MAX - 20) + range(MAX - 3, MAX - 3), renderRanges(rs));
        }

        final RowSetShiftData.Builder bottom = new RowSetShiftData.Builder();
        bottom.shiftRange(0, 5, -20);
        bottom.shiftRange(100, 110, -5);
        final RowSetShiftData bottomData = bottom.build();
        bottomData.validate();
        assertEquals(2, bottomData.size());
        try (final WritableRowSet rs = rowSetOf(50, 105)) {
            bottomData.apply(rs);
            assertEquals(range(50, 50) + range(100, 100), renderRanges(rs));
            bottomData.unapply(rs);
            assertEquals(range(50, 50) + range(105, 105), renderRanges(rs));
        }
    }

    /** Overlap of two windows inside the key space is still rejected, at the top of the key space as anywhere else. */
    @Test
    public void testOverlappingWindowsStillRejected() {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(MAX - 30, MAX - 20, 5);
        try {
            b.shiftRange(MAX - 19, MAX - 10, -1);
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError("overlapping post-shift windows were accepted");
    }

    /** A window lying wholly outside the key space in post-shift space holds no keys and is skipped everywhere. */
    @Test
    public void testWindowWhollyOutsideKeySpace() {
        final RowSetShiftData wholly = shift(MAX - 10, MAX - 5, 20);
        try (final WritableRowSet rs = rowSetOf(MAX - 20, MAX - 3)) {
            wholly.unapply(rs);
            assertEquals(range(MAX - 20, MAX - 20) + range(MAX - 3, MAX - 3), renderRanges(rs));
            assertEquals(false, RowSetShiftData.unapplyShift(rs, MAX - 10, MAX - 5, 20));
            try (final SafeCloseablePair<RowSet, RowSet> pair =
                    wholly.extractParallelShiftedRowsFromPostShiftRowSet(rs)) {
                assertEquals("", renderRanges(pair.first));
            }
        }
        final List<long[]> seen = new ArrayList<>();
        wholly.unapply((begin, end, delta) -> seen.add(new long[] {begin, end, delta}));
        assertEquals("no window to report", 0, seen.size());
    }
}
