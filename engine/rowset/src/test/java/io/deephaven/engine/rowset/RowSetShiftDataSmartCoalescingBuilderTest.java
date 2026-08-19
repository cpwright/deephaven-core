//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RowSetShiftData.SmartCoalescingBuilder}.
 *
 * <p>
 * Note that the SmartCoalescingBuilder takes ownership of the RowSet passed to its constructor and closes it when the
 * builder is closed (which {@code build()} does internally), so tests pass copies.
 * </p>
 */
public class RowSetShiftDataSmartCoalescingBuilderTest {

    private static RowSetShiftData.SmartCoalescingBuilder makeBuilder(final RowSet preShiftKeys) {
        return new RowSetShiftData.SmartCoalescingBuilder(preShiftKeys.copy());
    }

    @Test
    public void testReversedPolarityNotCoalescedWithInterveningKey() {
        // Same delta, but pre-shift key space is fully populated so key 19 intervenes between the ranges.
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(0, 100)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            // Reversed polarity (positive delta) runs are presented in descending order.
            builder.shiftRange(20, 29, 5);
            builder.shiftRange(0, 9, 5);
            final RowSetShiftData result = builder.build();
            result.validate();

            assertEquals(2, result.size());
            assertEquals(0, result.getBeginRange(0));
            assertEquals(9, result.getEndRange(0));
            assertEquals(5, result.getShiftDelta(0));
            assertEquals(20, result.getBeginRange(1));
            assertEquals(29, result.getEndRange(1));
            assertEquals(5, result.getShiftDelta(1));
        }
    }

    @Test
    public void testReversedPolarityCoalescedWithoutInterveningKey() {
        // No pre-shift keys exist in (9, 20), so the two ranges may be coalesced.
        try (final WritableRowSet preShiftKeys = RowSetFactory.fromRange(0, 9)) {
            preShiftKeys.insertRange(20, 29);
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(20, 29, 5);
            builder.shiftRange(0, 9, 5);
            final RowSetShiftData result = builder.build();
            result.validate();

            assertEquals(1, result.size());
            assertEquals(0, result.getBeginRange(0));
            assertEquals(29, result.getEndRange(0));
            assertEquals(5, result.getShiftDelta(0));
        }
    }

    @Test
    public void testReversedPolarityNewSegmentMergesToExistingShift() {
        // Ascending presentation of two positive ranges forces a reverse-iterator reinitialization (a new run
        // segment); with no intervening key the new range extends the end of the existing shift.
        try (final WritableRowSet preShiftKeys = RowSetFactory.fromRange(0, 9)) {
            preShiftKeys.insertRange(20, 29);
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(0, 9, 5);
            builder.shiftRange(20, 29, 5);
            final RowSetShiftData result = builder.build();
            result.validate();

            assertEquals(1, result.size());
            assertEquals(0, result.getBeginRange(0));
            assertEquals(29, result.getEndRange(0));
            assertEquals(5, result.getShiftDelta(0));
        }
    }

    @Test
    public void testReversedPolarityNewSegmentNotMergedWithInterveningKey() {
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(0, 100)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(0, 9, 5);
            builder.shiftRange(20, 29, 5);
            final RowSetShiftData result = builder.build();
            result.validate();

            assertEquals(2, result.size());
            assertEquals(0, result.getBeginRange(0));
            assertEquals(9, result.getEndRange(0));
            assertEquals(20, result.getBeginRange(1));
            assertEquals(29, result.getEndRange(1));
        }
    }

    @Test
    public void testForwardRunNotCoalescedWithInterveningKey() {
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(0, 100)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            // Forward polarity (negative delta) runs are presented in ascending order.
            builder.shiftRange(20, 29, -5);
            builder.shiftRange(40, 49, -5);
            final RowSetShiftData result = builder.build();
            result.validate();

            assertEquals(2, result.size());
            assertEquals(20, result.getBeginRange(0));
            assertEquals(29, result.getEndRange(0));
            assertEquals(-5, result.getShiftDelta(0));
            assertEquals(40, result.getBeginRange(1));
            assertEquals(49, result.getEndRange(1));
            assertEquals(-5, result.getShiftDelta(1));
        }
    }

    @Test
    public void testForwardRunCoalescedWithoutInterveningKey() {
        try (final WritableRowSet preShiftKeys = RowSetFactory.fromRange(20, 29)) {
            preShiftKeys.insertRange(40, 49);
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(20, 29, -5);
            builder.shiftRange(40, 49, -5);
            final RowSetShiftData result = builder.build();
            result.validate();

            assertEquals(1, result.size());
            assertEquals(20, result.getBeginRange(0));
            assertEquals(49, result.getEndRange(0));
            assertEquals(-5, result.getShiftDelta(0));
        }
    }

    @Test
    public void testForwardShiftEndRangeMaxValue() {
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(0, 100)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(10, Long.MAX_VALUE, -5);
            final RowSetShiftData result = builder.build();
            result.validate();

            assertEquals(1, result.size());
            assertEquals(10, result.getBeginRange(0));
            assertEquals(Long.MAX_VALUE, result.getEndRange(0));
            assertEquals(-5, result.getShiftDelta(0));
        }
    }

    @Test
    public void testReversedShiftBelowAllPreShiftKeysIsDropped() {
        // advance(endRange) exhausts the reverse iterator; the shift is irrelevant and dropped.
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(50, 60)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(0, 9, 5);
            assertFalse(builder.nonempty());
            assertSame(RowSetShiftData.EMPTY, builder.build());
        }
    }

    @Test
    public void testReversedShiftAboveAllPreShiftKeysIsDropped() {
        // The reverse iterator lands on a key below the shift's begin; the shift is irrelevant and dropped, but a
        // later relevant range in the same run is kept.
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(50, 60)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(70, 80, 5);
            assertFalse(builder.nonempty());
            builder.shiftRange(40, 60, 5);
            assertTrue(builder.nonempty());
            final RowSetShiftData result = builder.build();
            result.validate();

            assertEquals(1, result.size());
            assertEquals(40, result.getBeginRange(0));
            assertEquals(60, result.getEndRange(0));
            assertEquals(5, result.getShiftDelta(0));
        }
    }

    @Test
    public void testForwardShiftBelowAllPreShiftKeysIsDropped() {
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(50, 60)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(0, 9, -5);
            assertFalse(builder.nonempty());
            assertSame(RowSetShiftData.EMPTY, builder.build());
        }
    }

    @Test
    public void testForwardShiftWithEmptyPreShiftKeys() {
        try (final RowSet preShiftKeys = RowSetFactory.empty()) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(10, 19, -5);
            builder.shiftRange(30, 39, 5);
            assertSame(RowSetShiftData.EMPTY, builder.build());
        }
    }

    @Test
    public void testDegenerateShiftRangeArgs() {
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(0, 100)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(10, 5, 3); // end < begin
            builder.shiftRange(0, 9, 0); // zero delta
            assertFalse(builder.nonempty());
            assertSame(RowSetShiftData.EMPTY, builder.build());
        }
    }

    @Test
    public void testPolarityTransitionReversesRunAndMatchesPlainBuilder() {
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(0, 100)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            // Positive run presented in descending order, then a negative run.
            builder.shiftRange(40, 49, 5);
            builder.shiftRange(20, 29, 5);
            builder.shiftRange(60, 69, -5);
            final RowSetShiftData result = builder.build();
            result.validate();

            final RowSetShiftData.Builder plain = new RowSetShiftData.Builder();
            plain.shiftRange(20, 29, 5);
            plain.shiftRange(40, 49, 5);
            plain.shiftRange(60, 69, -5);
            assertEquals(plain.build(), result);
        }
    }

    @Test
    public void testCloseWithoutBuild() {
        try (final RowSet preShiftKeys = RowSetFactory.fromRange(0, 100)) {
            final RowSetShiftData.SmartCoalescingBuilder builder = makeBuilder(preShiftKeys);
            builder.shiftRange(20, 29, -5);
            builder.close();
        }
    }
}
