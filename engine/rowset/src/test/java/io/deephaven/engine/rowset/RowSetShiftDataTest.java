//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.util.SafeCloseablePair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link RowSetShiftData} and its {@link RowSetShiftData.Builder}.
 */
public class RowSetShiftDataTest {

    private static RowSetShiftData build(final long... triplets) {
        assertEquals(0, triplets.length % 3);
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        for (int ii = 0; ii < triplets.length; ii += 3) {
            builder.shiftRange(triplets[ii], triplets[ii + 1], triplets[ii + 2]);
        }
        return builder.build();
    }

    private static final class Emission {
        final long begin;
        final long end;
        final long delta;
        final boolean polarityReversed;

        Emission(final long begin, final long end, final long delta, final boolean polarityReversed) {
            this.begin = begin;
            this.end = end;
            this.delta = delta;
            this.polarityReversed = polarityReversed;
        }
    }

    private static List<Emission> drain(final RowSetShiftData.Iterator it) {
        final List<Emission> result = new ArrayList<>();
        while (it.hasNext()) {
            it.next();
            result.add(new Emission(it.beginRange(), it.endRange(), it.shiftDelta(), it.polarityReversed()));
        }
        return result;
    }

    private static void assertEmission(final Emission emission, final long begin, final long end, final long delta,
            final boolean polarityReversed) {
        assertEquals(begin, emission.begin);
        assertEquals(end, emission.end);
        assertEquals(delta, emission.delta);
        assertEquals(polarityReversed, emission.polarityReversed);
    }

    // region applyIterator

    @Test
    public void testApplyIteratorMultiPolarity() {
        final RowSetShiftData shiftData = build(
                0, 9, -5,
                20, 29, 5,
                40, 49, -3);
        shiftData.validate();

        final List<Emission> emissions = drain(shiftData.applyIterator());
        assertEquals(3, emissions.size());
        assertEmission(emissions.get(0), 0, 9, -5, false);
        assertEmission(emissions.get(1), 20, 29, 5, true);
        assertEmission(emissions.get(2), 40, 49, -3, false);
    }

    @Test
    public void testApplyIteratorPositiveRunTraversedInReverse() {
        // Two non-adjacent positive shifts form a single polarity run; the iterator must emit them in reverse order.
        final RowSetShiftData shiftData = build(
                0, 9, 5,
                20, 29, 5);
        shiftData.validate();

        final List<Emission> emissions = drain(shiftData.applyIterator());
        assertEquals(2, emissions.size());
        assertEmission(emissions.get(0), 20, 29, 5, true);
        assertEmission(emissions.get(1), 0, 9, 5, true);
    }

    @Test
    public void testApplyIteratorNegativeRunThenPositiveRun() {
        final RowSetShiftData shiftData = build(
                10, 19, -5,
                30, 39, -5,
                50, 59, 7);
        shiftData.validate();

        final List<Emission> emissions = drain(shiftData.applyIterator());
        assertEquals(3, emissions.size());
        // Negative runs traverse in ascending order.
        assertEmission(emissions.get(0), 10, 19, -5, false);
        assertEmission(emissions.get(1), 30, 39, -5, false);
        // Positive run.
        assertEmission(emissions.get(2), 50, 59, 7, true);
    }

    @Test
    public void testApplyIteratorEmpty() {
        final RowSetShiftData.Iterator it = RowSetShiftData.EMPTY.applyIterator();
        assertSame(RowSetShiftData.Iterator.EMPTY, it);
        assertFalse(it.hasNext());
        try {
            it.next();
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
        try {
            it.beginRange();
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
        try {
            it.endRange();
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
        try {
            it.shiftDelta();
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
        try {
            it.polarityReversed();
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testApplyCallbackMatchesApplyIteratorOrder() {
        final RowSetShiftData shiftData = build(
                10, 19, -5,
                30, 39, -5,
                50, 59, 7);

        final List<long[]> calls = new ArrayList<>();
        shiftData.apply((begin, end, delta) -> calls.add(new long[] {begin, end, delta}));

        final List<Emission> emissions = drain(shiftData.applyIterator());
        assertEquals(emissions.size(), calls.size());
        for (int ii = 0; ii < calls.size(); ++ii) {
            assertEquals(emissions.get(ii).begin, calls.get(ii)[0]);
            assertEquals(emissions.get(ii).end, calls.get(ii)[1]);
            assertEquals(emissions.get(ii).delta, calls.get(ii)[2]);
        }
    }

    @Test
    public void testUnapplyCallbackOrderAndValues() {
        final RowSetShiftData shiftData = build(
                10, 19, -5,
                30, 39, -5,
                50, 59, 7);

        final List<long[]> calls = new ArrayList<>();
        shiftData.unapply((begin, end, delta) -> calls.add(new long[] {begin, end, delta}));

        // The negative run is unapplied in descending order; ranges are pre-adjusted to post-shift space with delta
        // negated.
        assertEquals(3, calls.size());
        assertEquals(25, calls.get(0)[0]);
        assertEquals(34, calls.get(0)[1]);
        assertEquals(5, calls.get(0)[2]);
        assertEquals(5, calls.get(1)[0]);
        assertEquals(14, calls.get(1)[1]);
        assertEquals(5, calls.get(1)[2]);
        // The positive run is unapplied in ascending order.
        assertEquals(57, calls.get(2)[0]);
        assertEquals(66, calls.get(2)[1]);
        assertEquals(-7, calls.get(2)[2]);
    }

    // endregion applyIterator

    // region forAllInRowSet

    @Test
    public void testForAllInRowSetMixedPolarity() {
        final RowSetShiftData shiftData = build(
                0, 9, 10,
                100, 109, -10);

        try (final RowSet rowSet = RowSetFactory.fromKeys(5, 50, 105)) {
            final List<long[]> calls = new ArrayList<>();
            shiftData.forAllInRowSet(rowSet, (key, delta) -> calls.add(new long[] {key, delta}));

            assertEquals(2, calls.size());
            // The forward (positive delta) pass runs first.
            assertEquals(5, calls.get(0)[0]);
            assertEquals(10, calls.get(0)[1]);
            // Then the reverse (negative delta) pass.
            assertEquals(105, calls.get(1)[0]);
            assertEquals(-10, calls.get(1)[1]);
        }
    }

    @Test
    public void testForAllInRowSetExhaustsMidPositiveRun() {
        final RowSetShiftData shiftData = build(
                0, 9, 5,
                20, 29, 5);

        try (final RowSet rowSet = RowSetFactory.fromKeys(22, 25)) {
            final List<long[]> calls = new ArrayList<>();
            shiftData.forAllInRowSet(rowSet, (key, delta) -> calls.add(new long[] {key, delta}));

            // The positive pass iterates shifts in reverse; the rowset exhausts within the [20,29] shift.
            assertEquals(2, calls.size());
            assertEquals(25, calls.get(0)[0]);
            assertEquals(5, calls.get(0)[1]);
            assertEquals(22, calls.get(1)[0]);
            assertEquals(5, calls.get(1)[1]);
        }
    }

    @Test
    public void testForAllInRowSetExhaustsMidNegativeRun() {
        final RowSetShiftData shiftData = build(
                20, 29, -5,
                40, 49, -5);

        try (final RowSet rowSet = RowSetFactory.fromKeys(22, 25)) {
            final List<long[]> calls = new ArrayList<>();
            shiftData.forAllInRowSet(rowSet, (key, delta) -> calls.add(new long[] {key, delta}));

            assertEquals(2, calls.size());
            assertEquals(22, calls.get(0)[0]);
            assertEquals(-5, calls.get(0)[1]);
            assertEquals(25, calls.get(1)[0]);
            assertEquals(-5, calls.get(1)[1]);
        }
    }

    @Test
    public void testForAllInRowSetNoPositiveMatchesNoReverseShifts() {
        // All keys are above the only (positive) shift range; the reverse iterator advance fails immediately and
        // there is no reverse-polarity pass at all.
        final RowSetShiftData shiftData = build(20, 29, 5);
        try (final RowSet rowSet = RowSetFactory.fromKeys(0, 5)) {
            final List<long[]> calls = new ArrayList<>();
            shiftData.forAllInRowSet(rowSet, (key, delta) -> calls.add(new long[] {key, delta}));
            // Keys 0 and 5 are below [20,29]; the reverse iterator positions at 5 which is < 20.
            assertEquals(0, calls.size());
        }
        try (final RowSet rowSet = RowSetFactory.fromKeys(50)) {
            final List<long[]> calls = new ArrayList<>();
            shiftData.forAllInRowSet(rowSet, (key, delta) -> calls.add(new long[] {key, delta}));
            assertEquals(0, calls.size());
        }
    }

    @Test
    public void testForAllInRowSetReversePassAdvanceExhausts() {
        final RowSetShiftData shiftData = build(100, 109, -10);
        try (final RowSet rowSet = RowSetFactory.fromKeys(5)) {
            final List<long[]> calls = new ArrayList<>();
            shiftData.forAllInRowSet(rowSet, (key, delta) -> calls.add(new long[] {key, delta}));
            // The only key is below the negative shift range; advance(100) exhausts the forward iterator.
            assertEquals(0, calls.size());
        }
    }

    // endregion forAllInRowSet

    // region unapply family

    @Test
    public void testUnapplyRoundTripsApply() {
        final RowSetShiftData shiftData = build(
                0, 9, 10,
                50, 59, -10,
                100, 109, 10);
        shiftData.validate();

        try (final WritableRowSet orig = RowSetFactory.fromKeys(0, 1, 2, 3, 4, 5, 8, 52, 55, 100, 105, 200);
                final WritableRowSet shifted = orig.copy()) {
            shiftData.apply(shifted);

            try (final RowSet expectedShifted =
                    RowSetFactory.fromKeys(10, 11, 12, 13, 14, 15, 18, 42, 45, 110, 115, 200)) {
                assertEquals(expectedShifted, shifted);
            }

            shiftData.unapply(shifted);
            assertEquals(orig, shifted);
        }
    }

    @Test
    public void testUnapplyWithOffset() {
        final long offset = 1L << 20;
        final RowSetShiftData shiftData = build(
                0, 9, 10,
                50, 59, -10,
                100, 109, 10);

        try (final WritableRowSet orig = RowSetFactory.fromKeys(0, 1, 5, 8, 52, 55, 100, 105, 200);
                final WritableRowSet inner = orig.copy()) {
            shiftData.apply(inner);
            try (final WritableRowSet postShiftedByOffset = inner.shift(offset);
                    final WritableRowSet expected = orig.shift(offset)) {
                shiftData.unapply(postShiftedByOffset, offset);
                assertEquals(expected, postShiftedByOffset);
            }
        }
    }

    @Test
    public void testStaticUnapplyShift() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(105, 114)) {
            assertTrue(RowSetShiftData.unapplyShift(rowSet, 100, 109, 5));
            try (final RowSet expected = RowSetFactory.fromRange(100, 109)) {
                assertEquals(expected, rowSet);
            }
        }
    }

    @Test
    public void testStaticUnapplyShiftEmptyIntersection() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(0, 9);
                final RowSet expected = RowSetFactory.fromRange(0, 9)) {
            assertFalse(RowSetShiftData.unapplyShift(rowSet, 100, 109, 5));
            assertEquals(expected, rowSet);
        }
    }

    // endregion unapply family

    // region apply(long) and static applyShift

    @Test
    public void testApplySingleKey() {
        final RowSetShiftData shiftData = build(
                10, 19, 5,
                30, 39, -3);

        // Before the first range: already in post-shift space.
        assertEquals(5, shiftData.apply(5));
        // Inside the first range.
        assertEquals(20, shiftData.apply(15));
        // Between the two ranges.
        assertEquals(25, shiftData.apply(25));
        // Inside the second range.
        assertEquals(32, shiftData.apply(35));
        // After all ranges: falls off the end of the loop.
        assertEquals(50, shiftData.apply(50));
        // Empty shift data leaves keys untouched.
        assertEquals(42, RowSetShiftData.EMPTY.apply(42));
    }

    @Test
    public void testStaticApplyShift() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(0, 9)) {
            assertTrue(RowSetShiftData.applyShift(rowSet, 5, 20, 10));
            try (final WritableRowSet expected = RowSetFactory.fromRange(0, 4)) {
                expected.insertRange(15, 19);
                assertEquals(expected, rowSet);
            }
        }
    }

    @Test
    public void testStaticApplyShiftEmptyIntersection() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(0, 9);
                final RowSet expected = RowSetFactory.fromRange(0, 9)) {
            assertFalse(RowSetShiftData.applyShift(rowSet, 100, 200, 5));
            assertEquals(expected, rowSet);
        }
    }

    // endregion apply(long) and static applyShift

    // region effective size

    @Test
    public void testGetEffectiveSize() {
        final RowSetShiftData shiftData = build(
                0, 9, 10,
                100, 109, -10);
        assertEquals(20, shiftData.getEffectiveSize());
        // Second call hits the cache.
        assertEquals(20, shiftData.getEffectiveSize());
        assertEquals(0, RowSetShiftData.EMPTY.getEffectiveSize());
    }

    @Test
    public void testGetEffectiveSizeClamped() {
        // Use a fresh instance so the cache is not yet populated; clamping below the total returns early without
        // caching.
        final RowSetShiftData shiftData = build(
                0, 9, 10,
                100, 109, -10);
        assertEquals(5, shiftData.getEffectiveSizeClamped(5));
        assertEquals(10, shiftData.getEffectiveSizeClamped(10));
        // A clamp above the total computes and caches the full size.
        assertEquals(20, shiftData.getEffectiveSizeClamped(100));
        // Now the cached path is used.
        assertEquals(5, shiftData.getEffectiveSizeClamped(5));
        assertEquals(20, shiftData.getEffectiveSizeClamped(100));
        assertEquals(20, shiftData.getEffectiveSize());
    }

    // endregion effective size

    // region extractParallelShiftedRowsFromPostShiftRowSet

    @Test
    public void testExtractParallelShiftedRows() {
        final RowSetShiftData shiftData = build(
                0, 9, 10,
                100, 109, -10);

        try (final RowSet postShift = RowSetFactory.fromKeys(12, 15, 50, 95);
                final SafeCloseablePair<RowSet, RowSet> pair =
                        shiftData.extractParallelShiftedRowsFromPostShiftRowSet(postShift);
                final RowSet expectedPre = RowSetFactory.fromKeys(2, 5, 105);
                final RowSet expectedPost = RowSetFactory.fromKeys(12, 15, 95)) {
            assertEquals(expectedPre, pair.first);
            assertEquals(expectedPost, pair.second);
        }
    }

    @Test
    public void testExtractParallelShiftedRowsEmptyShiftData() {
        try (final RowSet postShift = RowSetFactory.fromKeys(1, 2, 3);
                final SafeCloseablePair<RowSet, RowSet> pair =
                        RowSetShiftData.EMPTY.extractParallelShiftedRowsFromPostShiftRowSet(postShift)) {
            assertTrue(pair.first.isEmpty());
            assertTrue(pair.second.isEmpty());
        }
    }

    // endregion extractParallelShiftedRowsFromPostShiftRowSet

    // region builder edges

    @Test
    public void testBuilderDegenerateShiftRangeArgs() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(10, 5, 3); // end < begin
        builder.shiftRange(0, 9, 0); // zero delta
        assertFalse(builder.nonempty());
        assertSame(RowSetShiftData.EMPTY, builder.build());
    }

    @Test
    public void testBuilderCoalescesAdjacentSameDelta() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(0, 9, 5);
        builder.shiftRange(10, 19, 5);
        final RowSetShiftData shiftData = builder.build();
        assertEquals(1, shiftData.size());
        assertEquals(0, shiftData.getBeginRange(0));
        assertEquals(19, shiftData.getEndRange(0));
        assertEquals(5, shiftData.getShiftDelta(0));
    }

    @Test
    public void testBuilderOverlapPreShiftThrows() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(0, 9, 5);
        try {
            builder.shiftRange(5, 15, 7);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testBuilderOverlapPostShiftThrows() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(0, 9, 5);
        try {
            builder.shiftRange(10, 19, 3); // post-shift 13 <= 14
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testBuilderLastShiftEnd() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        assertEquals(-1, builder.lastShiftEnd());
        builder.shiftRange(0, 9, 5);
        assertEquals(9, builder.lastShiftEnd());
        builder.shiftRange(20, 29, 5);
        assertEquals(29, builder.lastShiftEnd());
    }

    @Test
    public void testGetMinimumValidBeginForNextDeltaEmpty() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        assertEquals(0, builder.getMinimumValidBeginForNextDelta(5));
        assertEquals(5, builder.getMinimumValidBeginForNextDelta(-5));
    }

    @Test
    public void testGetMinimumValidBeginForNextDeltaNonEmpty() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(0, 9, 5);
        // max(end + 1, end + delta - nextDelta + 1)
        assertEquals(10, builder.getMinimumValidBeginForNextDelta(5));
        assertEquals(17, builder.getMinimumValidBeginForNextDelta(-2));
    }

    @Test
    public void testEquals() {
        final RowSetShiftData a = build(0, 9, 5, 20, 29, -5);
        final RowSetShiftData b = build(0, 9, 5, 20, 29, -5);
        final RowSetShiftData c = build(0, 9, 6);

        assertEquals(a, b);
        assertEquals(b, a);
        assertNotEquals(a, c);
        assertNotEquals(a, RowSetShiftData.EMPTY);
        assertFalse(a.equals("not a RowSetShiftData"));
        assertFalse(a.equals(null));
        assertEquals(RowSetShiftData.EMPTY, RowSetShiftData.EMPTY);
    }

    @Test
    public void testIntersect() {
        final RowSetShiftData shiftData = build(
                0, 9, 10,
                100, 109, -10);

        try (final RowSet hitsFirst = RowSetFactory.fromKeys(5);
                final RowSet hitsNone = RowSetFactory.fromKeys(50);
                final RowSet hitsBoth = RowSetFactory.fromKeys(5, 105)) {
            final RowSetShiftData first = shiftData.intersect(hitsFirst);
            assertEquals(1, first.size());
            assertEquals(0, first.getBeginRange(0));
            assertEquals(9, first.getEndRange(0));
            assertEquals(10, first.getShiftDelta(0));

            assertSame(RowSetShiftData.EMPTY, shiftData.intersect(hitsNone));

            assertEquals(shiftData, shiftData.intersect(hitsBoth));
        }
    }

    // endregion builder edges

    // region appendShiftData

    @Test
    public void testAppendShiftDataTruncatesByCardinality() {
        final RowSetShiftData inner = build(5, 100, 10);

        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.appendShiftData(inner, 1000, 50, 1000, 50);
        final RowSetShiftData result = builder.build();
        result.validate();

        // The inner shift is truncated to the current cardinality: end = min(prevCard - 1, currCard - 1 - delta).
        assertEquals(1, result.size());
        assertEquals(1005, result.getBeginRange(0));
        assertEquals(1039, result.getEndRange(0));
        assertEquals(10, result.getShiftDelta(0));
    }

    @Test
    public void testAppendShiftDataWithOffsetMoveAndNegativeDelta() {
        final RowSetShiftData inner = build(10, 19, -5);

        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.appendShiftData(inner, 0, 50, 100, 50);
        final RowSetShiftData result = builder.build();
        result.validate();

        assertEquals(3, result.size());
        // Keyspace before the inner shift moves by the offset delta; the negative inner delta shrinks the prefix.
        assertEquals(0, result.getBeginRange(0));
        assertEquals(4, result.getEndRange(0));
        assertEquals(100, result.getShiftDelta(0));
        // The inner shift itself is composed with the offset delta.
        assertEquals(10, result.getBeginRange(1));
        assertEquals(19, result.getEndRange(1));
        assertEquals(95, result.getShiftDelta(1));
        // The remaining keyspace moves by the offset delta.
        assertEquals(20, result.getBeginRange(2));
        assertEquals(49, result.getEndRange(2));
        assertEquals(100, result.getShiftDelta(2));
    }

    @Test
    public void testAppendShiftDataInnerShiftBeyondCardinality() {
        // The inner shift begins beyond the previous cardinality; only the prefix shift is emitted.
        final RowSetShiftData inner = build(60, 70, 5);

        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.appendShiftData(inner, 0, 50, 100, 50);
        final RowSetShiftData result = builder.build();
        result.validate();

        assertEquals(1, result.size());
        assertEquals(0, result.getBeginRange(0));
        assertEquals(49, result.getEndRange(0));
        assertEquals(100, result.getShiftDelta(0));
    }

    @Test
    public void testAppendShiftDataEmptyInnerNoOffsetChange() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.appendShiftData(RowSetShiftData.EMPTY, 0, 50, 0, 50);
        assertSame(RowSetShiftData.EMPTY, builder.build());
    }

    @Test
    public void testAppendShiftDataEmptyInnerWithOffsetChange() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.appendShiftData(RowSetShiftData.EMPTY, 0, 50, 200, 50);
        final RowSetShiftData result = builder.build();
        assertEquals(1, result.size());
        assertEquals(0, result.getBeginRange(0));
        assertEquals(49, result.getEndRange(0));
        assertEquals(200, result.getShiftDelta(0));
    }

    // endregion appendShiftData

    // region limitPreviousShiftFor

    @Test
    public void testLimitPreviousShiftForTrimsEnd() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(0, 100, 10);
        builder.limitPreviousShiftFor(50, 10);
        final RowSetShiftData result = builder.build();
        assertEquals(1, result.size());
        assertEquals(0, result.getBeginRange(0));
        assertEquals(49, result.getEndRange(0));
        assertEquals(10, result.getShiftDelta(0));
    }

    @Test
    public void testLimitPreviousShiftForTrimsForPostShiftOverlap() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(0, 100, 10);
        // Pre-shift begin (150) does not overlap, but post-shift begin (105) is within the previous post-shift range.
        builder.limitPreviousShiftFor(150, -45);
        final RowSetShiftData result = builder.build();
        assertEquals(1, result.size());
        assertEquals(0, result.getBeginRange(0));
        assertEquals(94, result.getEndRange(0));
        assertEquals(10, result.getShiftDelta(0));
    }

    @Test
    public void testLimitPreviousShiftForRemovesShiftCompletely() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(10, 20, 5);
        builder.limitPreviousShiftFor(10, 5);
        assertFalse(builder.nonempty());
        assertSame(RowSetShiftData.EMPTY, builder.build());
    }

    @Test
    public void testLimitPreviousShiftForMultiIterationTrim() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(0, 9, 5);
        builder.shiftRange(20, 29, 5);
        // Removes [20,29] entirely, then trims [0,9] down to [0,4].
        builder.limitPreviousShiftFor(5, 5);
        final RowSetShiftData result = builder.build();
        assertEquals(1, result.size());
        assertEquals(0, result.getBeginRange(0));
        assertEquals(4, result.getEndRange(0));
        assertEquals(5, result.getShiftDelta(0));
    }

    @Test
    public void testLimitPreviousShiftForRemovesPolaritySwapIndex() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(10, 19, -5);
        builder.shiftRange(30, 39, 5); // records a polarity swap
        builder.limitPreviousShiftFor(30, 5); // removes the positive shift and its polarity swap index
        builder.shiftRange(30, 39, -5); // same polarity as the remaining shift; no new swap
        final RowSetShiftData result = builder.build();
        result.validate();
        assertEquals(2, result.size());
        assertEquals(10, result.getBeginRange(0));
        assertEquals(19, result.getEndRange(0));
        assertEquals(-5, result.getShiftDelta(0));
        assertEquals(30, result.getBeginRange(1));
        assertEquals(39, result.getEndRange(1));
        assertEquals(-5, result.getShiftDelta(1));

        // The whole set is a single (negative) run; apply order must be ascending.
        final List<Emission> emissions = drain(result.applyIterator());
        assertEquals(2, emissions.size());
        assertEmission(emissions.get(0), 10, 19, -5, false);
        assertEmission(emissions.get(1), 30, 39, -5, false);
    }

    // endregion limitPreviousShiftFor
}
