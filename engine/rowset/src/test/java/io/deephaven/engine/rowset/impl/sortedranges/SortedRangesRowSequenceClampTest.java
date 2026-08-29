//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Coverage-focused tests for the "clamp to the sub-sequence's end" logic in
 * {@code SortedRangesRowSequence.Iterator.updateCurrThrough}.
 *
 * <p>
 * When a {@link RowSequence} is a strict <em>view</em> of a bigger {@link SortedRanges} (obtained via
 * {@code getRowSequenceByKeyRange} / {@code getRowSequenceByPosition}), iterating it with
 * {@code getNextRowSequenceThrough(maxKey)} for a {@code maxKey} beyond the view's last key must stop at the
 * <em>view's</em> end, not at the underlying set's end. The interesting case is a view whose end falls strictly inside
 * one of the underlying ranges: the internal key counting walks whole range elements, so it can overshoot and must be
 * clamped back to the number of keys the view still has (its {@code sizeLeft}).
 *
 * <p>
 * A regression here would silently hand the caller rows <em>outside</em> its slice (an over-read), so every assertion
 * below is made against an independent {@link TreeSet} model of the view.
 *
 * <p>
 * Note that {@code updateCurrThrough} has six such clamp sites. Three of them (the ones inside the range-walking loop,
 * reached when {@code maxKey} lands on a range end or on a singleton at index {@code i <= rsEndIdx}) look to be
 * defensive only: the overshoot being guarded against can only be produced by the <em>last</em> element of the view,
 * and that element is either handled by the pre-clamp of {@code packedToKey} against {@code rsEndOffset} or by the
 * clamp after the loop. The sweep below drives every view of a mixed shape through many key sequences without reaching
 * them.
 */
public class SortedRangesRowSequenceClampTest {

    /** {10-20, 30-40, 45, 50-60} — two multi-key ranges, a singleton, and a trailing multi-key range. */
    private static final long[] SHAPE = {10, 20, 30, 40, 45, 45, 50, 60};

    private static final long UNDERLYING_LAST_KEY = 60;

    // -----------------------------------------------------------------------------------------------------------------
    // Helpers.
    // -----------------------------------------------------------------------------------------------------------------

    /** Build a {@link SortedRanges}-backed row set from inclusive {@code [start, end]} pairs. */
    private static WritableRowSetImpl rowSetOf(final long... rangePairs) {
        assertEquals("rangePairs must be pairs", 0, rangePairs.length % 2);
        SortedRanges sr = SortedRanges.makeSingleRange(rangePairs[0], rangePairs[1]);
        assertNotNull(sr);
        for (int i = 2; i < rangePairs.length; i += 2) {
            if (rangePairs[i] == rangePairs[i + 1]) {
                sr = sr.add(rangePairs[i]);
            } else {
                sr = sr.addRange(rangePairs[i], rangePairs[i + 1]);
            }
            assertNotNull("SortedRanges overflowed while building the fixture", sr);
        }
        final WritableRowSetImpl rowSet = new WritableRowSetImpl(sr.ixCowRef());
        assertTrue("fixture must be SortedRanges backed", rowSet.getInnerSet() instanceof SortedRanges);
        return rowSet;
    }

    /**
     * Build a row set backed by one of the <em>packed</em> {@link SortedRanges} flavors (a nonzero base offset, keys
     * stored as shorts/ints), so the clamp arithmetic is also exercised against {@code pack}ed values.
     */
    private static WritableRowSetImpl packedRowSetOf(final long... rangePairs) {
        SortedRanges sr =
                SortedRanges.makeForKnownRange(rangePairs[0], rangePairs[rangePairs.length - 1], true);
        assertNotNull(sr);
        for (int i = 0; i < rangePairs.length; i += 2) {
            if (rangePairs[i] == rangePairs[i + 1]) {
                sr = sr.add(rangePairs[i]);
            } else {
                sr = sr.addRange(rangePairs[i], rangePairs[i + 1]);
            }
            assertNotNull("SortedRanges overflowed while building the fixture", sr);
        }
        final WritableRowSetImpl rowSet = new WritableRowSetImpl(sr.ixCowRef());
        assertFalse("fixture must use a packed SortedRanges flavor",
                rowSet.getInnerSet() instanceof SortedRangesLong);
        assertTrue("fixture must be SortedRanges backed", rowSet.getInnerSet() instanceof SortedRanges);
        return rowSet;
    }

    private static long[] modelOf(final long... rangePairs) {
        final NavigableSet<Long> model = new TreeSet<>();
        for (int i = 0; i < rangePairs.length; i += 2) {
            for (long v = rangePairs[i]; v <= rangePairs[i + 1]; ++v) {
                model.add(v);
            }
        }
        return toArray(model);
    }

    private static long[] toArray(final NavigableSet<Long> model) {
        return model.stream().mapToLong(Long::longValue).toArray();
    }

    /** The keys of {@code all} that fall in {@code [start, end]}, as a {@link TreeSet} derived model. */
    private static long[] modelSliceByKeyRange(final long[] all, final long start, final long end) {
        final NavigableSet<Long> model = new TreeSet<>();
        for (final long v : all) {
            if (v >= start && v <= end) {
                model.add(v);
            }
        }
        return toArray(model);
    }

    private static long[] modelSliceByPosition(final long[] all, final long pos, final long length) {
        final NavigableSet<Long> model = new TreeSet<>();
        for (long p = pos; p < pos + length && p < all.length; ++p) {
            if (p >= 0) {
                model.add(all[(int) p]);
            }
        }
        return toArray(model);
    }

    private static long[] keysOf(final RowSequence rs) {
        final List<Long> keys = new ArrayList<>();
        assertTrue(rs.forEachRowKey(v -> {
            keys.add(v);
            return true;
        }));
        final long[] out = new long[keys.size()];
        for (int i = 0; i < out.length; ++i) {
            out[i] = keys.get(i);
        }
        return out;
    }

    private static void assertViewKeys(final String tag, final RowSequence view, final long[] model) {
        assertTrue(tag, view instanceof SortedRangesRowSequence);
        assertEquals(tag + " size", model.length, view.size());
        assertArrayEquals(tag + " keys", model, keysOf(view));
        assertEquals(tag + " firstRowKey", model[0], view.firstRowKey());
        assertEquals(tag + " lastRowKey", model[model.length - 1], view.lastRowKey());
        ((SortedRangesRowSequence) view).validate();
    }

    /**
     * Drive {@code view.getRowSequenceIterator()} with the given sequence of {@code getNextRowSequenceThrough} keys,
     * asserting every returned sub-sequence against the model; then drain with {@link Long#MAX_VALUE} and assert the
     * iterator ends exactly at the view's last key.
     */
    private static void drive(final String tag, final RowSequence view, final long[] model, final long... maxKeys) {
        final long viewLastKey = model[model.length - 1];
        int consumed = 0;
        try (final RowSequence.Iterator it = view.getRowSequenceIterator()) {
            for (final long maxKey : maxKeys) {
                final String m = tag + " maxKey=" + maxKey + " consumed=" + consumed;
                assertEquals(m + " hasMore", consumed < model.length, it.hasMore());
                if (consumed < model.length) {
                    assertEquals(m + " peekNextKey", model[consumed], it.peekNextKey());
                }
                int end = consumed;
                while (end < model.length && model[end] <= maxKey) {
                    ++end;
                }
                final long[] expected = Arrays.copyOfRange(model, consumed, end);
                final RowSequence rs = it.getNextRowSequenceThrough(maxKey);
                assertEquals(m + " sub size", expected.length, rs.size());
                assertArrayEquals(m + " sub keys", expected, keysOf(rs));
                if (expected.length > 0) {
                    assertEquals(m + " sub firstRowKey", expected[0], rs.firstRowKey());
                    assertEquals(m + " sub lastRowKey", expected[expected.length - 1], rs.lastRowKey());
                    assertTrue(m + " must not over-read past the view", rs.lastRowKey() <= viewLastKey);
                } else {
                    assertTrue(m + " empty", rs.isEmpty());
                }
                consumed = end;
                assertEquals(m + " relativePosition", consumed - model.length, it.getRelativePosition());
            }

            // Drain: whatever is left must come back in a single sequence ending exactly at the view's last key.
            final long[] rest = Arrays.copyOfRange(model, consumed, model.length);
            final RowSequence tail = it.getNextRowSequenceThrough(Long.MAX_VALUE);
            assertArrayEquals(tag + " drain keys (consumed=" + consumed + ")", rest, keysOf(tail));
            if (rest.length > 0) {
                assertEquals(tag + " drain lastRowKey", viewLastKey, tail.lastRowKey());
            }
            assertFalse(tag + " exhausted", it.hasMore());
            assertEquals(tag + " exhausted peekNextKey", RowSequence.NULL_ROW_KEY, it.peekNextKey());
            assertTrue(tag + " past the end", it.getNextRowSequenceThrough(Long.MAX_VALUE).isEmpty());
            assertTrue(tag + " past the end (underlying last)",
                    it.getNextRowSequenceThrough(UNDERLYING_LAST_KEY).isEmpty());
        }
    }

    /**
     * Like {@link #drive}, but consumes {@code lengths} keys at a time via {@code getNextRowSequenceWithLength} before
     * the final {@code getNextRowSequenceThrough} drain, so that the clamp is exercised from iterator states produced
     * by the length-based path.
     */
    private static void driveWithLengthsThenThrough(final String tag, final RowSequence view, final long[] model,
            final long throughKey, final long... lengths) {
        final long viewLastKey = model[model.length - 1];
        int consumed = 0;
        try (final RowSequence.Iterator it = view.getRowSequenceIterator()) {
            for (final long length : lengths) {
                final int end = (int) Math.min(model.length, consumed + length);
                final long[] expected = Arrays.copyOfRange(model, consumed, end);
                final RowSequence rs = it.getNextRowSequenceWithLength(length);
                assertArrayEquals(tag + " len=" + length + " consumed=" + consumed, expected, keysOf(rs));
                consumed = end;
            }
            int end = consumed;
            while (end < model.length && model[end] <= throughKey) {
                ++end;
            }
            final long[] expected = Arrays.copyOfRange(model, consumed, end);
            final RowSequence rs = it.getNextRowSequenceThrough(throughKey);
            assertArrayEquals(tag + " through=" + throughKey + " consumed=" + consumed, expected, keysOf(rs));
            if (expected.length > 0) {
                assertTrue(tag + " must not over-read past the view", rs.lastRowKey() <= viewLastKey);
            }
            consumed = end;
            final long[] rest = Arrays.copyOfRange(model, consumed, model.length);
            assertArrayEquals(tag + " drain", rest, keysOf(it.getNextRowSequenceThrough(Long.MAX_VALUE)));
            assertFalse(tag + " exhausted", it.hasMore());
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Targeted cases: the view's end lands (a) mid-range, (b) on a range end, (c) on a singleton, (d) on the
    // underlying set's last key.
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * The whole view lives inside a single underlying range and its end is strictly inside that range, so a
     * {@code maxKey} that is still inside the same range must clamp to the view's end on the very first call.
     */
    @Test
    public void testViewInsideOneRangeClampsOnFirstCall() {
        final long[] all = modelOf(SHAPE);
        try (final WritableRowSetImpl rowSet = rowSetOf(SHAPE)) {
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(13, 15)) {
                final long[] model = modelSliceByKeyRange(all, 13, 15);
                assertViewKeys("insideRange", view, model);
                // maxKey inside the underlying range 10-20 but past the view's end (15).
                drive("insideRange/16", view, model, 16);
                drive("insideRange/18", view, model, 18);
                // maxKey exactly at the underlying range's end.
                drive("insideRange/20", view, model, 20);
                // maxKey past the underlying range, and past everything.
                drive("insideRange/30", view, model, 30);
                drive("insideRange/last", view, model, UNDERLYING_LAST_KEY);
                drive("insideRange/max", view, model, Long.MAX_VALUE);
                // Incremental: the clamp must also fire on a non-first call.
                drive("insideRange/incremental", view, model, 13, 14, 19, Long.MAX_VALUE);
                drive("insideRange/incrementalInRange", view, model, 14, 18);
                driveWithLengthsThenThrough("insideRange/lengths", view, model, 19, 1, 1);
            }
            // A single-key view whose key is strictly inside a range.
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(35, 35)) {
                final long[] model = modelSliceByKeyRange(all, 35, 35);
                assertViewKeys("singleKeyInRange", view, model);
                drive("singleKeyInRange/38", view, model, 38);
                drive("singleKeyInRange/40", view, model, 40);
                drive("singleKeyInRange/max", view, model, Long.MAX_VALUE);
            }
        }
    }

    /** (a) The view's end lands strictly inside an underlying range. */
    @Test
    public void testViewEndMidRange() {
        final long[] all = modelOf(SHAPE);
        try (final WritableRowSetImpl rowSet = rowSetOf(SHAPE)) {
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(13, 35)) {
                final long[] model = modelSliceByKeyRange(all, 13, 35);
                assertViewKeys("midRange", view, model);
                assertEquals(35, view.lastRowKey());
                drive("midRange/max", view, model, Long.MAX_VALUE);
                drive("midRange/last", view, model, UNDERLYING_LAST_KEY);
                // maxKey past the view's end but inside the same underlying range (30-40).
                drive("midRange/36", view, model, 36);
                drive("midRange/40", view, model, 40);
                // Incremental drives whose final call must clamp.
                drive("midRange/inc1", view, model, 20, Long.MAX_VALUE);
                drive("midRange/inc2", view, model, 20, 32, Long.MAX_VALUE);
                drive("midRange/inc3", view, model, 20, 32, 38);
                drive("midRange/inc4", view, model, 15, 20, 30, 34, 45);
                drive("midRange/inc5", view, model, 19, 34, UNDERLYING_LAST_KEY);
                driveWithLengthsThenThrough("midRange/lengths1", view, model, Long.MAX_VALUE, 1);
                driveWithLengthsThenThrough("midRange/lengths2", view, model, 41, 8, 4);
                driveWithLengthsThenThrough("midRange/lengths3", view, model, 39, 12);
                driveWithLengthsThenThrough("midRange/lengths4", view, model, 45, 13, 1);
            }
            // A view that starts and ends inside the trailing range.
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(13, 55)) {
                final long[] model = modelSliceByKeyRange(all, 13, 55);
                assertViewKeys("midTrailingRange", view, model);
                drive("midTrailingRange/max", view, model, Long.MAX_VALUE);
                // The clamp fires on the fourth call, from a start inside the trailing range.
                drive("midTrailingRange/inc", view, model, 20, 40, 45, 58);
                drive("midTrailingRange/inc2", view, model, 20, 40, 45, UNDERLYING_LAST_KEY);
                drive("midTrailingRange/inc3", view, model, 33, 52, 57);
            }
        }
    }

    /** (b) The view's end lands exactly on an underlying range's end. */
    @Test
    public void testViewEndOnRangeEnd() {
        final long[] all = modelOf(SHAPE);
        try (final WritableRowSetImpl rowSet = rowSetOf(SHAPE)) {
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(13, 40)) {
                final long[] model = modelSliceByKeyRange(all, 13, 40);
                assertViewKeys("rangeEnd", view, model);
                assertEquals(40, view.lastRowKey());
                drive("rangeEnd/max", view, model, Long.MAX_VALUE);
                drive("rangeEnd/41", view, model, 41);
                drive("rangeEnd/45", view, model, 45);
                drive("rangeEnd/last", view, model, UNDERLYING_LAST_KEY);
                drive("rangeEnd/inc", view, model, 20, 30, 39, Long.MAX_VALUE);
                drive("rangeEnd/inc2", view, model, 18, 44);
                driveWithLengthsThenThrough("rangeEnd/lengths", view, model, Long.MAX_VALUE, 3, 7);
            }
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(30, 40)) {
                final long[] model = modelSliceByKeyRange(all, 30, 40);
                assertViewKeys("wholeRange", view, model);
                drive("wholeRange/max", view, model, Long.MAX_VALUE);
                drive("wholeRange/inc", view, model, 35, 46);
            }
        }
    }

    /** (c) The view's end lands on a singleton. */
    @Test
    public void testViewEndOnSingleton() {
        final long[] all = modelOf(SHAPE);
        try (final WritableRowSetImpl rowSet = rowSetOf(SHAPE)) {
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(13, 45)) {
                final long[] model = modelSliceByKeyRange(all, 13, 45);
                assertViewKeys("singleton", view, model);
                assertEquals(45, view.lastRowKey());
                drive("singleton/max", view, model, Long.MAX_VALUE);
                drive("singleton/46", view, model, 46);
                drive("singleton/50", view, model, 50);
                drive("singleton/last", view, model, UNDERLYING_LAST_KEY);
                drive("singleton/inc", view, model, 20, 38, 44, Long.MAX_VALUE);
                drive("singleton/inc2", view, model, 20, 40, 55);
                driveWithLengthsThenThrough("singleton/lengths", view, model, 52, 9, 9);
            }
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(41, 45)) {
                final long[] model = modelSliceByKeyRange(all, 41, 45);
                assertViewKeys("singletonOnly", view, model);
                assertEquals(1, view.size());
                drive("singletonOnly/max", view, model, Long.MAX_VALUE);
                drive("singletonOnly/45", view, model, 45);
            }
        }
    }

    /** (d) The view's end lands exactly on the underlying set's last key. */
    @Test
    public void testViewEndOnUnderlyingLastKey() {
        final long[] all = modelOf(SHAPE);
        try (final WritableRowSetImpl rowSet = rowSetOf(SHAPE)) {
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(13, UNDERLYING_LAST_KEY)) {
                final long[] model = modelSliceByKeyRange(all, 13, UNDERLYING_LAST_KEY);
                assertViewKeys("lastKey", view, model);
                drive("lastKey/max", view, model, Long.MAX_VALUE);
                drive("lastKey/last", view, model, UNDERLYING_LAST_KEY);
                drive("lastKey/inc", view, model, 20, 45, Long.MAX_VALUE);
                drive("lastKey/inc2", view, model, 19, 40, 59, 61);
                driveWithLengthsThenThrough("lastKey/lengths", view, model, Long.MAX_VALUE, 5, 5, 5);
            }
            try (final RowSequence view = rowSet.getRowSequenceByKeyRange(55, UNDERLYING_LAST_KEY)) {
                final long[] model = modelSliceByKeyRange(all, 55, UNDERLYING_LAST_KEY);
                assertViewKeys("tailOnly", view, model);
                drive("tailOnly/max", view, model, Long.MAX_VALUE);
                drive("tailOnly/inc", view, model, 57, 100);
            }
        }
    }

    /** The same four end placements, but for views built with {@code getRowSequenceByPosition}. */
    @Test
    public void testByPositionViews() {
        final long[] all = modelOf(SHAPE);
        // Positions: 0-10 -> 10-20, 11-21 -> 30-40, 22 -> 45, 23-33 -> 50-60.
        final long[][] posLen = {
                {0, 3}, // ends mid-range 10-20.
                {3, 3}, // starts and ends mid-range 10-20.
                {0, 11}, // ends on range end 20.
                {3, 20}, // ends mid-range 30-40 (key 50 excluded).
                {3, 10}, // ends mid-range 30-40.
                {5, 18}, // ends on the singleton 45.
                {11, 12}, // starts on a range start, ends on the singleton.
                {22, 1}, // the singleton alone.
                {22, 12}, // singleton through the underlying last key.
                {13, 21}, // ends on the underlying last key.
                {0, 34}, // the whole set.
                {0, 100}, // length past the end.
                {24, 5}, // inside the trailing range.
        };
        try (final WritableRowSetImpl rowSet = rowSetOf(SHAPE)) {
            for (final long[] pl : posLen) {
                final long pos = pl[0];
                final long len = pl[1];
                final String tag = "byPos(" + pos + "," + len + ")";
                final long[] model = modelSliceByPosition(all, pos, len);
                try (final RowSequence view = rowSet.getRowSequenceByPosition(pos, len)) {
                    assertViewKeys(tag, view, model);
                    final long viewLast = model[model.length - 1];
                    drive(tag + "/max", view, model, Long.MAX_VALUE);
                    drive(tag + "/last", view, model, UNDERLYING_LAST_KEY);
                    // A maxKey just past the view's end, and one further past it.
                    drive(tag + "/+1", view, model, viewLast + 1);
                    drive(tag + "/+5", view, model, viewLast + 5);
                    // Incremental: stop inside the view, then clamp.
                    drive(tag + "/inc", view, model, model[model.length / 2], Long.MAX_VALUE);
                    drive(tag + "/inc2", view, model, model[0], viewLast + 3);
                    drive(tag + "/inc3", view, model, model[model.length / 2], viewLast + 2, Long.MAX_VALUE);
                    driveWithLengthsThenThrough(tag + "/lengths", view, model, Long.MAX_VALUE, 1, 2);
                    driveWithLengthsThenThrough(tag + "/lengths2", view, model, viewLast + 4, 1);
                }
            }
        }
    }

    /** Views of views: the clamp must respect the innermost view's end. */
    @Test
    public void testNestedViewsClamp() {
        final long[] all = modelOf(SHAPE);
        try (final WritableRowSetImpl rowSet = rowSetOf(SHAPE)) {
            try (final RowSequence outer = rowSet.getRowSequenceByKeyRange(13, 55)) {
                final long[] outerModel = modelSliceByKeyRange(all, 13, 55);
                assertViewKeys("outer", outer, outerModel);
                try (final RowSequence inner = outer.getRowSequenceByKeyRange(18, 35)) {
                    final long[] innerModel = modelSliceByKeyRange(all, 18, 35);
                    assertViewKeys("outer/innerByKey", inner, innerModel);
                    drive("outer/innerByKey/max", inner, innerModel, Long.MAX_VALUE);
                    drive("outer/innerByKey/38", inner, innerModel, 38);
                    drive("outer/innerByKey/inc", inner, innerModel, 20, 33, 39);
                }
                try (final RowSequence inner = outer.getRowSequenceByPosition(5, 10)) {
                    final long[] innerModel = modelSliceByPosition(outerModel, 5, 10);
                    assertViewKeys("outer/innerByPos", inner, innerModel);
                    drive("outer/innerByPos/max", inner, innerModel, Long.MAX_VALUE);
                    drive("outer/innerByPos/inc", inner, innerModel,
                            innerModel[3], innerModel[innerModel.length - 1] + 2);
                }
            }
        }
    }

    /**
     * The same clamp scenarios against a packed (offset-based) SortedRanges flavor, whose keys live far from zero so
     * that {@code pack}/{@code unpack} is not the identity.
     */
    @Test
    public void testPackedFlavorViewsClamp() {
        final long base = 1_000_000L;
        final long[] shape = {base + 10, base + 20, base + 30, base + 40, base + 45, base + 45, base + 50, base + 60};
        final long[] all = modelOf(shape);
        try (final WritableRowSetImpl rowSet = packedRowSetOf(shape)) {
            // (a) mid-range end, (b) range end, (c) singleton, (d) underlying last key.
            final long[][] keyRanges = {
                    {base + 13, base + 15},
                    {base + 13, base + 35},
                    {base + 13, base + 40},
                    {base + 13, base + 45},
                    {base + 13, base + 60},
                    {base + 35, base + 55},
            };
            for (final long[] kr : keyRanges) {
                final String tag = "packed(" + kr[0] + "," + kr[1] + ")";
                final long[] model = modelSliceByKeyRange(all, kr[0], kr[1]);
                try (final RowSequence view = rowSet.getRowSequenceByKeyRange(kr[0], kr[1])) {
                    assertViewKeys(tag, view, model);
                    final long viewLast = model[model.length - 1];
                    drive(tag + "/max", view, model, Long.MAX_VALUE);
                    drive(tag + "/last", view, model, base + 60);
                    drive(tag + "/+1", view, model, viewLast + 1);
                    drive(tag + "/+5", view, model, viewLast + 5);
                    drive(tag + "/inc", view, model, model[model.length / 2], viewLast + 2, Long.MAX_VALUE);
                    driveWithLengthsThenThrough(tag + "/lengths", view, model, viewLast + 3, 1, 2);
                }
            }
            for (final long[] pl : new long[][] {{0, 3}, {3, 10}, {5, 18}, {13, 21}, {22, 12}}) {
                final String tag = "packedByPos(" + pl[0] + "," + pl[1] + ")";
                final long[] model = modelSliceByPosition(all, pl[0], pl[1]);
                try (final RowSequence view = rowSet.getRowSequenceByPosition(pl[0], pl[1])) {
                    assertViewKeys(tag, view, model);
                    final long viewLast = model[model.length - 1];
                    drive(tag + "/max", view, model, Long.MAX_VALUE);
                    drive(tag + "/+2", view, model, viewLast + 2);
                    drive(tag + "/inc", view, model, model[0], viewLast + 4);
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Systematic sweep.
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Sweep every key-range and position view of a mixed-shape SortedRanges, driving each with several
     * {@code getNextRowSequenceThrough} sequences and comparing against a TreeSet model. This covers the clamp for
     * every relative placement of the view's end (mid-range, range end, singleton, set end) and for both first and
     * subsequent calls.
     */
    @Test
    public void testSweepAllViewsAndThroughSequences() {
        final long[] shape = {2, 6, 9, 9, 12, 15, 18, 19, 22, 22, 25, 30};
        final long[] all = modelOf(shape);
        final long lastKey = all[all.length - 1];
        final long[] candidates = {0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 14, 15, 16, 18, 19, 20, 22, 23,
                25, 27, 29, 30, 31, 33, lastKey + 100, Long.MAX_VALUE};
        final long[] coarse = {1, 5, 9, 13, 18, 22, 26, 30, 32, Long.MAX_VALUE};

        try (final WritableRowSetImpl rowSet = rowSetOf(shape)) {
            // Key-range views.
            for (final long s : candidates) {
                if (s > lastKey + 2) {
                    continue;
                }
                for (final long e : candidates) {
                    if (e < s || e > lastKey + 2) {
                        continue;
                    }
                    final long[] model = modelSliceByKeyRange(all, s, e);
                    try (final RowSequence view = rowSet.getRowSequenceByKeyRange(s, e)) {
                        final String tag = "sweepKeys(" + s + "," + e + ")";
                        if (model.length == 0) {
                            assertTrue(tag, view.isEmpty());
                            continue;
                        }
                        assertViewKeys(tag, view, model);
                        for (final long t : candidates) {
                            drive(tag + "/t=" + t, view, model, t);
                        }
                        for (final long t1 : coarse) {
                            for (final long t2 : coarse) {
                                drive(tag + "/t=" + t1 + "," + t2, view, model, t1, t2);
                            }
                        }
                        for (final long len : new long[] {1, 2, 3, 7}) {
                            for (final long t : coarse) {
                                driveWithLengthsThenThrough(tag + "/len=" + len + "/t=" + t, view, model, t, len, len);
                            }
                        }
                    }
                }
            }

            // Position views.
            final long card = all.length;
            for (long pos = 0; pos <= card; ++pos) {
                for (long len = 1; len <= card + 1 - Math.min(pos, card); ++len) {
                    final long[] model = modelSliceByPosition(all, pos, len);
                    try (final RowSequence view = rowSet.getRowSequenceByPosition(pos, len)) {
                        final String tag = "sweepPos(" + pos + "," + len + ")";
                        if (model.length == 0) {
                            assertTrue(tag, view.isEmpty());
                            continue;
                        }
                        assertViewKeys(tag, view, model);
                        for (final long t : coarse) {
                            drive(tag + "/t=" + t, view, model, t);
                        }
                        final long viewLast = model[model.length - 1];
                        drive(tag + "/justPast", view, model, viewLast + 1);
                        drive(tag + "/past", view, model, viewLast + 3);
                        drive(tag + "/incPast", view, model, model[0], viewLast + 2, Long.MAX_VALUE);
                        driveWithLengthsThenThrough(tag + "/lengths", view, model, viewLast + 2, 1, 2);
                    }
                }
            }
        }
    }
}
