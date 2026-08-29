//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@code invert*} family of {@link SortedRanges}: {@code ixInvertOnNew} must throw
 * {@link IllegalArgumentException} when the {@code keys} argument contains any key not present in the receiver, and
 * must otherwise return the positions of those keys (truncated at {@code maximumPosition}).
 * <p>
 * Both operand shapes are covered: a {@link SingleRange} {@code keys} argument (which dispatches to
 * {@code invertRangeOnNew}) and a multi-range {@code keys} argument, i.e. {@link SortedRanges} or {@link RspBitmap}
 * (which dispatches to {@code invertOnNew}).
 */
public class SortedRangesInvertTest {

    private static final String KEYS_NOT_PRESENT_MSG = "keys argument has elements not in the rowSet";

    /** The receiver used by most tests: {10, 20-30, 50}. */
    private static final long[] RECEIVER_PAIRS = new long[] {10, 10, 20, 30, 50, 50};

    // region helpers

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

    private static SortedRanges receiver() {
        return sr(RECEIVER_PAIRS);
    }

    /** All keys of the receiver, in ascending order; index in this array == position. */
    private static long[] keysInOrder(final long... startEndPairs) {
        final LongArrayList ans = new LongArrayList();
        for (int i = 0; i < startEndPairs.length; i += 2) {
            for (long v = startEndPairs[i]; v <= startEndPairs[i + 1]; ++v) {
                ans.add(v);
            }
        }
        return ans.toLongArray();
    }

    /**
     * The model answer: for each key (ascending) look up its position in the receiver's key order, and keep only
     * positions {@code <= maximumPosition}. Fails the test if any key is absent (i.e. this helper may only be used for
     * the success cases).
     */
    private static long[] modelPositions(final long maximumPosition, final long... keyPairs) {
        final long[] order = keysInOrder(RECEIVER_PAIRS);
        final LongArrayList ans = new LongArrayList();
        for (int i = 0; i < keyPairs.length; i += 2) {
            for (long v = keyPairs[i]; v <= keyPairs[i + 1]; ++v) {
                final int pos = Arrays.binarySearch(order, v);
                assertTrue("key " + v + " must be present in the receiver for a success case", pos >= 0);
                if (pos <= maximumPosition) {
                    ans.add(pos);
                }
            }
        }
        return ans.toLongArray();
    }

    /** Consumes {@code ols} (which must be a freshly created "OnNew" result or a cowRef). */
    private static long[] toArray(final OrderedLongSet ols) {
        assertNotNull(ols);
        ols.ixValidate();
        final LongArrayList ans = new LongArrayList();
        try (final RowSet rs = new WritableRowSetImpl(ols)) {
            rs.forAllRowKeys(ans::add);
        }
        return ans.toLongArray();
    }

    /**
     * Assert that inverting {@code keys} throws with the documented message, for all of: the {@link SingleRange}
     * dispatch (when {@code keys} is a single range), the {@link SortedRanges} dispatch and the {@link RspBitmap}
     * dispatch.
     */
    private static void assertKeysNotPresent(final long... keyPairs) {
        if (keyPairs.length == 2) {
            assertKeysNotPresent(SingleRange.make(keyPairs[0], keyPairs[1]),
                    "SingleRange" + Arrays.toString(keyPairs));
        }
        assertKeysNotPresent(sr(keyPairs), "SortedRanges" + Arrays.toString(keyPairs));
        assertKeysNotPresent(rsp(keyPairs), "RspBitmap" + Arrays.toString(keyPairs));
    }

    private static void assertKeysNotPresent(final OrderedLongSet keys, final String msg) {
        final SortedRanges s = receiver();
        try {
            final OrderedLongSet r = s.ixInvertOnNew(keys, Long.MAX_VALUE);
            fail(msg + ": expected IllegalArgumentException, got " + r);
        } catch (IllegalArgumentException e) {
            assertEquals(msg, KEYS_NOT_PRESENT_MSG, e.getMessage());
        }
        // The same through the public RowSet API.
        try (final RowSet rs = new WritableRowSetImpl(s.ixCowRef());
                final RowSet keysRs = new WritableRowSetImpl(keys.ixCowRef())) {
            try (final RowSet ignored = rs.invert(keysRs)) {
                fail(msg + ": expected IllegalArgumentException from RowSet.invert");
            } catch (IllegalArgumentException e) {
                assertEquals(msg, KEYS_NOT_PRESENT_MSG, e.getMessage());
            }
        }
    }

    /**
     * Assert the inverted positions of {@code keyPairs} (all of which must be present in the receiver) against the
     * model, for each of the SingleRange / SortedRanges / RspBitmap dispatches.
     */
    private static void assertInverts(final long maximumPosition, final long... keyPairs) {
        final long[] expected = modelPositions(maximumPosition, keyPairs);
        if (keyPairs.length == 2) {
            assertInverts(SingleRange.make(keyPairs[0], keyPairs[1]), maximumPosition, expected,
                    "SingleRange" + Arrays.toString(keyPairs) + " maxPos=" + maximumPosition);
        }
        assertInverts(sr(keyPairs), maximumPosition, expected,
                "SortedRanges" + Arrays.toString(keyPairs) + " maxPos=" + maximumPosition);
        assertInverts(rsp(keyPairs), maximumPosition, expected,
                "RspBitmap" + Arrays.toString(keyPairs) + " maxPos=" + maximumPosition);
    }

    private static void assertInverts(
            final OrderedLongSet keys, final long maximumPosition, final long[] expected, final String msg) {
        final SortedRanges s = receiver();
        assertArrayEquals(msg, expected, toArray(s.ixInvertOnNew(keys, maximumPosition)));
        // The same through the public RowSet API.
        try (final RowSet rs = new WritableRowSetImpl(s.ixCowRef());
                final RowSet keysRs = new WritableRowSetImpl(keys.ixCowRef());
                final RowSet result = rs.invert(keysRs, maximumPosition)) {
            final LongArrayList actual = new LongArrayList();
            result.forAllRowKeys(actual::add);
            assertArrayEquals(msg + " (RowSet.invert)", expected, actual.toLongArray());
        }
    }

    // endregion helpers

    @Test
    public void testReceiverShape() {
        final SortedRanges s = receiver();
        assertArrayEquals(new long[] {10, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 50}, toArray(s.ixCowRef()));
        assertEquals(13, s.getCardinality());
    }

    // region keys not present

    @Test
    public void testInvertKeysExtendPastRangeEnd() {
        // 25-35 starts inside 20-30 but extends past its end.
        assertKeysNotPresent(25, 35);
        assertKeysNotPresent(30, 31);
        assertKeysNotPresent(20, 40);
    }

    @Test
    public void testInvertKeysStartAtTrailingSingletonAndExtendPast() {
        // 50 is the last key of the receiver; 51 is absent and there is nothing after it in the array.
        assertKeysNotPresent(50, 51);
        assertKeysNotPresent(50, 60);
    }

    @Test
    public void testInvertKeysStartAtSingletonAndExtendIntoGap() {
        // 10 is a singleton in the receiver; 11 is absent and the next array entry is a positive (range start) value.
        assertKeysNotPresent(10, 11);
        assertKeysNotPresent(10, 19);
        assertKeysNotPresent(10, 20);
    }

    @Test
    public void testInvertKeysSpanRangeEndIntoGap() {
        // 20-31 covers a full receiver range and one key past its end.
        assertKeysNotPresent(20, 31);
        assertKeysNotPresent(29, 31);
    }

    @Test
    public void testInvertKeysEntirelyOutside() {
        // Above everything.
        assertKeysNotPresent(60, 61);
        assertKeysNotPresent(51, 51);
        // Below everything.
        assertKeysNotPresent(1, 2);
        assertKeysNotPresent(9, 9);
        // Inside a gap.
        assertKeysNotPresent(15, 16);
        assertKeysNotPresent(31, 31);
        assertKeysNotPresent(31, 49);
        assertKeysNotPresent(40, 40);
    }

    @Test
    public void testInvertKeysAbsentInTheMiddle() {
        // Multi-range keys where an interior range is absent from the receiver; the leading ranges are present, so the
        // failure can only be discovered part way through the iteration.
        assertKeysNotPresent(10, 10, 15, 15, 20, 20);
        assertKeysNotPresent(20, 21, 40, 40, 50, 50);
        assertKeysNotPresent(10, 10, 20, 21, 31, 31);
        assertKeysNotPresent(20, 20, 25, 26, 45, 45, 50, 50);
        // Trailing absent range, after a fully-present prefix.
        assertKeysNotPresent(10, 10, 20, 30, 50, 51);
        assertKeysNotPresent(20, 20, 50, 51);
        assertKeysNotPresent(20, 20, 60, 61);
        // Leading absent range, before a present suffix.
        assertKeysNotPresent(5, 5, 20, 20);
    }

    @Test
    public void testInvertKeysNotPresentWithBoundedMaximumPosition() {
        // A maximumPosition big enough that the absent key is still visited: the exception must still be thrown.
        // (Note the implementations are documented as "stopping at maximumPosition", so an absent key beyond
        // maximumPosition is not necessarily detected; that is not asserted here.)
        final SortedRanges s = receiver();
        for (final OrderedLongSet keys : new OrderedLongSet[] {
                SingleRange.make(25, 35), sr(25, 35), rsp(25, 35), sr(20, 21, 40, 40), rsp(20, 21, 40, 40)}) {
            try {
                final OrderedLongSet r = s.ixInvertOnNew(keys, 12);
                fail("expected IllegalArgumentException, got " + r);
            } catch (IllegalArgumentException e) {
                assertEquals(KEYS_NOT_PRESENT_MSG, e.getMessage());
            }
        }
    }

    // endregion keys not present

    // region success paths

    @Test
    public void testInvertExactSingleKey() {
        // Leading singleton, interior range start / middle / end, trailing singleton.
        assertInverts(Long.MAX_VALUE, 10, 10);
        assertInverts(Long.MAX_VALUE, 20, 20);
        assertInverts(Long.MAX_VALUE, 25, 25);
        assertInverts(Long.MAX_VALUE, 30, 30);
        assertInverts(Long.MAX_VALUE, 50, 50);
    }

    @Test
    public void testInvertFullRange() {
        assertInverts(Long.MAX_VALUE, 20, 30);
        assertInverts(Long.MAX_VALUE, 20, 25);
        assertInverts(Long.MAX_VALUE, 25, 30);
        assertInverts(Long.MAX_VALUE, 21, 29);
    }

    @Test
    public void testInvertMultipleDisjointRanges() {
        assertInverts(Long.MAX_VALUE, 10, 10, 25, 27, 50, 50);
        assertInverts(Long.MAX_VALUE, 10, 10, 20, 30, 50, 50);
        assertInverts(Long.MAX_VALUE, 20, 21, 25, 26);
        assertInverts(Long.MAX_VALUE, 10, 10, 50, 50);
        assertInverts(Long.MAX_VALUE, 21, 22, 25, 26, 29, 30);
        assertInverts(Long.MAX_VALUE, 10, 10, 20, 20, 30, 30, 50, 50);
    }

    @Test
    public void testInvertEverything() {
        assertInverts(Long.MAX_VALUE, RECEIVER_PAIRS);
        assertInverts(12, RECEIVER_PAIRS);
    }

    @Test
    public void testInvertMaximumPositionTruncation() {
        // Truncation inside a range.
        assertInverts(5, 20, 30);
        assertInverts(1, 20, 30);
        assertInverts(11, 20, 30);
        // Truncation dropping the whole answer (first inverted position is already past maximumPosition).
        assertInverts(2, 25, 27);
        assertInverts(0, 50, 50);
        assertInverts(5, 50, 50);
        // Truncation with multi-range keys.
        assertInverts(3, 10, 10, 20, 30);
        assertInverts(2, 10, 10, 25, 27);
        assertInverts(8, 10, 10, 25, 27);
        assertInverts(0, 10, 10, 50, 50);
        assertInverts(0, 20, 20, 50, 50);
        assertInverts(6, 10, 10, 20, 21, 25, 27, 50, 50);
        assertInverts(11, 10, 10, 20, 30, 50, 50);
    }

    @Test
    public void testInvertEmptyKeys() {
        final SortedRanges s = receiver();
        assertArrayEquals(new long[0], toArray(s.ixInvertOnNew(OrderedLongSet.EMPTY, Long.MAX_VALUE)));
        assertArrayEquals(new long[0], toArray(s.ixInvertOnNew(OrderedLongSet.EMPTY, 0)));
    }

    @Test
    public void testInvertAllSingleRangesOfReceiverKeys() {
        // Exhaustive cross check of every contiguous key range that is fully present in the receiver, against the
        // model, for every dispatch and for a few maximumPosition values.
        final long[] order = keysInOrder(RECEIVER_PAIRS);
        for (int i = 0; i < order.length; ++i) {
            for (int j = i; j < order.length; ++j) {
                if (order[j] - order[i] != j - i) {
                    // Not contiguous in key space; those keys are not all present.
                    continue;
                }
                for (final long maxPos : new long[] {0, 1, 5, 11, 12, Long.MAX_VALUE}) {
                    assertInverts(maxPos, order[i], order[j]);
                }
            }
        }
    }

    @Test
    public void testInvertNonContiguousSingleRangesThrow() {
        // Every contiguous key range that is NOT fully present must be rejected.
        for (long start = 5; start <= 55; ++start) {
            for (long end = start; end <= 55; ++end) {
                final boolean allPresent = allPresent(start, end);
                final SortedRanges s = receiver();
                for (final OrderedLongSet keys : new OrderedLongSet[] {
                        SingleRange.make(start, end), sr(start, end), rsp(start, end)}) {
                    final String msg = "start=" + start + " end=" + end + " keys=" + keys.getClass().getSimpleName();
                    if (allPresent) {
                        assertArrayEquals(msg, modelPositions(Long.MAX_VALUE, start, end),
                                toArray(s.ixInvertOnNew(keys, Long.MAX_VALUE)));
                    } else {
                        try {
                            final OrderedLongSet r = s.ixInvertOnNew(keys, Long.MAX_VALUE);
                            fail(msg + ": expected IllegalArgumentException, got " + r);
                        } catch (IllegalArgumentException e) {
                            assertEquals(msg, KEYS_NOT_PRESENT_MSG, e.getMessage());
                        }
                    }
                }
            }
        }
    }

    private static boolean allPresent(final long start, final long end) {
        final long[] order = keysInOrder(RECEIVER_PAIRS);
        for (long v = start; v <= end; ++v) {
            if (Arrays.binarySearch(order, v) < 0) {
                return false;
            }
        }
        return true;
    }

    // endregion success paths
}
