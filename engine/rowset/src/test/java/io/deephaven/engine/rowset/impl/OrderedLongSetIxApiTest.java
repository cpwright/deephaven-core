//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Reference-model tests for the {@link OrderedLongSet} ix-API, driving each operation across every ordered pair of
 * backing implementations ({@code OrderedLongSet.EMPTY}, {@link SingleRange}, {@link SortedRanges} and
 * {@link RspBitmap}, both with container spans and full-block spans) and comparing against a
 * {@link TreeSet}&lt;Long&gt; model.
 */
public class OrderedLongSetIxApiTest {

    private static final long B = RspArray.BLOCK_SIZE;

    private enum Kind {
        EMPTY, SINGLE, SORTED, RSP
    }

    private static final class Shape {
        final String name;
        final Kind kind;
        final long[][] ranges;
        private NavigableSet<Long> model;

        Shape(final String name, final Kind kind, final long[][] ranges) {
            this.name = name;
            this.kind = kind;
            this.ranges = ranges;
        }

        OrderedLongSet make() {
            switch (kind) {
                case EMPTY:
                    return OrderedLongSet.EMPTY;
                case SINGLE:
                    assertEquals(name, 1, ranges.length);
                    return SingleRange.make(ranges[0][0], ranges[0][1]);
                case SORTED: {
                    SortedRanges sr = SortedRanges.makeSingleRange(ranges[0][0], ranges[0][1]);
                    for (int i = 1; i < ranges.length; ++i) {
                        sr = sr.addRange(ranges[i][0], ranges[i][1]);
                        assertNotNull(name + ": SortedRanges capacity overflow", sr);
                    }
                    return sr;
                }
                case RSP: {
                    RspBitmap rb = RspBitmap.makeEmpty();
                    for (final long[] range : ranges) {
                        rb = rb.addRange(range[0], range[1]);
                    }
                    rb.finishMutationsAndOptimize();
                    return rb;
                }
                default:
                    throw new IllegalStateException();
            }
        }

        NavigableSet<Long> model() {
            if (model == null) {
                final TreeSet<Long> t = new TreeSet<>();
                for (final long[] range : ranges) {
                    for (long v = range[0]; v <= range[1]; ++v) {
                        t.add(v);
                    }
                }
                model = t;
            }
            return model;
        }

        boolean isEmpty() {
            return ranges.length == 0;
        }
    }

    private static final Shape EMPTY = new Shape("empty", Kind.EMPTY, new long[0][]);
    private static final Shape RSP_EMPTY = new Shape("rspEmpty", Kind.RSP, new long[0][]);
    private static final Shape SINGLE_KEY = new Shape("singleKey", Kind.SINGLE, new long[][] {{3 * B + 5, 3 * B + 5}});
    private static final Shape SINGLE_SMALL = new Shape("singleSmall", Kind.SINGLE, new long[][] {{10, 25}});
    private static final Shape SINGLE_BIG = new Shape("singleBig", Kind.SINGLE, new long[][] {{0, 3 * B + 100}});
    private static final Shape SORTED_A =
            new Shape("sortedA", Kind.SORTED, new long[][] {{5, 8}, {20, 30}, {50, 50}});
    private static final Shape SORTED_B =
            new Shape("sortedB", Kind.SORTED, new long[][] {{22, 40}, {90, 95}, {2 * B + 10, 2 * B + 20}});
    private static final Shape RSP_SMALL =
            new Shape("rspSmall", Kind.RSP, new long[][] {{12, 18}, {40, 60}, {B + 10, B + 20}});
    private static final Shape RSP_FULL =
            new Shape("rspFull", Kind.RSP, new long[][] {{2 * B, 3 * B - 1}, {3 * B + 80, 3 * B + 90}});
    private static final Shape RSP_MIX =
            new Shape("rspMix", Kind.RSP, new long[][] {{0, 5}, {B, 2 * B - 1}, {2 * B + 10, 2 * B + 90}});

    private static final Shape[] SHAPES = new Shape[] {
            EMPTY, RSP_EMPTY, SINGLE_KEY, SINGLE_SMALL, SINGLE_BIG, SORTED_A, SORTED_B, RSP_SMALL, RSP_FULL, RSP_MIX
    };

    // region model helpers

    private static TreeSet<Long> union(final NavigableSet<Long> a, final NavigableSet<Long> b) {
        final TreeSet<Long> t = new TreeSet<>(a);
        t.addAll(b);
        return t;
    }

    private static TreeSet<Long> minus(final NavigableSet<Long> a, final NavigableSet<Long> b) {
        final TreeSet<Long> t = new TreeSet<>(a);
        t.removeAll(b);
        return t;
    }

    private static TreeSet<Long> intersect(final NavigableSet<Long> a, final NavigableSet<Long> b) {
        final TreeSet<Long> t = new TreeSet<>(a);
        t.retainAll(b);
        return t;
    }

    private static TreeSet<Long> shifted(final NavigableSet<Long> a, final long amount) {
        final TreeSet<Long> t = new TreeSet<>();
        for (final long v : a) {
            t.add(v + amount);
        }
        return t;
    }

    private static NavigableSet<Long> rangeModel(final NavigableSet<Long> a, final long start, final long end) {
        if (start > end) {
            return new TreeSet<>();
        }
        return a.subSet(start, true, end, true);
    }

    // endregion model helpers

    /**
     * Assert the full contents of {@code actual} match {@code expected}: cardinality, emptiness, first/last keys, and
     * full membership in order.
     */
    private static void assertContent(final String ctx, final NavigableSet<Long> expected,
            final OrderedLongSet actual) {
        actual.ixValidate(ctx);
        assertEquals(ctx + ": cardinality", expected.size(), actual.ixCardinality());
        assertEquals(ctx + ": isEmpty", expected.isEmpty(), actual.ixIsEmpty());
        if (expected.isEmpty()) {
            return;
        }
        assertEquals(ctx + ": firstKey", (long) expected.first(), actual.ixFirstKey());
        assertEquals(ctx + ": lastKey", (long) expected.last(), actual.ixLastKey());
        final Iterator<Long> eit = expected.iterator();
        final long[] pos = new long[1];
        final boolean complete = actual.ixForEachLong((final long v) -> {
            if (!eit.hasNext() || eit.next() != v) {
                return false;
            }
            ++pos[0];
            return true;
        });
        assertTrue(ctx + ": content mismatch at position " + pos[0], complete);
        assertFalse(ctx + ": actual is missing trailing values", eit.hasNext());
    }

    private interface BinOp {
        OrderedLongSet apply(OrderedLongSet recv, OrderedLongSet arg);
    }

    private interface ModelBinOp {
        NavigableSet<Long> apply(NavigableSet<Long> recv, NavigableSet<Long> arg);
    }

    /**
     * Drive a binary ix operation over the full (receiver shape) x (argument shape) matrix, comparing against the model
     * operation.
     *
     * @param receiverPreserved true for "OnNew" style operations that leave the receiver untouched; false for mutators,
     *        whose receiver reference is consumed and superseded by the returned reference.
     */
    private static void checkBinaryOpMatrix(final String opName, final boolean receiverPreserved, final BinOp op,
            final ModelBinOp modelOp) {
        for (final Shape rs : SHAPES) {
            for (final Shape as : SHAPES) {
                final String ctx = opName + "(" + rs.name + ", " + as.name + ")";
                final OrderedLongSet recv = rs.make();
                final OrderedLongSet arg = as.make();
                final NavigableSet<Long> expected = modelOp.apply(rs.model(), as.model());
                final OrderedLongSet result = op.apply(recv, arg);
                assertContent(ctx, expected, result);
                if (as.model().size() <= 100_000) {
                    assertContent(ctx + " [arg preserved]", as.model(), arg);
                }
                if (receiverPreserved && rs.model().size() <= 100_000) {
                    assertContent(ctx + " [receiver preserved]", rs.model(), recv);
                }
                result.ixRelease();
                if (receiverPreserved && result != recv) {
                    recv.ixRelease();
                }
                arg.ixRelease();
            }
        }
    }

    @Test
    public void testInsertMatrix() {
        checkBinaryOpMatrix("ixInsert", false, OrderedLongSet::ixInsert, OrderedLongSetIxApiTest::union);
    }

    @Test
    public void testRemoveMatrix() {
        checkBinaryOpMatrix("ixRemove", false, OrderedLongSet::ixRemove, OrderedLongSetIxApiTest::minus);
    }

    @Test
    public void testRetainMatrix() {
        checkBinaryOpMatrix("ixRetain", false, OrderedLongSet::ixRetain, OrderedLongSetIxApiTest::intersect);
    }

    @Test
    public void testIntersectOnNewMatrix() {
        checkBinaryOpMatrix("ixIntersectOnNew", true, OrderedLongSet::ixIntersectOnNew,
                OrderedLongSetIxApiTest::intersect);
    }

    @Test
    public void testMinusOnNewMatrix() {
        checkBinaryOpMatrix("ixMinusOnNew", true, OrderedLongSet::ixMinusOnNew, OrderedLongSetIxApiTest::minus);
    }

    @Test
    public void testUnionOnNewMatrix() {
        checkBinaryOpMatrix("ixUnionOnNew", true, OrderedLongSet::ixUnionOnNew, OrderedLongSetIxApiTest::union);
    }

    @Test
    public void testSubsetOfMatrix() {
        for (final Shape rs : SHAPES) {
            for (final Shape as : SHAPES) {
                final String ctx = "ixSubsetOf(" + rs.name + ", " + as.name + ")";
                final OrderedLongSet recv = rs.make();
                final OrderedLongSet arg = as.make();
                final boolean expected = as.model().containsAll(rs.model());
                assertEquals(ctx, expected, recv.ixSubsetOf(arg));
                recv.ixRelease();
                arg.ixRelease();
            }
        }
    }

    @Test
    public void testOverlapsMatrix() {
        for (final Shape rs : SHAPES) {
            for (final Shape as : SHAPES) {
                final String ctx = "ixOverlaps(" + rs.name + ", " + as.name + ")";
                final OrderedLongSet recv = rs.make();
                final OrderedLongSet arg = as.make();
                final boolean expected = !intersect(rs.model(), as.model()).isEmpty();
                assertEquals(ctx, expected, recv.ixOverlaps(arg));
                recv.ixRelease();
                arg.ixRelease();
            }
        }
    }

    /**
     * Full (receiver) x (added) x (removed) matrix for {@code ixUpdate}. The ix-API assumes added and removed are
     * disjoint (see the comment on {@code RspBitmap#ixUpdate}), so pairs violating that are skipped.
     */
    @Test
    public void testUpdateMatrix() {
        int cases = 0;
        for (final Shape rs : SHAPES) {
            for (final Shape addedShape : SHAPES) {
                for (final Shape removedShape : SHAPES) {
                    if (!Collections.disjoint(addedShape.model(), removedShape.model())) {
                        continue;
                    }
                    ++cases;
                    final String ctx =
                            "ixUpdate(" + rs.name + "; +" + addedShape.name + "; -" + removedShape.name + ")";
                    final OrderedLongSet recv = rs.make();
                    final OrderedLongSet added = addedShape.make();
                    final OrderedLongSet removed = removedShape.make();
                    final TreeSet<Long> expected = new TreeSet<>(rs.model());
                    expected.removeAll(removedShape.model());
                    expected.addAll(addedShape.model());
                    final OrderedLongSet result = recv.ixUpdate(added, removed);
                    assertContent(ctx, expected, result);
                    result.ixRelease();
                    added.ixRelease();
                    removed.ixRelease();
                }
            }
        }
        // Make sure the disjointness filter did not trivialize the matrix.
        assertTrue("expected a meaningful number of update cases, got " + cases, cases > 100);
    }

    /** ixUpdate combinations that empty out the receiver entirely. */
    @Test
    public void testUpdateEmptiesReceiver() {
        for (final Shape rs : new Shape[] {SINGLE_SMALL, SINGLE_KEY, SORTED_A, RSP_SMALL, RSP_FULL, RSP_MIX}) {
            final String ctx = "ixUpdate empties " + rs.name;
            final OrderedLongSet recv = rs.make();
            final OrderedLongSet removed = SingleRange.make(0, 4 * B);
            final OrderedLongSet result = recv.ixUpdate(OrderedLongSet.EMPTY, removed);
            assertContent(ctx, new TreeSet<>(), result);
            assertSame(ctx + ": expected the EMPTY singleton", OrderedLongSet.EMPTY, result);
            removed.ixRelease();
        }
        // Removing everything while adding something new.
        final OrderedLongSet recv = RSP_SMALL.make();
        final OrderedLongSet added = SingleRange.make(5 * B, 5 * B + 5);
        final OrderedLongSet removed = SingleRange.make(0, 4 * B);
        final OrderedLongSet result = recv.ixUpdate(added, removed);
        final TreeSet<Long> expected = new TreeSet<>();
        for (long v = 5 * B; v <= 5 * B + 5; ++v) {
            expected.add(v);
        }
        assertContent("ixUpdate remove-all-add-new", expected, result);
        result.ixRelease();
        added.ixRelease();
        removed.ixRelease();
    }

    // region range probes: ixOverlapsRange / ixContainsRange / ixRetainRange / ixSubindexByKeyOnNew

    private static final long[][] PROBES = new long[][] {
            {0, 4}, // before most shapes
            {10, 25}, // exact SINGLE_SMALL
            {12, 12}, // single key
            {21, 44}, // partial overlaps
            {26, 39}, // inside gaps of several shapes
            {50, B}, // spans into block 1
            {B + 15, 2 * B + 7}, // one-side trims
            {2 * B + 100, 2 * B + 200}, // inside RSP_FULL's full block
            {2 * B, 3 * B - 1}, // exactly a full block
            {3 * B + 85, 4 * B}, // trims RSP_FULL's tail range
            {4 * B + 1, 5 * B}, // past everything
            {0, 4 * B}, // covers everything
    };

    @Test
    public void testOverlapsRangeAndContainsRange() {
        for (final Shape rs : SHAPES) {
            final OrderedLongSet recv = rs.make();
            for (final long[] probe : PROBES) {
                final long s = probe[0];
                final long e = probe[1];
                final String ctx = rs.name + " range [" + s + ", " + e + "]";
                final NavigableSet<Long> sub = rangeModel(rs.model(), s, e);
                assertEquals("ixOverlapsRange " + ctx, !sub.isEmpty(), recv.ixOverlapsRange(s, e));
                assertEquals("ixContainsRange " + ctx, sub.size() == e - s + 1, recv.ixContainsRange(s, e));
            }
            recv.ixRelease();
        }
    }

    @Test
    public void testRetainRange() {
        for (final Shape rs : SHAPES) {
            for (final long[] probe : PROBES) {
                final long s = probe[0];
                final long e = probe[1];
                final String ctx = "ixRetainRange " + rs.name + " [" + s + ", " + e + "]";
                final OrderedLongSet recv = rs.make();
                final NavigableSet<Long> expected = rangeModel(rs.model(), s, e);
                final OrderedLongSet result = recv.ixRetainRange(s, e);
                assertContent(ctx, expected, result);
                if (expected.isEmpty() && !rs.isEmpty()) {
                    assertSame(ctx + ": disjoint range should produce the EMPTY singleton",
                            OrderedLongSet.EMPTY, result);
                }
                result.ixRelease();
            }
        }
    }

    @Test
    public void testSubindexByKeyOnNew() {
        for (final Shape rs : SHAPES) {
            final OrderedLongSet recv = rs.make();
            for (final long[] probe : PROBES) {
                final long s = probe[0];
                final long e = probe[1];
                final String ctx = "ixSubindexByKeyOnNew " + rs.name + " [" + s + ", " + e + "]";
                final NavigableSet<Long> expected = rangeModel(rs.model(), s, e);
                final OrderedLongSet result = recv.ixSubindexByKeyOnNew(s, e);
                assertContent(ctx, expected, result);
                result.ixRelease();
            }
            // An inverted window produces EMPTY for every implementation.
            assertSame(rs.name + ": inverted key range", OrderedLongSet.EMPTY, recv.ixSubindexByKeyOnNew(50, 10));
            assertContent(rs.name + " [receiver preserved]", rs.model(), recv);
            recv.ixRelease();
        }
    }

    // endregion

    @Test
    public void testSubindexByPosOnNew() {
        for (final Shape rs : SHAPES) {
            final OrderedLongSet recv = rs.make();
            final long[] keys = rs.model().stream().mapToLong(Long::longValue).toArray();
            final long card = keys.length;
            final long[][] windows = new long[][] {
                    {0, card}, // everything
                    {0, card + 10}, // everything, overlong
                    {1, Math.max(1, card - 1)}, // trims both ends
                    {card / 2, card / 2 + 10}, // middle window
                    {card, card + 5}, // start at cardinality -> empty
                    {card + 3, card + 8}, // start past cardinality -> empty
                    {5, 3}, // inverted -> empty
                    {2, 2}, // empty window
                    {0, 0}, // empty window at zero
                    {Math.max(0, card - 1), card}, // last element
            };
            for (final long[] w : windows) {
                final long startPos = w[0];
                final long endPosExclusive = w[1];
                final String ctx = "ixSubindexByPosOnNew " + rs.name + " [" + startPos + ", " + endPosExclusive + ")";
                final TreeSet<Long> expected = new TreeSet<>();
                for (long p = Math.max(0, startPos); p < Math.min(card, endPosExclusive); ++p) {
                    expected.add(keys[(int) p]);
                }
                final OrderedLongSet result = recv.ixSubindexByPosOnNew(startPos, endPosExclusive);
                assertContent(ctx, expected, result);
                result.ixRelease();
            }
            assertContent(rs.name + " [receiver preserved]", rs.model(), recv);
            recv.ixRelease();
        }
    }

    // region invert

    private static TreeSet<Long> invertModel(final NavigableSet<Long> recv, final NavigableSet<Long> keys,
            final long maxPos) {
        final TreeSet<Long> result = new TreeSet<>();
        long pos = 0;
        for (final long v : recv) {
            if (pos > maxPos) {
                break;
            }
            if (keys.contains(v)) {
                result.add(pos);
            }
            ++pos;
        }
        return result;
    }

    private static void checkInvert(final String ctx, final Shape receiverShape, final OrderedLongSet keys,
            final NavigableSet<Long> keysModel, final long maxPos) {
        final OrderedLongSet recv = receiverShape.make();
        final OrderedLongSet result = recv.ixInvertOnNew(keys, maxPos);
        assertContent(ctx, invertModel(receiverShape.model(), keysModel, maxPos), result);
        result.ixRelease();
        if (result != recv) {
            recv.ixRelease();
        }
        keys.ixRelease();
    }

    @Test
    public void testInvertOnNew() {
        for (final Shape rs : new Shape[] {SINGLE_SMALL, SORTED_A, RSP_SMALL, RSP_FULL, RSP_MIX}) {
            final long card = rs.model().size();
            for (final long maxPos : new long[] {Long.MAX_VALUE, card / 2, 0}) {
                // SingleRange keys argument: the receiver's first stored range (always contiguous and present).
                {
                    final long s = rs.ranges[0][0];
                    final long e = rs.ranges[0][1];
                    final SingleRange keys = SingleRange.make(s, e);
                    checkInvert("ixInvertOnNew(" + rs.name + ", single[" + s + "," + e + "], maxPos=" + maxPos + ")",
                            rs, keys, rangeModel(rs.model(), s, e), maxPos);
                }
                // SortedRanges keys argument: the first two keys of each stored range.
                {
                    SortedRanges keys = SortedRanges.makeEmpty();
                    final TreeSet<Long> keysModel = new TreeSet<>();
                    for (final long[] range : rs.ranges) {
                        final long e = Math.min(range[0] + 1, range[1]);
                        keys = keys.addRange(range[0], e);
                        assertNotNull(keys);
                        for (long v = range[0]; v <= e; ++v) {
                            keysModel.add(v);
                        }
                    }
                    checkInvert("ixInvertOnNew(" + rs.name + ", sortedKeys, maxPos=" + maxPos + ")",
                            rs, keys, keysModel, maxPos);
                }
                // RspBitmap keys argument: a few keys from each stored range.
                {
                    RspBitmap keys = RspBitmap.makeEmpty();
                    final TreeSet<Long> keysModel = new TreeSet<>();
                    for (final long[] range : rs.ranges) {
                        final long e = Math.min(range[0] + 2, range[1]);
                        keys = keys.addRange(range[0], e);
                        for (long v = range[0]; v <= e; ++v) {
                            keysModel.add(v);
                        }
                    }
                    keys.finishMutationsAndOptimize();
                    checkInvert("ixInvertOnNew(" + rs.name + ", rspKeys, maxPos=" + maxPos + ")",
                            rs, keys, keysModel, maxPos);
                }
            }
            // Empty keys argument.
            checkInvert("ixInvertOnNew(" + rs.name + ", EMPTY)", rs, OrderedLongSet.EMPTY, new TreeSet<>(),
                    Long.MAX_VALUE);
        }
    }

    @Test
    public void testInvertOnNewTruncationInsideFullBlockSpan() {
        // Keys within (and covering) a full-block span of the receiver, with maximumPosition truncating inside it.
        {
            final SingleRange keys = SingleRange.make(2 * B + 100, 2 * B + 200);
            checkInvert("ixInvertOnNew(rspFull, singleKeysInFullBlock, maxPos=150)",
                    RSP_FULL, keys, rangeModel(RSP_FULL.model(), 2 * B + 100, 2 * B + 200), 150);
        }
        {
            RspBitmap keys = RspBitmap.makeEmpty();
            keys = keys.addRange(2 * B, 3 * B - 1); // the entire full-block span
            keys.finishMutationsAndOptimize();
            checkInvert("ixInvertOnNew(rspFull, fullBlockKeys, maxPos=" + (B / 2) + ")",
                    RSP_FULL, keys, rangeModel(RSP_FULL.model(), 2 * B, 3 * B - 1), B / 2);
        }
    }

    @Test
    public void testInvertOnNewNonExistingKeyThrows() {
        for (final Shape rs : new Shape[] {SINGLE_SMALL, SORTED_A, RSP_SMALL}) {
            final OrderedLongSet recv = rs.make();
            final SingleRange keys = SingleRange.make(0, 3); // not contained in any of these shapes
            try {
                final OrderedLongSet result = recv.ixInvertOnNew(keys, Long.MAX_VALUE);
                result.ixRelease();
                fail(rs.name + ": expected IllegalArgumentException for non-existing key");
            } catch (IllegalArgumentException expected) {
                // expected
            }
            recv.ixRelease();
        }
    }

    // endregion

    // region shifts

    @Test
    public void testShiftOnNewAndInPlace() {
        final long[] amounts = new long[] {0, 3, B, 2 * B + 7, -7, -B};
        for (final Shape rs : SHAPES) {
            for (final long amount : amounts) {
                if (!rs.isEmpty() && rs.model().first() + amount < 0) {
                    continue; // would shift keys negative
                }
                final NavigableSet<Long> expected = shifted(rs.model(), amount);
                {
                    final String ctx = "ixShiftOnNew(" + rs.name + ", " + amount + ")";
                    final OrderedLongSet recv = rs.make();
                    final OrderedLongSet result = recv.ixShiftOnNew(amount);
                    assertContent(ctx, expected, result);
                    assertContent(ctx + " [receiver preserved]", rs.model(), recv);
                    result.ixRelease();
                    if (result != recv) {
                        recv.ixRelease();
                    }
                }
                {
                    final String ctx = "ixShiftInPlace(" + rs.name + ", " + amount + ")";
                    final OrderedLongSet recv = rs.make();
                    final OrderedLongSet result = recv.ixShiftInPlace(amount);
                    assertContent(ctx, expected, result);
                    result.ixRelease();
                }
            }
        }
    }

    @Test
    public void testInsertWithShiftMatrix() {
        final long[] amounts = new long[] {0, 3, B};
        for (final long amount : amounts) {
            checkBinaryOpMatrix("ixInsertWithShift(" + amount + ")", false,
                    (recv, arg) -> recv.ixInsertWithShift(amount, arg),
                    (recvModel, argModel) -> union(recvModel, shifted(argModel, amount)));
        }
    }

    // endregion

    @Test
    public void testAppendRange() {
        for (final Shape rs : SHAPES) {
            final long last = rs.isEmpty() ? -1 : rs.model().last();
            // Adjacent append (merges with the trailing range).
            {
                final String ctx = "ixAppendRange adjacent " + rs.name;
                final OrderedLongSet recv = rs.make();
                final TreeSet<Long> expected = new TreeSet<>(rs.model());
                for (long v = last + 1; v <= last + 6; ++v) {
                    expected.add(v);
                }
                final OrderedLongSet result = recv.ixAppendRange(last + 1, last + 6);
                assertContent(ctx, expected, result);
                result.ixRelease();
            }
            // Gap append.
            {
                final String ctx = "ixAppendRange gap " + rs.name;
                final OrderedLongSet recv = rs.make();
                final TreeSet<Long> expected = new TreeSet<>(rs.model());
                for (long v = last + 10; v <= last + 15; ++v) {
                    expected.add(v);
                }
                final OrderedLongSet result = recv.ixAppendRange(last + 10, last + 15);
                assertContent(ctx, expected, result);
                result.ixRelease();
            }
            // Far-away append into a new block.
            {
                final String ctx = "ixAppendRange far " + rs.name;
                final OrderedLongSet recv = rs.make();
                final TreeSet<Long> expected = new TreeSet<>(rs.model());
                final long start = 8 * B;
                for (long v = start; v <= start + 20; ++v) {
                    expected.add(v);
                }
                final OrderedLongSet result = recv.ixAppendRange(start, start + 20);
                assertContent(ctx, expected, result);
                result.ixRelease();
            }
        }
    }

    @Test
    public void testSingleRangeAppendRangeOverlappingThrows() {
        final OrderedLongSet recv = SingleRange.make(10, 25);
        try {
            final OrderedLongSet result = recv.ixAppendRange(20, 30);
            result.ixRelease();
            fail("expected IllegalStateException appending an overlapping range to a SingleRange");
        } catch (IllegalStateException expected) {
            // expected
        }
        recv.ixRelease();
    }

    @Test
    public void testCompact() {
        for (final Shape rs : SHAPES) {
            final String ctx = "ixCompact " + rs.name;
            final OrderedLongSet recv = rs.make();
            final OrderedLongSet result = recv.ixCompact();
            assertContent(ctx, rs.model(), result);
            result.ixRelease();
            if (result != recv) {
                recv.ixRelease();
            }
        }
        // An RspBitmap holding a single small range compacts down to a SingleRange.
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(5, 10);
        rb.finishMutationsAndOptimize();
        final OrderedLongSet compacted = rb.ixCompact();
        assertTrue("single-range rsp should compact to SingleRange, got " + compacted.getClass().getSimpleName(),
                compacted instanceof SingleRange);
        assertEquals(5, compacted.ixFirstKey());
        assertEquals(10, compacted.ixLastKey());
        compacted.ixRelease();
        // A SortedRanges holding a single range compacts down to a SingleRange.
        final SortedRanges sr = SortedRanges.makeSingleRange(7, 12);
        final OrderedLongSet srCompacted = sr.ixCompact();
        assertTrue(srCompacted instanceof SingleRange);
        assertEquals(7, srCompacted.ixFirstKey());
        assertEquals(12, srCompacted.ixLastKey());
        srCompacted.ixRelease();
        sr.ixRelease();
        // An empty RspBitmap compacts to the EMPTY singleton.
        final RspBitmap emptyRb = RspBitmap.makeEmpty();
        assertSame(OrderedLongSet.EMPTY, emptyRb.ixCompact());
    }

    @Test
    public void testToRspOnNewAndAsRspBitmap() {
        for (final Shape rs : SHAPES) {
            final String ctx = "ixToRspOnNew " + rs.name;
            final OrderedLongSet recv = rs.make();
            final RspBitmap rsp = recv.ixToRspOnNew();
            assertContent(ctx, rs.model(), rsp);
            if (recv instanceof RspBitmap) {
                // ixToRspOnNew on an RspBitmap is a cowRef of the same data.
                assertEquals(ctx + ": refCount", 2, recv.ixRefCount());
                // asRspBitmap must return the same instance without a new reference.
                assertSame(ctx + ": asRspBitmap identity", recv, OrderedLongSet.asRspBitmap(recv));
                assertEquals(ctx + ": asRspBitmap must not acquire", 2, recv.ixRefCount());
            }
            rsp.ixRelease();
            recv.ixRelease();
        }
        // asRspBitmap converts non-Rsp implementations.
        final OrderedLongSet single = SingleRange.make(3, 9);
        final RspBitmap converted = OrderedLongSet.asRspBitmap(single);
        final TreeSet<Long> expected = new TreeSet<>();
        for (long v = 3; v <= 9; ++v) {
            expected.add(v);
        }
        assertContent("asRspBitmap(single)", expected, converted);
        converted.ixRelease();
        single.ixRelease();
    }

    // region getKeysForPositions / ixGet

    private static long[] keysForPositions(final OrderedLongSet s, final long[] positions) {
        final ArrayList<Long> out = new ArrayList<>();
        s.ixGetKeysForPositions(Arrays.stream(positions).iterator(), out::add);
        final long[] result = new long[out.size()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = out.get(i);
        }
        return result;
    }

    @Test
    public void testGetKeysForPositionsValid() {
        for (final Shape rs : SHAPES) {
            if (rs.isEmpty()) {
                continue;
            }
            final OrderedLongSet recv = rs.make();
            final long[] keys = rs.model().stream().mapToLong(Long::longValue).toArray();
            final int card = keys.length;
            final TreeSet<Integer> positionSet = new TreeSet<>(Arrays.asList(0, Math.min(1, card - 1),
                    card / 2, card - 1));
            final long[] positions = positionSet.stream().mapToLong(Integer::longValue).toArray();
            final long[] expected = new long[positions.length];
            for (int i = 0; i < positions.length; ++i) {
                expected[i] = keys[(int) positions[i]];
            }
            assertTrue("ixGetKeysForPositions " + rs.name,
                    Arrays.equals(expected, keysForPositions(recv, positions)));
            // ixGet at the same positions.
            for (final long pos : positions) {
                assertEquals("ixGet " + rs.name + " pos " + pos, keys[(int) pos], recv.ixGet(pos));
            }
            recv.ixRelease();
        }
    }

    @Test
    public void testGetKeysForPositionsOutOfRange() {
        // EMPTY: every position maps to NULL_ROW_KEY.
        assertTrue(Arrays.equals(new long[] {-1, -1, -1},
                keysForPositions(OrderedLongSet.EMPTY, new long[] {0, 3, 100})));

        // SingleRange: out-of-range positions map to NULL_ROW_KEY independently, valid ones still resolve.
        final OrderedLongSet single = SINGLE_SMALL.make(); // [10, 25], cardinality 16
        assertTrue(Arrays.equals(new long[] {-1, 12, 25, -1},
                keysForPositions(single, new long[] {-3, 2, 15, 16})));
        assertEquals(RowSequence.NULL_ROW_KEY, single.ixGet(-1));
        assertEquals(RowSequence.NULL_ROW_KEY, single.ixGet(16));
        single.ixRelease();

        // RspBitmap: the first out-of-range position makes all subsequent outputs NULL_ROW_KEY.
        final OrderedLongSet rsp = RSP_SMALL.make(); // cardinality 39
        final long card = rsp.ixCardinality();
        assertTrue(Arrays.equals(new long[] {12, 13, -1, -1},
                keysForPositions(rsp, new long[] {0, 1, card, card + 3})));
        // A negative position drains everything to NULL_ROW_KEY.
        assertTrue(Arrays.equals(new long[] {-1, -1},
                keysForPositions(rsp, new long[] {-1, 0})));
        assertEquals(RowSequence.NULL_ROW_KEY, rsp.ixGet(-1));
        rsp.ixRelease();

        // RspBitmap with full-block spans.
        final OrderedLongSet full = RSP_FULL.make();
        final long fullCard = full.ixCardinality();
        assertTrue(Arrays.equals(new long[] {2 * B, 3 * B - 1, 3 * B + 80, 3 * B + 90, -1},
                keysForPositions(full, new long[] {0, B - 1, B, fullCard - 1, fullCard})));
        full.ixRelease();
    }

    // endregion

    // region search iterator

    @Test
    public void testSearchIteratorAdvanceFreshIterators() {
        final long[] probes = new long[] {
                0, 5, 12, 15, 19, 30, 45, 60, 61, B, B + 10, B + 15, B + 21, 2 * B, 2 * B + 50, 3 * B - 1, 3 * B,
                3 * B + 85, 3 * B + 91, 10 * B};
        for (final Shape rs : SHAPES) {
            final OrderedLongSet recv = rs.make();
            for (final long v : probes) {
                final Long expected = rs.model().ceiling(v);
                try (final RowSet.SearchIterator it = recv.ixSearchIterator()) {
                    final boolean found = it.advance(v);
                    if (expected == null) {
                        assertFalse(rs.name + ": advance(" + v + ") should be exhausted", found);
                        assertFalse(rs.name + ": hasNext after failed advance", it.hasNext());
                    } else {
                        assertTrue(rs.name + ": advance(" + v + ")", found);
                        assertEquals(rs.name + ": currentValue after advance(" + v + ")",
                                (long) expected, it.currentValue());
                    }
                }
            }
            recv.ixRelease();
        }
    }

    @Test
    public void testSearchIteratorAdvanceSequence() {
        // RSP_SMALL is {12-18, 40-60, B+10..B+20}.
        final OrderedLongSet recv = RSP_SMALL.make();
        try (final RowSet.SearchIterator it = recv.ixSearchIterator()) {
            // From not-started, before the first range.
            assertTrue(it.advance(0));
            assertEquals(12, it.currentValue());
            // Within the current range.
            assertTrue(it.advance(15));
            assertEquals(15, it.currentValue());
            // No-op advance to a value at or before the current position.
            assertTrue(it.advance(14));
            assertEquals(15, it.currentValue());
            // Into a gap: lands on the start of the next range.
            assertTrue(it.advance(30));
            assertEquals(40, it.currentValue());
            // To the end of the current range.
            assertTrue(it.advance(60));
            assertEquals(60, it.currentValue());
            // Into a later range.
            assertTrue(it.advance(B + 11));
            assertEquals(B + 11, it.currentValue());
            // Past the end.
            assertFalse(it.advance(B + 21));
            assertFalse(it.hasNext());
            // Advancing an exhausted iterator stays exhausted.
            assertFalse(it.advance(10 * B));
        }
        // Interleave nextLong() and advance().
        try (final RowSet.SearchIterator it = recv.ixSearchIterator()) {
            assertTrue(it.hasNext());
            assertEquals(12, it.nextLong());
            assertTrue(it.advance(14));
            assertEquals(14, it.currentValue());
            assertEquals(15, it.nextLong());
        }
        recv.ixRelease();

        // Advance across a full-block span.
        final OrderedLongSet full = RSP_FULL.make();
        try (final RowSet.SearchIterator it = full.ixSearchIterator()) {
            assertTrue(it.advance(2 * B + 1000));
            assertEquals(2 * B + 1000, it.currentValue());
            assertTrue(it.advance(3 * B)); // gap after the full block
            assertEquals(3 * B + 80, it.currentValue());
            assertFalse(it.advance(3 * B + 91));
        }
        full.ixRelease();

        // Advance on an empty RspBitmap starts and ends exhausted.
        final OrderedLongSet empty = RSP_EMPTY.make();
        try (final RowSet.SearchIterator it = empty.ixSearchIterator()) {
            assertFalse(it.advance(0));
        }
        empty.ixRelease();
    }

    private static RowSet.TargetComparator targetComparatorFor(final long target) {
        return (final long rKey, final int direction) -> Long.compare(target, rKey);
    }

    @Test
    public void testSearchIteratorBinarySearchValue() {
        // RSP_SMALL is {12-18, 40-60, B+10..B+20}.
        final OrderedLongSet recv = RSP_SMALL.make();
        // From a not-started iterator: target below the first value returns -1.
        try (final RowSet.SearchIterator it = recv.ixSearchIterator()) {
            assertEquals(-1, it.binarySearchValue(targetComparatorFor(5), 1));
        }
        // Floor semantics within and between ranges.
        try (final RowSet.SearchIterator it = recv.ixSearchIterator()) {
            assertEquals(45, it.binarySearchValue(targetComparatorFor(45), 1));
        }
        try (final RowSet.SearchIterator it = recv.ixSearchIterator()) {
            assertEquals(18, it.binarySearchValue(targetComparatorFor(30), 1));
        }
        try (final RowSet.SearchIterator it = recv.ixSearchIterator()) {
            assertEquals(B + 20, it.binarySearchValue(targetComparatorFor(10 * B), 1));
        }
        // After advancing, the search continues from the current position.
        try (final RowSet.SearchIterator it = recv.ixSearchIterator()) {
            assertTrue(it.advance(40));
            assertEquals(45, it.binarySearchValue(targetComparatorFor(45), 1));
        }
        recv.ixRelease();
        // On an empty RspBitmap, binarySearchValue from not-started returns -1.
        final OrderedLongSet empty = RSP_EMPTY.make();
        try (final RowSet.SearchIterator it = empty.ixSearchIterator()) {
            assertEquals(-1, it.binarySearchValue(targetComparatorFor(100), 1));
        }
        empty.ixRelease();
    }

    // endregion

    // region EMPTY singleton dispatch

    @Test
    public void testEmptySingletonBasics() {
        final OrderedLongSet e = OrderedLongSet.EMPTY;
        assertSame(e, e.ixCowRef());
        e.ixRelease(); // no-op
        assertEquals(1, e.ixRefCount());
        assertEquals(RowSequence.NULL_ROW_KEY, e.ixFirstKey());
        assertEquals(RowSequence.NULL_ROW_KEY, e.ixLastKey());
        assertEquals(0, e.ixCardinality());
        assertTrue(e.ixIsEmpty());
        assertEquals(RowSequence.NULL_ROW_KEY, e.ixGet(0));
        assertEquals(RowSequence.NULL_ROW_KEY, e.ixFind(7));
        assertTrue(e.ixForEachLong(v -> {
            fail("consumer must not be called");
            return true;
        }));
        assertTrue(e.ixForEachLongRange((s, l) -> {
            fail("consumer must not be called");
            return true;
        }));
        assertSame(e, e.ixSubindexByPosOnNew(0, 10));
        assertSame(e, e.ixSubindexByKeyOnNew(0, 10));
        assertSame(RowSet.EMPTY_ITERATOR, e.ixIterator());
        assertSame(RowSet.EMPTY_ITERATOR, e.ixSearchIterator());
        assertSame(RowSet.EMPTY_ITERATOR, e.ixReverseIterator());
        assertSame(RowSet.RangeIterator.empty, e.ixRangeIterator());
        assertSame(e, e.ixRemove(5));
        assertSame(e, e.ixRemoveRange(0, 100));
        assertSame(e, e.ixRetainRange(0, 100));
        assertSame(e, e.ixShiftOnNew(17));
        assertSame(e, e.ixShiftInPlace(17));
        assertSame(e, e.ixCompact());
        assertFalse(e.ixContainsRange(0, 1));
        assertFalse(e.ixOverlapsRange(0, 100));
        assertSame(RowSequenceFactory.EMPTY, e.ixGetRowSequenceByPosition(0, 10));
        assertSame(RowSequenceFactory.EMPTY, e.ixGetRowSequenceByKeyRange(0, 10));
        assertSame(RowSequenceFactory.EMPTY_ITERATOR, e.ixGetRowSequenceIterator());
        assertEquals(0, e.ixRangesCountUpperBound());
        assertEquals(1, e.ixGetAverageRunLengthEstimate());
        assertEquals("EMPTY", e.toString());
        e.ixValidate();
        final RspBitmap asRsp = e.ixToRspOnNew();
        assertTrue(asRsp.ixIsEmpty());
        asRsp.ixRelease();
        // Inserting into EMPTY produces SingleRanges.
        final OrderedLongSet ins = e.ixInsert(5);
        assertTrue(ins instanceof SingleRange);
        assertEquals(5, ins.ixFirstKey());
        assertEquals(5, ins.ixLastKey());
        ins.ixRelease();
        final OrderedLongSet insRange = e.ixInsertRange(3, 9);
        assertTrue(insRange instanceof SingleRange);
        assertEquals(3, insRange.ixFirstKey());
        assertEquals(9, insRange.ixLastKey());
        insRange.ixRelease();
        // ixRemoveSecondHalf must never be reached on EMPTY.
        try {
            e.ixRemoveSecondHalf(LongChunk.chunkWrap(new long[] {1, 5}), 0, 2);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    @Test
    public void testEmptySingletonDispatchWithNonEmptyArgs() {
        final OrderedLongSet e = OrderedLongSet.EMPTY;
        for (final Shape as : new Shape[] {SINGLE_SMALL, SORTED_A, RSP_SMALL, RSP_FULL}) {
            final OrderedLongSet arg = as.make();
            final String ctx = "EMPTY vs " + as.name;

            // ixUpdate returns a cow reference of added.
            final OrderedLongSet updated = e.ixUpdate(arg, OrderedLongSet.EMPTY);
            assertContent(ctx + ": ixUpdate", as.model(), updated);
            if (arg instanceof SingleRange) {
                // SingleRange.ixCowRef() returns a copy (SingleRange has no refcount); content equality suffices.
                assertTrue(ctx + ": cowRef of a SingleRange is a SingleRange", updated instanceof SingleRange);
            } else {
                assertSame(ctx + ": cowRef shares the instance", arg, updated);
                assertEquals(ctx + ": cowRef bumps refCount", 2, arg.ixRefCount());
            }
            updated.ixRelease();

            // ixUpdate with empty added leaves EMPTY alone.
            final OrderedLongSet notUpdated = e.ixUpdate(OrderedLongSet.EMPTY, arg);
            assertSame(ctx + ": ixUpdate(EMPTY, x)", e, notUpdated);

            // ixUnionOnNew returns a cow reference of the argument (a copy for SingleRange).
            final OrderedLongSet unioned = e.ixUnionOnNew(arg);
            assertContent(ctx + ": ixUnionOnNew", as.model(), unioned);
            if (!(arg instanceof SingleRange)) {
                assertSame(ctx + ": ixUnionOnNew shares the instance", arg, unioned);
            }
            unioned.ixRelease();

            // ixInsert returns a cow reference of the argument (a copy for SingleRange).
            final OrderedLongSet inserted = e.ixInsert(arg);
            assertContent(ctx + ": ixInsert", as.model(), inserted);
            if (!(arg instanceof SingleRange)) {
                assertSame(ctx + ": ixInsert shares the instance", arg, inserted);
            }
            inserted.ixRelease();

            // ixInsertWithShift shifts the argument onto a new set.
            final OrderedLongSet shiftedResult = e.ixInsertWithShift(2 * B, arg);
            assertContent(ctx + ": ixInsertWithShift", shifted(as.model(), 2 * B), shiftedResult);
            shiftedResult.ixRelease();

            // Set predicates and set ops against non-empty arguments.
            assertSame(ctx + ": ixRetain", e, e.ixRetain(arg));
            assertSame(ctx + ": ixIntersectOnNew", e, e.ixIntersectOnNew(arg));
            assertSame(ctx + ": ixMinusOnNew", e, e.ixMinusOnNew(arg));
            assertSame(ctx + ": ixRemove", e, e.ixRemove(arg));
            assertSame(ctx + ": ixInvertOnNew", e, e.ixInvertOnNew(arg, Long.MAX_VALUE));
            assertFalse(ctx + ": ixOverlaps", e.ixOverlaps(arg));
            assertTrue(ctx + ": ixSubsetOf", e.ixSubsetOf(arg));

            assertContent(ctx + ": arg preserved", as.model(), arg);
            arg.ixRelease();
        }

        // ixGetKeysForPositions on EMPTY yields NULL_ROW_KEY for every input position.
        assertTrue(Arrays.equals(new long[] {-1, -1, -1, -1},
                keysForPositions(e, new long[] {0, 1, 2, 1000})));

        // ixAppendRange on EMPTY behaves like ixInsertRange.
        final OrderedLongSet appended = e.ixAppendRange(7, 11);
        assertTrue(appended instanceof SingleRange);
        assertEquals(7, appended.ixFirstKey());
        assertEquals(11, appended.ixLastKey());
        appended.ixRelease();

        // ixInsertSecondHalf via the chunked ixInsert path.
        final LongChunk<OrderedRowKeys> chunk = LongChunk.chunkWrap(new long[] {1, 3, 7, 8});
        final OrderedLongSet fromChunk = e.ixInsert(chunk, 0, 4);
        final TreeSet<Long> chunkModel = new TreeSet<>(Arrays.asList(1L, 3L, 7L, 8L));
        assertContent("EMPTY ixInsert(chunk)", chunkModel, fromChunk);
        fromChunk.ixRelease();
    }

    // endregion

    /**
     * {@code OrderedLongSet.twoRanges} is reached through SingleRange operations that split a range in two.
     */
    @Test
    public void testTwoRangesViaSingleRangeSplits() {
        // Removing a middle key splits a SingleRange.
        final OrderedLongSet single = SingleRange.make(10, 25);
        final OrderedLongSet split = single.ixRemove(17);
        final TreeSet<Long> expected = new TreeSet<>();
        for (long v = 10; v <= 25; ++v) {
            if (v != 17) {
                expected.add(v);
            }
        }
        assertContent("SingleRange split by remove", expected, split);
        split.ixRelease();

        // Appending a non-adjacent range makes two ranges.
        final OrderedLongSet single2 = SingleRange.make(10, 25);
        final OrderedLongSet appended = single2.ixAppendRange(100, 105);
        final TreeSet<Long> expected2 = new TreeSet<>();
        for (long v = 10; v <= 25; ++v) {
            expected2.add(v);
        }
        for (long v = 100; v <= 105; ++v) {
            expected2.add(v);
        }
        assertContent("SingleRange gap append", expected2, appended);
        appended.ixRelease();

        // Adjacent append merges instead.
        final OrderedLongSet single3 = SingleRange.make(10, 25);
        final OrderedLongSet merged = single3.ixAppendRange(26, 30);
        assertTrue("adjacent append should stay a SingleRange", merged instanceof SingleRange);
        assertEquals(10, merged.ixFirstKey());
        assertEquals(30, merged.ixLastKey());
        merged.ixRelease();
    }

    /** Exercise the BuilderSequential default methods declared on the OrderedLongSet interface. */
    @Test
    public void testBuilderSequentialDefaults() {
        final OrderedLongSet.BuilderSequential builder = new OrderedLongSetBuilderSequential();
        builder.appendOrderedRowKeysChunk(LongChunk.chunkWrap(new long[] {1, 2, 5}), 0, 3);
        final OrderedLongSet donor = RSP_SMALL.make();
        builder.appendOrderedLongSet(1000, donor);
        final OrderedLongSet built = builder.getOrderedLongSet();
        final TreeSet<Long> expected = new TreeSet<>(Arrays.asList(1L, 2L, 5L));
        expected.addAll(shifted(RSP_SMALL.model(), 1000));
        assertContent("builder sequential defaults", expected, built);
        built.ixRelease();
        donor.ixRelease();
    }
}
