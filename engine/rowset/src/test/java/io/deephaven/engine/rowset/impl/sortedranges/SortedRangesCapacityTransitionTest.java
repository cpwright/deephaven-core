//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

/**
 * Tests for the SortedRanges capacity-transition machinery: every mutation shape that can overflow a SortedRanges at
 * its type's max capacity must return null at the SortedRanges level, and the corresponding public ix* operation must
 * transparently fall back to an RspBitmap-backed result with identical membership.
 *
 * Every membership assertion is mirrored against a {@code TreeSet<Long>} model.
 */
public class SortedRangesCapacityTransitionTest {

    // Spacing wide enough that a long-typed SortedRanges at LONG_SPARSE_MAX_CAPACITY spans more than
    // Integer.MAX_VALUE, so it can neither grow nor pack into an int/short-typed SortedRanges.
    private static final long SP = 1L << 21;

    // ---------------------------------------------------------------------------------------------------------
    // Model helpers.
    // ---------------------------------------------------------------------------------------------------------

    private static void addRangeToModel(final TreeSet<Long> model, final long start, final long end) {
        for (long v = start; v <= end; ++v) {
            model.add(v);
        }
    }

    private static void removeRangeFromModel(final TreeSet<Long> model, final long start, final long end) {
        for (long v = start; v <= end; ++v) {
            model.remove(v);
        }
    }

    private static void assertSameSet(final String msg, final OrderedLongSet set, final TreeSet<Long> model) {
        set.ixValidate();
        assertEquals(msg + ": cardinality", model.size(), set.ixCardinality());
        final Iterator<Long> mit = model.iterator();
        final boolean match = set.ixForEachLong(v -> mit.hasNext() && mit.next() == v);
        assertTrue(msg + ": contents", match);
        assertFalse(msg + ": model has leftover values", mit.hasNext());
    }

    private static void assertSameSet(final String msg, final RowSet rowSet, final TreeSet<Long> model) {
        assertEquals(msg + ": size", model.size(), rowSet.size());
        final Iterator<Long> mit = model.iterator();
        final boolean match = rowSet.forEachRowKey(v -> mit.hasNext() && mit.next() == v);
        assertTrue(msg + ": contents", match);
        assertFalse(msg + ": model has leftover values", mit.hasNext());
    }

    private static void assertRsp(final String msg, final OrderedLongSet set) {
        assertTrue(msg + ": expected RspBitmap-backed result but got " + set.getClass().getSimpleName(),
                set instanceof RspBitmap);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Builders.
    // ---------------------------------------------------------------------------------------------------------

    private static SortedRanges buildSingles(
            final long base, final long spacing, final int n, final TreeSet<Long> model) {
        SortedRanges sr = SortedRanges.makeEmpty();
        for (int i = 0; i < n; ++i) {
            final long v = base + spacing * i;
            sr = sr.add(v);
            assertNotNull("buildSingles i=" + i, sr);
            if (model != null) {
                model.add(v);
            }
        }
        sr.validate();
        return sr;
    }

    private static SortedRanges buildRanges(
            final long base, final long spacing, final long rangeLen, final int n, final TreeSet<Long> model) {
        SortedRanges sr = SortedRanges.makeEmpty();
        for (int i = 0; i < n; ++i) {
            final long s = base + spacing * i;
            final long e = s + rangeLen - 1;
            sr = sr.appendRange(s, e);
            assertNotNull("buildRanges i=" + i, sr);
            if (model != null) {
                addRangeToModel(model, s, e);
            }
        }
        sr.validate();
        return sr;
    }

    /**
     * Build a long-typed SortedRanges at exactly LONG_SPARSE_MAX_CAPACITY, alternating singletons (even i at i*SP) and
     * ranges (odd i at [i*SP, i*SP+10]), and ending in a singleton. The span exceeds Integer.MAX_VALUE so the array can
     * neither grow nor be packed; every mutation needing an extra slot must fail.
     */
    private SortedRanges buildLongAtCapMixed(final TreeSet<Long> model) {
        SortedRanges sr = SortedRanges.makeEmpty();
        long i = 0;
        while (true) {
            final long base = i * SP;
            final SortedRanges next = ((i & 1) == 0) ? sr.append(base) : sr.appendRange(base, base + 10);
            if (next == null) {
                break;
            }
            sr = next;
            if ((i & 1) == 0) {
                model.add(base);
            } else {
                addRangeToModel(model, base, base + 10);
            }
            ++i;
        }
        // Top off any remaining single slot with singletons.
        while (true) {
            final long base = i * SP;
            final SortedRanges next = sr.append(base);
            if (next == null) {
                break;
            }
            sr = next;
            model.add(base);
            ++i;
        }
        assertTrue(sr instanceof SortedRangesLong);
        assertEquals(SortedRanges.LONG_SPARSE_MAX_CAPACITY, sr.count());
        // The matrix's append shapes assume the set ends in a singleton.
        assertTrue(sr.unpackedGet(sr.count() - 1) >= 0);
        sr.validate();
        assertEquals(model.size(), sr.getCardinality());
        return sr;
    }

    /**
     * Build a long-typed SortedRanges at exactly LONG_SPARSE_MAX_CAPACITY made purely of ranges [i*SP, i*SP+10], so it
     * ends in a range.
     */
    private SortedRanges buildLongAtCapRanges(final TreeSet<Long> model) {
        SortedRanges sr = SortedRanges.makeEmpty();
        long i = 0;
        while (true) {
            final long base = i * SP;
            final SortedRanges next = sr.appendRange(base, base + 10);
            if (next == null) {
                break;
            }
            sr = next;
            addRangeToModel(model, base, base + 10);
            ++i;
        }
        assertTrue(sr instanceof SortedRangesLong);
        assertEquals(SortedRanges.LONG_SPARSE_MAX_CAPACITY, sr.count());
        assertTrue(sr.unpackedGet(sr.count() - 1) < 0); // ends in a range
        sr.validate();
        return sr;
    }

    /**
     * Build a short-typed SortedRanges at exactly SHORT_MAX_CAPACITY: singletons spaced 4 apart, whole span within
     * short offsets. Construction goes long -> int -> short via the eager packing in ensureCanAppend.
     */
    private SortedRanges buildShortAtCap(final TreeSet<Long> model) {
        SortedRanges sr = SortedRanges.makeEmpty();
        long i = 0;
        while (true) {
            final long v = 4 * i;
            final SortedRanges next = sr.add(v);
            if (next == null) {
                break;
            }
            sr = next;
            model.add(v);
            ++i;
        }
        assertTrue("expected short-typed, got " + sr.getClass().getSimpleName(), sr instanceof SortedRangesShort);
        assertEquals(SortedRanges.SHORT_MAX_CAPACITY, sr.count());
        sr.validate();
        return sr;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Overflow-matrix helpers: each drives a single shape both directly (null return) and through the ix* API
    // (transparent RspBitmap promotion), asserting membership against the model.
    // ---------------------------------------------------------------------------------------------------------

    private void checkInsertOverflow(final SortedRanges base, final TreeSet<Long> model, final long v) {
        final String msg = "insert v=" + v;
        final SortedRanges copy = base.deepCopy();
        assertNull(msg, copy.add(v));
        assertNull(msg + " (unsafe)", copy.addUnsafe(v));
        final OrderedLongSet res = copy.ixInsert(v);
        assertRsp(msg, res);
        final TreeSet<Long> expected = new TreeSet<>(model);
        expected.add(v);
        assertSameSet(msg, res, expected);
        res.ixRelease();
    }

    private void checkInsertRangeOverflow(
            final SortedRanges base, final TreeSet<Long> model, final long s, final long e) {
        final String msg = "insertRange [" + s + "," + e + "]";
        final SortedRanges copy = base.deepCopy();
        assertNull(msg, copy.addRange(s, e));
        assertNull(msg + " (unsafe)", copy.addRangeUnsafe(s, e));
        final OrderedLongSet res = copy.ixInsertRange(s, e);
        assertRsp(msg, res);
        final TreeSet<Long> expected = new TreeSet<>(model);
        addRangeToModel(expected, s, e);
        assertSameSet(msg, res, expected);
        res.ixRelease();
    }

    private void checkRemoveOverflow(final SortedRanges base, final TreeSet<Long> model, final long v) {
        final String msg = "remove v=" + v;
        final SortedRanges copy = base.deepCopy();
        assertNull(msg, copy.remove(v));
        final OrderedLongSet res = copy.ixRemove(v);
        assertRsp(msg, res);
        final TreeSet<Long> expected = new TreeSet<>(model);
        expected.remove(v);
        assertSameSet(msg, res, expected);
        res.ixRelease();
    }

    private void checkRemoveRangeOverflow(
            final SortedRanges base, final TreeSet<Long> model, final long s, final long e) {
        final String msg = "removeRange [" + s + "," + e + "]";
        final SortedRanges copy = base.deepCopy();
        assertNull(msg, copy.removeRange(s, e));
        final OrderedLongSet res = copy.ixRemoveRange(s, e);
        assertRsp(msg, res);
        final TreeSet<Long> expected = new TreeSet<>(model);
        removeRangeFromModel(expected, s, e);
        assertSameSet(msg, res, expected);
        res.ixRelease();
    }

    // ---------------------------------------------------------------------------------------------------------
    // 1. The overflow matrix.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testOverflowMatrixLongAtCapAdds() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges base = buildLongAtCapMixed(model);
        final long s = 2 * SP; // an interior singleton
        final long last = base.last();

        // Merge with the singleton to its right: open(pos, v, -(v+1)).
        checkInsertOverflow(base, model, s - 1);
        // Merge with the singleton to its left: openNeg(pos, -v).
        checkInsertOverflow(base, model, s + 1);
        // No merge at all, interior: open(pos, v).
        checkInsertOverflow(base, model, s + 100);
        // Append adjacent to the trailing singleton (extends it into a range): packedAppend(-v).
        checkInsertOverflow(base, model, last + 1);
        // Append isolated past the end: packedAppend(v).
        checkInsertOverflow(base, model, last + 100);
    }

    @Test
    public void testOverflowMatrixLongAtCapAddRanges() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges base = buildLongAtCapMixed(model);
        final long s = 2 * SP; // an interior singleton
        final long last = base.last();

        // Isolated interior range: open2.
        checkInsertRangeOverflow(base, model, s + 2, s + 20);
        // Merge to left singleton only: openNeg.
        checkInsertRangeOverflow(base, model, s + 1, s + 20);
        // Start lands exactly on the singleton, then merge-left-single: openNeg.
        checkInsertRangeOverflow(base, model, s, s + 20);
        // Merge to right singleton only: open(pos, start, -(end+1)).
        checkInsertRangeOverflow(base, model, s - 20, s - 1);
        // Range covers exactly one existing singleton (len == 1): open(pos, start, -end).
        checkInsertRangeOverflow(base, model, s - 5, s + 5);
        // Append isolated past the end: packedAppend2.
        checkInsertRangeOverflow(base, model, last + 2, last + 20);
        // Append adjacent to trailing singleton: packedAppend(-end).
        checkInsertRangeOverflow(base, model, last + 1, last + 20);
        // Start lands exactly on the trailing singleton (iStart walks to count): packedAppend(-end).
        checkInsertRangeOverflow(base, model, last, last + 20);
    }

    @Test
    public void testOverflowMatrixLongAtCapRemoves() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges base = buildLongAtCapMixed(model);
        final long r = 3 * SP; // an interior range [r, r+10]
        assertTrue(base.containsRange(r, r + 10));

        // Split strictly interior: open2Neg.
        checkRemoveOverflow(base, model, r + 5);
        // Remove start+1 of the range: open(pos, v+1).
        checkRemoveOverflow(base, model, r + 1);
        // Remove end-1 of the range: openNeg(pos, -(v-1), v+1).
        checkRemoveOverflow(base, model, r + 9);

        // removeRange interior splits.
        // Truncate at both sides: open2Neg.
        checkRemoveRangeOverflow(base, model, r + 3, r + 5);
        // Truncate left, right side leaves a single: openNeg.
        checkRemoveRangeOverflow(base, model, r + 3, r + 9);
        // No left truncation (starts at start+1), truncate right: open.
        checkRemoveRangeOverflow(base, model, r + 1, r + 5);
    }

    @Test
    public void testOverflowMatrixShortAtCap() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges base = buildShortAtCap(model);
        final long s = 400; // an interior singleton (all values are multiples of 4)
        final long last = base.last();

        checkInsertOverflow(base, model, s + 1); // merge-left-single: openNeg
        checkInsertOverflow(base, model, s - 1); // merge-right-single: open(v, -(v+1))
        checkInsertOverflow(base, model, s + 2); // isolated interior: open
        checkInsertOverflow(base, model, last + 1); // append adjacent: packedAppend(-v)
        checkInsertOverflow(base, model, last + 2); // append isolated: packedAppend(v)

        checkInsertRangeOverflow(base, model, last + 4, last + 14); // packedAppend2
        checkInsertRangeOverflow(base, model, last + 1, last + 14); // packedAppend(-end)
        checkInsertRangeOverflow(base, model, s + 1, s + 2); // openNeg via merge-left-single
    }

    // ---------------------------------------------------------------------------------------------------------
    // mergeAppend at capacity.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testMergeAppendAtCap() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges base = buildLongAtCapMixed(model);
        final long last = base.last(); // trailing singleton

        // Non-adjacent other: plain append shape, needs one more slot.
        final SortedRanges otherIsolated = SortedRanges.makeSingleRange(last + 10, last + 10);
        assertNull(base.deepCopy().mergeAppend(otherIsolated, true));

        // Adjacent single-value merge: our trailing singleton becomes a range, but the rest still needs a slot.
        SortedRanges otherAdjacent = SortedRanges.makeSingleRange(last + 1, last + 1);
        otherAdjacent = otherAdjacent.append(last + 10);
        assertNotNull(otherAdjacent);
        assertNull(base.deepCopy().mergeAppend(otherAdjacent, true));

        // Adjacent range merge with extra content.
        SortedRanges otherAdjRange = SortedRanges.makeSingleRange(last + 1, last + 5);
        otherAdjRange = otherAdjRange.appendRange(last + 100, last + 110);
        assertNotNull(otherAdjRange);
        assertNull(base.deepCopy().mergeAppend(otherAdjRange, true));

        // Via the public insert API the same operation must promote to RspBitmap.
        final OrderedLongSet res = base.deepCopy().ixInsert(otherAdjRange);
        assertRsp("mergeAppend fallback", res);
        final TreeSet<Long> expected = new TreeSet<>(model);
        addRangeToModel(expected, last + 1, last + 5);
        addRangeToModel(expected, last + 100, last + 110);
        assertSameSet("mergeAppend fallback", res, expected);
        res.ixRelease();

        // Ranges-ending base: weEndInRange variants.
        final TreeSet<Long> rModel = new TreeSet<>();
        final SortedRanges rBase = buildLongAtCapRanges(rModel);
        final long rLast = rBase.last(); // end of trailing range

        // Pure range merge into our last range succeeds at cap (no extra slot needed).
        final SortedRanges pureMerge = SortedRanges.makeSingleRange(rLast + 1, rLast + 5);
        final SortedRanges merged = rBase.deepCopy().mergeAppend(pureMerge, true);
        assertNotNull(merged);
        final TreeSet<Long> mergedExpected = new TreeSet<>(rModel);
        addRangeToModel(mergedExpected, rLast + 1, rLast + 5);
        assertSameSet("mergeAppend in-place merge at cap", merged, mergedExpected);

        // Range merge plus extra range: needs more slots -> null (weEndInRange, range-merge branch).
        SortedRanges rangePlus = SortedRanges.makeSingleRange(rLast + 1, rLast + 5);
        rangePlus = rangePlus.appendRange(rLast + 100, rLast + 110);
        assertNotNull(rangePlus);
        assertNull(rBase.deepCopy().mergeAppend(rangePlus, true));

        // Single-value merge plus extra singleton: needs more slots -> null (weEndInRange, single-merge branch).
        SortedRanges singlePlus = SortedRanges.makeSingleRange(rLast + 1, rLast + 1);
        singlePlus = singlePlus.append(rLast + 100);
        assertNotNull(singlePlus);
        assertNull(rBase.deepCopy().mergeAppend(singlePlus, true));

        // ixAppendRange at cap: appendRange fails, falls back to RspBitmap.
        final SortedRanges copy = rBase.deepCopy();
        assertNull(copy.appendRange(rLast + 100, rLast + 110));
        assertNull(copy.appendRangeUnsafe(rLast + 100, rLast + 110));
        assertNull(copy.append(rLast + 100));
        assertNull(copy.appendUnsafe(rLast + 100));
        final OrderedLongSet appended = copy.ixAppendRange(rLast + 100, rLast + 110);
        assertRsp("ixAppendRange fallback", appended);
        final TreeSet<Long> appendExpected = new TreeSet<>(rModel);
        addRangeToModel(appendExpected, rLast + 100, rLast + 110);
        assertSameSet("ixAppendRange fallback", appended, appendExpected);
        appended.ixRelease();

        // insertImpl on an empty target returns a cowRef of other.
        final OrderedLongSet cow = SortedRanges.makeEmpty().insertImpl(pureMerge);
        assertSame(pureMerge, cow);
        cow.ixRelease();
    }

    // ---------------------------------------------------------------------------------------------------------
    // 2. Union work-buffer overflow and union results too large for any SortedRanges type.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testUnionWorkBufferOverflowToRsp() {
        // Two int-typed sets of 4100 interleaved singletons each; the merged form needs 8200 slots, which
        // overflows the MAX_CAPACITY work buffer used by SortedRanges.union.
        final TreeSet<Long> m1 = new TreeSet<>();
        final TreeSet<Long> m2 = new TreeSet<>();
        final SortedRanges a = buildSingles(0, 1L << 16, 4100, m1);
        final SortedRanges b = buildSingles(8, 1L << 16, 4100, m2);
        assertTrue(a instanceof SortedRangesInt);
        assertTrue(b instanceof SortedRangesInt);

        final OrderedLongSet union = a.ixUnionOnNew(b);
        assertRsp("union work-buffer overflow", union);
        final TreeSet<Long> expected = new TreeSet<>(m1);
        expected.addAll(m2);
        assertSameSet("union work-buffer overflow", union, expected);
        union.ixRelease();

        // Same through the RowSet-level API (insert == in-place union).
        try (final WritableRowSetImpl ra = new WritableRowSetImpl(a.ixCowRef());
                final WritableRowSetImpl rb = new WritableRowSetImpl(b.ixCowRef())) {
            ra.insert(rb);
            assertRsp("RowSet insert union overflow", ra.getInnerSet());
            assertSameSet("RowSet insert union overflow", ra, expected);
        }

        // Disjoint high/low sets: the buffer overflows while draining the leftovers of the first iterator.
        final TreeSet<Long> mLow = new TreeSet<>();
        final TreeSet<Long> mHigh = new TreeSet<>();
        final SortedRanges low = buildSingles(0, 1L << 16, 4100, mLow);
        final SortedRanges high = buildSingles(4100L * (1L << 16) + 32, 1L << 16, 4100, mHigh);
        final OrderedLongSet disjointUnion = high.ixUnionOnNew(low);
        assertRsp("union leftover-drain overflow", disjointUnion);
        final TreeSet<Long> disjointExpected = new TreeSet<>(mLow);
        disjointExpected.addAll(mHigh);
        assertSameSet("union leftover-drain overflow", disjointUnion, disjointExpected);
        disjointUnion.ixRelease();
    }

    @Test
    public void testUnionResultTooLargeForSortedRangesGoesRsp() {
        // The union fits the work buffer (5000 entries) but its span exceeds Integer.MAX_VALUE while its count
        // exceeds LONG_SPARSE_MAX_CAPACITY, so no SortedRanges flavor can hold it -> RspBitmap.
        final TreeSet<Long> m1 = new TreeSet<>();
        final TreeSet<Long> m2 = new TreeSet<>();
        final SortedRanges a = buildSingles(0, 1L << 20, 2500, m1);
        final SortedRanges b = buildSingles(8, 1L << 20, 2500, m2);

        final OrderedLongSet union = a.ixUnionOnNew(b);
        assertRsp("union count > long-sparse capacity", union);
        final TreeSet<Long> expected = new TreeSet<>(m1);
        expected.addAll(m2);
        assertSameSet("union count > long-sparse capacity", union, expected);
        union.ixRelease();
    }

    @Test
    public void testUnionResultTooLargeForShortOrDenseGoesRsp() {
        // Union fits the buffer, span fits short offsets, but count exceeds SHORT_MAX_CAPACITY -> RspBitmap.
        final TreeSet<Long> m1 = new TreeSet<>();
        final TreeSet<Long> m2 = new TreeSet<>();
        final SortedRanges a = buildSingles(0, 8, SortedRanges.SHORT_MAX_CAPACITY, m1);
        final SortedRanges b = buildSingles(4, 8, SortedRanges.SHORT_MAX_CAPACITY, m2);
        assertTrue(a instanceof SortedRangesShort);
        assertTrue(b instanceof SortedRangesShort);

        final OrderedLongSet union = a.ixUnionOnNew(b);
        assertRsp("union count > short capacity", union);
        final TreeSet<Long> expected = new TreeSet<>(m1);
        expected.addAll(m2);
        assertSameSet("union count > short capacity", union, expected);
        union.ixRelease();

        // Union whose span exceeds short offsets and whose result is dense -> RspBitmap via the dense check.
        final int n = 4090;
        final int[] d1 = new int[n];
        final int[] d2 = new int[n];
        final TreeSet<Long> m3 = new TreeSet<>();
        final TreeSet<Long> m4 = new TreeSet<>();
        for (int i = 0; i < n; ++i) {
            d1[i] = 10 * i;
            m3.add((long) (10 * i));
            d2[i] = 10 * i + 5;
            m4.add((long) (10 * i + 5));
        }
        final SortedRanges dense1 = new SortedRangesInt(d1, 0, n, n);
        final SortedRanges dense2 = new SortedRangesInt(d2, 0, n, n);
        dense1.validate();
        dense2.validate();
        final OrderedLongSet denseUnion = dense1.ixUnionOnNew(dense2);
        assertRsp("dense union", denseUnion);
        final TreeSet<Long> denseExpected = new TreeSet<>(m3);
        denseExpected.addAll(m4);
        assertSameSet("dense union", denseUnion, denseExpected);
        denseUnion.ixRelease();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Minus / remove / update / retain work-buffer overflow.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testMinusRemoveUpdateWorkBufferOverflow() {
        // A: 4090 ranges of length 17 (8180 slots, int-typed). B: one interior singleton per range.
        // A \ B splits every range in two: 16360 slots > MAX_CAPACITY work buffer -> RspBitmap.
        final TreeSet<Long> mA = new TreeSet<>();
        final TreeSet<Long> mB = new TreeSet<>();
        final SortedRanges a = buildRanges(0, 1L << 16, 17, 4090, mA);
        final SortedRanges b = buildSingles(8, 1L << 16, 4090, mB);
        assertTrue(a instanceof SortedRangesInt);

        final TreeSet<Long> expected = new TreeSet<>(mA);
        expected.removeAll(mB);

        final OrderedLongSet minus = a.ixMinusOnNew(b);
        assertRsp("ixMinusOnNew overflow", minus);
        assertSameSet("ixMinusOnNew overflow", minus, expected);
        minus.ixRelease();

        final OrderedLongSet removed = a.deepCopy().ixRemove(b);
        assertRsp("ixRemove(set) overflow", removed);
        assertSameSet("ixRemove(set) overflow", removed, expected);
        removed.ixRelease();

        // ixUpdate with the same removal plus a small addition.
        final long addBase = a.last() + 1000;
        final SortedRanges added = SortedRanges.makeSingleRange(addBase, addBase + 3);
        final OrderedLongSet updated = a.deepCopy().ixUpdate(added, b);
        assertRsp("ixUpdate overflow", updated);
        final TreeSet<Long> updExpected = new TreeSet<>(expected);
        addRangeToModel(updExpected, addBase, addBase + 3);
        assertSameSet("ixUpdate overflow", updated, updExpected);
        updated.ixRelease();

        // ixMinusOnNew with a SingleRange whose removal splits a range at capacity also promotes.
        final TreeSet<Long> capModel = new TreeSet<>();
        final SortedRanges cap = buildLongAtCapMixed(capModel);
        final long r = 3 * SP;
        final OrderedLongSet singleMinus = cap.ixMinusOnNew(SingleRange.make(r + 3, r + 5));
        assertRsp("ixMinusOnNew(SingleRange) at cap", singleMinus);
        final TreeSet<Long> capExpected = new TreeSet<>(capModel);
        removeRangeFromModel(capExpected, r + 3, r + 5);
        assertSameSet("ixMinusOnNew(SingleRange) at cap", singleMinus, capExpected);
        singleMinus.ixRelease();

        // ixUnionOnNew with a SingleRange needing a new interior slot at capacity also promotes.
        final long s = 2 * SP;
        final OrderedLongSet singleUnion = cap.ixUnionOnNew(SingleRange.make(s + 2, s + 6));
        assertRsp("ixUnionOnNew(SingleRange) at cap", singleUnion);
        final TreeSet<Long> unionExpected = new TreeSet<>(capModel);
        addRangeToModel(unionExpected, s + 2, s + 6);
        assertSameSet("ixUnionOnNew(SingleRange) at cap", singleUnion, unionExpected);
        singleUnion.ixRelease();
    }

    @Test
    public void testRetainIntersectWorkBufferOverflow() {
        // Staggered ranges: A_i = [G*i, G*i+25], B_i = [G*i+20, G*(i+1)+4]. Every B range clips the tail of A_i
        // and the head of A_{i+1}, so the intersection has ~2N ranges (~4N slots) > MAX_CAPACITY -> RspBitmap.
        final long G = 16384;
        final int n = 4090;
        SortedRanges a = SortedRanges.makeEmpty();
        SortedRanges b = SortedRanges.makeEmpty();
        for (int i = 0; i < n; ++i) {
            a = a.appendRange(G * i, G * i + 25);
            assertNotNull(a);
            b = b.appendRange(G * i + 20, G * (i + 1) + 4);
            assertNotNull(b);
        }
        a.validate();
        b.validate();
        assertTrue(a instanceof SortedRangesInt);
        assertTrue(b instanceof SortedRangesInt);

        // Expected intersection ranges, in order.
        final ArrayList<long[]> expectedRanges = new ArrayList<>();
        for (int i = 0; i < n; ++i) {
            if (i > 0) {
                expectedRanges.add(new long[] {G * i, G * i + 4});
            }
            expectedRanges.add(new long[] {G * i + 20, G * i + 25});
        }
        long expectedCard = 0;
        for (final long[] range : expectedRanges) {
            expectedCard += range[1] - range[0] + 1;
        }

        final OrderedLongSet intersected = a.ixIntersectOnNew(b);
        assertRsp("ixIntersectOnNew overflow", intersected);
        checkRanges("ixIntersectOnNew overflow", intersected, expectedRanges, expectedCard);
        intersected.ixRelease();

        final OrderedLongSet retained = a.deepCopy().ixRetain(b);
        assertRsp("ixRetain overflow", retained);
        checkRanges("ixRetain overflow", retained, expectedRanges, expectedCard);
        retained.ixRelease();
    }

    private static void checkRanges(
            final String msg, final OrderedLongSet set, final ArrayList<long[]> expectedRanges,
            final long expectedCard) {
        set.ixValidate();
        assertEquals(msg + ": cardinality", expectedCard, set.ixCardinality());
        try (final RowSet.RangeIterator it = set.ixRangeIterator()) {
            int idx = 0;
            while (it.hasNext()) {
                it.next();
                assertTrue(msg + ": too many ranges", idx < expectedRanges.size());
                final long[] expected = expectedRanges.get(idx);
                assertEquals(msg + ": range start " + idx, expected[0], it.currentRangeStart());
                assertEquals(msg + ": range end " + idx, expected[1], it.currentRangeEnd());
                ++idx;
            }
            assertEquals(msg + ": range count", expectedRanges.size(), idx);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3. Pack / convert refusals.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testLongDenseCapacityPackRefusals() {
        // A dense long-typed SortedRanges whose count exceeds INT_DENSE_MAX_CAPACITY: growing is refused (dense
        // long cap) and packing to int is refused (dense int cap), so append/interior-add must fail.
        final int n = 600;
        final long[] data = new long[n];
        final TreeSet<Long> model = new TreeSet<>();
        for (int i = 0; i < n; ++i) {
            data[i] = 4L * i;
            model.add(4L * i);
        }
        final SortedRangesLong sr = new SortedRangesLong(data, n, n);
        sr.validate();
        assertTrue(sr.isDense());

        // Append shape: ensureCanAppend -> tryPackFor (int capacity 0) -> null.
        final long appendV = 4L * n + 2;
        assertNull(sr.deepCopy().append(appendV));
        final OrderedLongSet appended = sr.deepCopy().ixInsert(appendV);
        assertRsp("dense long append refusal", appended);
        final TreeSet<Long> appendExpected = new TreeSet<>(model);
        appendExpected.add(appendV);
        assertSameSet("dense long append refusal", appended, appendExpected);
        appended.ixRelease();

        // Interior open shape: checkSizeAndMoveData -> tryMakePackedType (int capacity 0) -> null.
        assertNull(sr.deepCopy().add(6));
        final OrderedLongSet inserted = sr.deepCopy().ixInsert(6);
        assertRsp("dense long interior refusal", inserted);
        final TreeSet<Long> insertExpected = new TreeSet<>(model);
        insertExpected.add(6L);
        assertSameSet("dense long interior refusal", inserted, insertExpected);
        inserted.ixRelease();
    }

    @Test
    public void testIntDenseCapacityPackRefusals() {
        // A dense int-typed SortedRanges whose count exceeds SHORT_MAX_CAPACITY while its span fits short offsets:
        // growing is refused (dense int cap) and packing to short is refused (short capacity), so mutations fail.
        // Note: SortedRangesInt.deepCopy pads the backing array to the allocation rounding, which would leave a
        // spare slot; construct a fresh, exactly-full instance for each mutation instead.
        final int n = SortedRanges.SHORT_MAX_CAPACITY + 2;
        final long offset = 100;
        final int[] data = new int[n];
        final TreeSet<Long> model = new TreeSet<>();
        for (int i = 0; i < n; ++i) {
            data[i] = 8 * i;
            model.add(offset + 8L * i);
        }
        final java.util.function.Supplier<SortedRangesInt> make =
                () -> new SortedRangesInt(data.clone(), offset, n, n);
        final SortedRangesInt sr = make.get();
        sr.validate();
        assertTrue(sr.isDense());
        assertTrue(sr.last() - sr.first() <= Short.MAX_VALUE);

        // Append shape: tryPackFor -> shortArrayCapacityForLastIndex == 0 -> null.
        final long appendV = offset + 8L * n + 4;
        assertNull(make.get().append(appendV));
        final OrderedLongSet appended = make.get().ixInsert(appendV);
        assertRsp("dense int append refusal", appended);
        final TreeSet<Long> appendExpected = new TreeSet<>(model);
        appendExpected.add(appendV);
        assertSameSet("dense int append refusal", appended, appendExpected);
        appended.ixRelease();

        // Interior open shape: tryMakePackedType -> short capacity 0 -> null.
        final long interiorV = offset + 12;
        assertNull(make.get().add(interiorV));
        final OrderedLongSet inserted = make.get().ixInsert(interiorV);
        assertRsp("dense int interior refusal", inserted);
        final TreeSet<Long> insertExpected = new TreeSet<>(model);
        insertExpected.add(interiorV);
        assertSameSet("dense int interior refusal", inserted, insertExpected);
        inserted.ixRelease();
    }

    @Test
    public void testTryPackViaCompact() {
        // Long -> Short pack: span fits short offsets.
        final int n = 600;
        final long[] shortRangeData = new long[n + 8]; // some slack so compact has something to do
        final TreeSet<Long> m1 = new TreeSet<>();
        for (int i = 0; i < n; ++i) {
            shortRangeData[i] = 4L * i + 3;
            m1.add(4L * i + 3);
        }
        final OrderedLongSet packedShort = new SortedRangesLong(shortRangeData, n, n).ixCompact();
        assertTrue(packedShort instanceof SortedRangesShort);
        assertSameSet("long->short pack", packedShort, m1);

        // Long -> Int pack: span exceeds short but fits int offsets.
        final long[] intRangeData = new long[n];
        final TreeSet<Long> m2 = new TreeSet<>();
        for (int i = 0; i < n; ++i) {
            intRangeData[i] = (1L << 18) * i;
            m2.add((1L << 18) * i);
        }
        final OrderedLongSet packedInt = new SortedRangesLong(intRangeData, n, n).ixCompact();
        assertTrue(packedInt instanceof SortedRangesInt);
        assertSameSet("long->int pack", packedInt, m2);

        // Long, span exceeds int offsets: tryPack refuses, compact just trims the array.
        final long[] wideData = new long[32];
        final TreeSet<Long> m3 = new TreeSet<>();
        wideData[0] = 0;
        m3.add(0L);
        for (int i = 1; i < 20; ++i) {
            wideData[i] = (1L << 32) + (1L << 16) * i;
            m3.add(wideData[i]);
        }
        final SortedRangesLong wide = new SortedRangesLong(wideData, 20, 20);
        wide.validate();
        final OrderedLongSet compacted = wide.ixCompact();
        assertTrue(compacted instanceof SortedRangesLong);
        assertSameSet("long compact no pack", compacted, m3);

        // Int -> Short pack via compact.
        final int[] intData = new int[128];
        final TreeSet<Long> m4 = new TreeSet<>();
        for (int i = 0; i < 100; ++i) {
            intData[i] = 8 * i;
            m4.add(50L + 8 * i);
        }
        final OrderedLongSet intPacked = new SortedRangesInt(intData, 50, 100, 100).ixCompact();
        assertTrue(intPacked instanceof SortedRangesShort);
        assertSameSet("int->short pack", intPacked, m4);

        // Int, span exceeds short offsets: tryPack refuses; stays int.
        final TreeSet<Long> m5 = new TreeSet<>();
        final SortedRanges intWide = buildSingles(0, 1L << 16, 600, m5);
        assertTrue(intWide instanceof SortedRangesInt);
        final OrderedLongSet intCompacted = intWide.ixCompact();
        assertTrue(intCompacted instanceof SortedRangesInt);
        assertSameSet("int compact no pack", intCompacted, m5);

        // tryCompact on a shared (non-writable) SortedRanges is a no-op returning this.
        final TreeSet<Long> m6 = new TreeSet<>();
        final SortedRanges shared = buildSingles(0, 1L << 16, 50, m6);
        shared.acquire();
        try {
            final OrderedLongSet same = shared.ixCompact();
            assertSame(shared, same);
        } finally {
            shared.release();
        }
    }

    @Test
    public void testPackedAddOutsideOffsetWindow() {
        // An int-typed SortedRanges at (or beyond) LONG_SPARSE_MAX_CAPACITY count with a positive offset:
        // any mutation outside the packed window needs a conversion to SortedRangesLong, which is refused
        // because the count exceeds the long-typed capacity.
        final long offset = 1_000_000;
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges sr = buildSingles(offset, 1L << 16, 4100, model);
        assertTrue(sr instanceof SortedRangesInt);

        // add below the offset window.
        assertNull(sr.deepCopy().add(5));
        final OrderedLongSet lowInsert = sr.deepCopy().ixInsert(5);
        assertRsp("packed low insert", lowInsert);
        final TreeSet<Long> lowExpected = new TreeSet<>(model);
        lowExpected.add(5L);
        assertSameSet("packed low insert", lowInsert, lowExpected);
        lowInsert.ixRelease();

        // addRange below the offset window.
        assertNull(sr.deepCopy().addRange(5, 10));
        final OrderedLongSet lowRange = sr.deepCopy().ixInsertRange(5, 10);
        assertRsp("packed low insertRange", lowRange);
        final TreeSet<Long> lowRangeExpected = new TreeSet<>(model);
        addRangeToModel(lowRangeExpected, 5, 10);
        assertSameSet("packed low insertRange", lowRange, lowRangeExpected);
        lowRange.ixRelease();

        // append above the offset window.
        final long highV = offset + Integer.MAX_VALUE + 10L;
        assertNull(sr.deepCopy().append(highV));
        assertNull(sr.deepCopy().add(highV));
        final OrderedLongSet highInsert = sr.deepCopy().ixInsert(highV);
        assertRsp("packed high insert", highInsert);
        final TreeSet<Long> highExpected = new TreeSet<>(model);
        highExpected.add(highV);
        assertSameSet("packed high insert", highInsert, highExpected);
        highInsert.ixRelease();

        // appendRange above the offset window.
        assertNull(sr.deepCopy().appendRange(highV, highV + 5));
        assertNull(sr.deepCopy().addRange(highV, highV + 5));
        final OrderedLongSet highRange = sr.deepCopy().ixAppendRange(highV, highV + 5);
        assertRsp("packed high appendRange", highRange);
        final TreeSet<Long> highRangeExpected = new TreeSet<>(model);
        addRangeToModel(highRangeExpected, highV, highV + 5);
        assertSameSet("packed high appendRange", highRange, highRangeExpected);
        highRange.ixRelease();
    }

    @Test
    public void testPackedRemoveOutsideWindowAndEmptyConversion() {
        final long offset = 1_000_000;
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges sr = buildSingles(offset, 1L << 16, 300, model);
        assertTrue(sr instanceof SortedRangesInt);

        // Removals outside the packed window are no-ops returning this.
        assertSame(sr, sr.remove(5));
        assertSame(sr, sr.removeRange(0, 500));

        // removeRange whose end exceeds the window is clamped; removing everything empties the set.
        final SortedRanges cleared = sr.deepCopy().removeRange(0, Long.MAX_VALUE / 2);
        assertNotNull(cleared);
        assertTrue(cleared.isEmpty());

        // An empty packed SortedRanges converts to a long-typed one when adding outside its window.
        final SortedRanges emptyInt = new SortedRangesInt(2, 1000);
        final SortedRanges converted = emptyInt.add(5);
        assertNotNull(converted);
        assertTrue(converted instanceof SortedRangesLong);
        assertEquals(1, converted.getCardinality());
        assertTrue(converted.contains(5));

        final SortedRanges emptyInt2 = new SortedRangesInt(2, 1000);
        final SortedRanges convertedRange = emptyInt2.addRange(5, 8);
        assertNotNull(convertedRange);
        assertTrue(convertedRange instanceof SortedRangesLong);
        assertEquals(4, convertedRange.getCardinality());
        assertTrue(convertedRange.containsRange(5, 8));
    }

    // ---------------------------------------------------------------------------------------------------------
    // 4. Apply-shift family.
    // ---------------------------------------------------------------------------------------------------------

    private static TreeSet<Long> shiftModel(final TreeSet<Long> model, final long shift) {
        final TreeSet<Long> out = new TreeSet<>();
        for (final long v : model) {
            out.add(v + shift);
        }
        return out;
    }

    @Test
    public void testApplyShiftLong() {
        final TreeSet<Long> model = new TreeSet<>();
        SortedRanges sr = SortedRanges.makeSingleRange(100, 100);
        model.add(100L);
        sr = sr.appendRange(1L << 32, (1L << 32) + 10);
        assertNotNull(sr);
        addRangeToModel(model, 1L << 32, (1L << 32) + 10);
        sr = sr.append((1L << 32) + 100);
        assertNotNull(sr);
        model.add((1L << 32) + 100);
        assertTrue(sr instanceof SortedRangesLong);

        // In-place shift on an unshared set.
        final SortedRanges shifted = sr.applyShift(1000);
        assertSame(sr, shifted);
        assertSameSet("long shift in place", shifted, shiftModel(model, 1000));

        // Shift by zero is a no-op.
        assertSame(shifted, shifted.applyShift(0));

        // Shift on a shared set produces a new object and leaves the original untouched.
        shifted.acquire();
        try {
            final SortedRanges shifted2 = shifted.applyShift(500);
            assertNotSame(shifted, shifted2);
            assertSameSet("long shift shared copy", shifted2, shiftModel(model, 1500));
            assertSameSet("long shift shared orig", shifted, shiftModel(model, 1000));
        } finally {
            shifted.release();
        }

        // applyShiftOnNew always leaves the original untouched.
        final SortedRanges onNew = shifted.applyShiftOnNew(200);
        assertNotSame(shifted, onNew);
        assertSameSet("long shiftOnNew", onNew, shiftModel(model, 1200));
        assertSameSet("long shiftOnNew orig", shifted, shiftModel(model, 1000));
        onNew.release();

        // applyShiftOnNew by zero returns this (with an extra reference).
        final SortedRanges zero = shifted.applyShiftOnNew(0);
        assertSame(shifted, zero);
        zero.release();

        // Shifting the first key negative throws.
        try {
            shifted.applyShift(-(shifted.first() + 1));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            shifted.applyShiftOnNew(-(shifted.first() + 1));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }

    private void checkPackedShifts(final String type, final SortedRanges sr, final TreeSet<Long> model) {
        // Positive shift, unshared: fast path just bumps the offset.
        final SortedRanges up = sr.deepCopy();
        final SortedRanges upShifted = up.applyShift(5000);
        assertSame(up, upShifted);
        assertSameSet(type + " shift up in place", upShifted, shiftModel(model, 5000));

        // Negative shift, unshared: rebase path.
        final SortedRanges down = sr.deepCopy();
        final SortedRanges downShifted = down.applyShift(-100);
        assertSame(down, downShifted);
        assertSameSet(type + " shift down in place", downShifted, shiftModel(model, -100));

        // Positive shift, shared: copy-on-write via offset bump.
        sr.acquire();
        try {
            final SortedRanges sharedUp = sr.applyShift(5000);
            assertNotSame(sr, sharedUp);
            assertSameSet(type + " shift up shared copy", sharedUp, shiftModel(model, 5000));
            assertSameSet(type + " shift up shared orig", sr, model);

            // Negative shift, shared: copy-on-write via rebase.
            final SortedRanges sharedDown = sr.applyShift(-100);
            assertNotSame(sr, sharedDown);
            assertSameSet(type + " shift down shared copy", sharedDown, shiftModel(model, -100));
            assertSameSet(type + " shift down shared orig", sr, model);
        } finally {
            sr.release();
        }

        // applyShiftOnNew, both directions.
        final SortedRanges onNewUp = sr.applyShiftOnNew(5000);
        assertNotSame(sr, onNewUp);
        assertSameSet(type + " shiftOnNew up", onNewUp, shiftModel(model, 5000));
        onNewUp.release();
        final SortedRanges onNewDown = sr.applyShiftOnNew(-100);
        assertNotSame(sr, onNewDown);
        assertSameSet(type + " shiftOnNew down", onNewDown, shiftModel(model, -100));
        onNewDown.release();

        // Shift by zero.
        assertSame(sr, sr.applyShift(0));
        final SortedRanges zero = sr.applyShiftOnNew(0);
        assertSame(sr, zero);
        zero.release();

        // Underflow throws.
        try {
            sr.deepCopy().applyShift(-(sr.first() + 1));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            sr.applyShiftOnNew(-(sr.first() + 1));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        // ix-level entry points.
        final OrderedLongSet ixOnNew = sr.ixShiftOnNew(300);
        assertSameSet(type + " ixShiftOnNew", ixOnNew, shiftModel(model, 300));
        ixOnNew.ixRelease();
        final SortedRanges forInPlace = sr.deepCopy();
        final OrderedLongSet ixInPlace = forInPlace.ixShiftInPlace(300);
        assertSameSet(type + " ixShiftInPlace", ixInPlace, shiftModel(model, 300));
    }

    @Test
    public void testApplyShiftInt() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges sr = buildSingles(1_000_000, 1L << 16, 300, model);
        assertTrue(sr instanceof SortedRangesInt);
        checkPackedShifts("int", sr, model);
    }

    @Test
    public void testApplyShiftShort() {
        final TreeSet<Long> model = new TreeSet<>();
        SortedRanges sr = new SortedRangesShort(8, 500);
        for (int i = 0; i < 40; ++i) {
            sr = sr.add(500 + 3L * i);
            assertNotNull(sr);
            model.add(500 + 3L * i);
        }
        sr = sr.appendRange(1000, 1010);
        assertNotNull(sr);
        addRangeToModel(model, 1000, 1010);
        assertTrue(sr instanceof SortedRangesShort);
        checkPackedShifts("short", sr, model);
    }

    @Test
    public void testShiftViaRowSet() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges sr = buildSingles(1_000_000, 1L << 16, 300, model);
        assertTrue(sr instanceof SortedRangesInt);
        try (final WritableRowSetImpl rs = new WritableRowSetImpl(sr)) {
            rs.shiftInPlace(1L << 20);
            assertSameSet("RowSet shiftInPlace", rs, shiftModel(model, 1L << 20));
            rs.shiftInPlace(-(1L << 19));
            assertSameSet("RowSet shiftInPlace negative", rs, shiftModel(model, 1L << 19));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // ixInsertWithShift fallbacks.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testInsertWithShift() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges base = buildLongAtCapMixed(model);
        final long s = 2 * SP;

        // SingleRange other landing isolated at capacity: addRange fails -> RspBitmap.
        final OrderedLongSet singleRes = base.deepCopy().ixInsertWithShift(100, SingleRange.make(s + 2, s + 6));
        assertRsp("insertWithShift single", singleRes);
        final TreeSet<Long> singleExpected = new TreeSet<>(model);
        addRangeToModel(singleExpected, s + 102, s + 106);
        assertSameSet("insertWithShift single", singleRes, singleExpected);
        singleRes.ixRelease();

        // SortedRanges other appended beyond the end: mergeAppend fails -> RspBitmap.
        SortedRanges other = SortedRanges.makeSingleRange(10, 10);
        other = other.append(50);
        assertNotNull(other);
        final long shift = base.last() + 1000;
        final OrderedLongSet srRes = base.deepCopy().ixInsertWithShift(shift, other);
        assertRsp("insertWithShift sorted ranges", srRes);
        final TreeSet<Long> srExpected = new TreeSet<>(model);
        srExpected.add(shift + 10);
        srExpected.add(shift + 50);
        assertSameSet("insertWithShift sorted ranges", srRes, srExpected);
        srRes.ixRelease();

        // Empty other returns this.
        final SortedRanges copy = base.deepCopy();
        assertSame(copy, copy.ixInsertWithShift(5, OrderedLongSet.EMPTY));

        // Empty this defers to other.ixShiftOnNew.
        final TreeSet<Long> smallModel = new TreeSet<>();
        final SortedRanges small = buildSingles(100, 10, 5, smallModel);
        final OrderedLongSet fromEmpty = SortedRanges.makeEmpty().ixInsertWithShift(7, small);
        assertSameSet("insertWithShift from empty", fromEmpty, shiftModel(smallModel, 7));
        fromEmpty.ixRelease();

        // RspBitmap other: result is an RspBitmap union.
        final RspBitmap rsp = new RspBitmap(0, 5);
        final OrderedLongSet rspRes = small.ixInsertWithShift(1000, rsp);
        assertRsp("insertWithShift rsp", rspRes);
        final TreeSet<Long> rspExpected = new TreeSet<>(smallModel);
        addRangeToModel(rspExpected, 1000, 1005);
        assertSameSet("insertWithShift rsp", rspRes, rspExpected);
        rspRes.ixRelease();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Static factory capacity bounds.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testTryMakeForKnownRangeCapacityBounds() {
        // Short-offset range but final capacity exceeding the short cap.
        assertNull(SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                4, SortedRanges.SHORT_MAX_CAPACITY + 1, 0, 100, false));
        final SortedRanges shortSr = SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                4, 8, 0, 100, false);
        assertTrue(shortSr instanceof SortedRangesShort);

        // Int-offset range but final capacity exceeding the int cap.
        assertNull(SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                4, SortedRanges.INT_SPARSE_MAX_CAPACITY + 1, 0, 100_000, false));
        assertNull(SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                4, SortedRanges.INT_DENSE_MAX_CAPACITY + 1, 0, 100_000, true));
        final SortedRanges intSr = SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                4, 8, 0, 100_000, false);
        assertTrue(intSr instanceof SortedRangesInt);

        // Long range but final capacity exceeding the long cap.
        assertNull(SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                4, SortedRanges.LONG_SPARSE_MAX_CAPACITY + 1, 0, 1L << 40, false));
        assertNull(SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                4, SortedRanges.LONG_DENSE_MAX_CAPACITY + 1, 0, 1L << 40, true));
        final SortedRanges longSr = SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                4, 8, 0, 1L << 40, false);
        assertTrue(longSr instanceof SortedRangesLong);
    }

    // ---------------------------------------------------------------------------------------------------------
    // 5. Iterators, row sequences, degenerate positions, aborts, misc.
    // ---------------------------------------------------------------------------------------------------------

    private static SortedRanges makeSmall() {
        // {10, [20,30], 50}
        SortedRanges sr = SortedRanges.makeSingleRange(10, 10);
        sr = sr.appendRange(20, 30);
        assertNotNull(sr);
        sr = sr.append(50);
        assertNotNull(sr);
        return sr;
    }

    @Test
    public void testSearchIteratorBinarySearchRollback() {
        final SortedRanges sr = makeSmall();
        try (final RowSet.SearchIterator it = sr.getSearchIterator()) {
            // Target before the first element on a fresh iterator: not found, and the consumed range is rolled
            // back so the following nextLong still returns the first value.
            final long notFound = it.binarySearchValue((v, dir) -> Long.compare(5, v), 1);
            assertEquals(-1, notFound);
            assertTrue(it.hasNext());
            assertEquals(10, it.nextLong());

            // Search landing inside the following range.
            final long inRange = it.binarySearchValue((v, dir) -> Long.compare(25, v), 1);
            assertEquals(25, inRange);
            assertEquals(25, it.currentValue());

            // Search within the current range (no repositioning needed).
            final long inCurrent = it.binarySearchValue((v, dir) -> Long.compare(28, v), 1);
            assertEquals(28, inCurrent);

            // Search past the last element from a mid position: lands on the last value.
            final long pastEnd = it.binarySearchValue((v, dir) -> Long.compare(100, v), 1);
            assertEquals(50, pastEnd);
        }

        // Search across ranges using the array binary search (more than one range ahead).
        try (final RowSet.SearchIterator it = sr.getSearchIterator()) {
            assertEquals(10, it.nextLong());
            final long found = it.binarySearchValue((v, dir) -> Long.compare(40, v), 1);
            assertEquals(30, found);
        }

        // Search on a two-entry tail: target beyond everything closes out via the current range.
        SortedRanges sr2 = SortedRanges.makeSingleRange(10, 10);
        sr2 = sr2.appendRange(20, 30);
        assertNotNull(sr2);
        try (final RowSet.SearchIterator it = sr2.getSearchIterator()) {
            assertEquals(10, it.nextLong());
            assertEquals(20, it.nextLong());
            final long found = it.binarySearchValue((v, dir) -> Long.compare(100, v), 1);
            assertEquals(30, found);
        }
    }

    @Test
    public void testReverseIteratorAdvance() {
        final SortedRanges sr = makeSmall(); // {10, [20,30], 50}

        // Advance beyond the last value: positions at the last value.
        try (final RowSet.SearchIterator rit = sr.getReverseIterator()) {
            assertTrue(rit.advance(60));
            assertEquals(50, rit.currentValue());
        }

        // Advance into the middle of a range.
        try (final RowSet.SearchIterator rit = sr.getReverseIterator()) {
            assertTrue(rit.advance(25));
            assertEquals(25, rit.currentValue());
            assertEquals(24, rit.nextLong());
        }

        // Advance landing exactly on a range end.
        try (final RowSet.SearchIterator rit = sr.getReverseIterator()) {
            assertTrue(rit.advance(30));
            assertEquals(30, rit.currentValue());
        }

        // Advance into a gap: positions at the previous singleton.
        try (final RowSet.SearchIterator rit = sr.getReverseIterator()) {
            assertTrue(rit.advance(15));
            assertEquals(10, rit.currentValue());
        }

        // Advance landing exactly on a singleton.
        try (final RowSet.SearchIterator rit = sr.getReverseIterator()) {
            assertTrue(rit.advance(10));
            assertEquals(10, rit.currentValue());
            assertFalse(rit.hasNext());
        }

        // Advance past (before) the first element: exhausted.
        try (final RowSet.SearchIterator rit = sr.getReverseIterator()) {
            assertFalse(rit.advance(5));
        }

        // Advance into a gap whose left neighbor is a range: positions at that range's end.
        SortedRanges withLeadingRange = SortedRanges.makeSingleRange(10, 15);
        withLeadingRange = withLeadingRange.append(30);
        withLeadingRange = withLeadingRange.append(50);
        assertNotNull(withLeadingRange);
        try (final RowSet.SearchIterator rit = withLeadingRange.getReverseIterator()) {
            assertTrue(rit.advance(20));
            assertEquals(15, rit.currentValue());
            assertEquals(14, rit.nextLong());
        }

        // On a packed type, advancing before the offset window also exhausts.
        SortedRanges packed = new SortedRangesShort(8, 1000);
        packed = packed.add(1000);
        packed = packed.addRange(1010, 1020);
        assertNotNull(packed);
        try (final RowSet.SearchIterator rit = packed.getReverseIterator()) {
            assertFalse(rit.advance(5));
        }

        // Full reverse traversal.
        try (final RowSet.SearchIterator rit = sr.getReverseIterator()) {
            final long[] expected = {50, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 10};
            for (final long e : expected) {
                assertTrue(rit.hasNext());
                assertEquals(e, rit.nextLong());
            }
            assertFalse(rit.hasNext());
        }

        // Reverse iterators do not support binary search.
        try (final RowSet.SearchIterator rit = sr.getReverseIterator()) {
            try {
                rit.binarySearchValue((v, dir) -> Long.compare(25, v), 1);
                fail("expected UnsupportedOperationException");
            } catch (UnsupportedOperationException expected) {
            }
        }
    }

    @Test
    public void testRangeIteratorPostpone() {
        final SortedRanges sr = makeSmall();
        try (final RowSet.RangeIterator rit = sr.getRangeIterator()) {
            assertTrue(rit.hasNext());
            rit.next();
            assertEquals(10, rit.currentRangeStart());
            rit.postpone(12);
            assertEquals(12, rit.currentRangeStart());
        }
    }

    @Test
    public void testRowSequenceByPositionEndPastLast() {
        // Set ending in a singleton: request starting at the very last position.
        SortedRanges sr = SortedRanges.makeSingleRange(0, 10);
        sr = sr.appendRange(20, 30);
        assertNotNull(sr);
        sr = sr.append(100);
        assertNotNull(sr);
        final long card = sr.getCardinality();
        try (final RowSequence rs = sr.getRowSequenceByPosition(card - 1, 5)) {
            assertEquals(1, rs.size());
            assertEquals(100, rs.firstRowKey());
            assertEquals(100, rs.lastRowKey());
        }
        // Starting past the end yields the empty sequence.
        try (final RowSequence rs = sr.getRowSequenceByPosition(card, 1)) {
            assertEquals(0, rs.size());
        }
        // A window whose end clamps into the trailing singleton.
        try (final RowSequence rs = sr.getRowSequenceByPosition(5, card)) {
            assertEquals(card - 5, rs.size());
            assertEquals(5, rs.firstRowKey());
            assertEquals(100, rs.lastRowKey());
        }
        // A window ending inside the middle range.
        try (final RowSequence rs = sr.getRowSequenceByPosition(5, 10)) {
            assertEquals(10, rs.size());
            assertEquals(5, rs.firstRowKey());
            assertEquals(23, rs.lastRowKey());
        }
    }

    @Test
    public void testSubRangesByPosDegenerate() {
        final SortedRanges sr = makeSmall(); // {10, [20,30], 50}; positions 0..12
        assertNull(sr.subRangesByPos(0, -1));
        assertNull(sr.subRangesByPos(5, 3));
        assertNull(sr.subRangesByPos(99, 100));

        // Single element set.
        final SortedRanges single = SortedRanges.makeSingleElement(7);
        final SortedRanges singleSub = single.subRangesByPos(0, 5);
        assertNotNull(singleSub);
        singleSub.validate();
        assertEquals(1, singleSub.getCardinality());
        assertTrue(singleSub.contains(7));

        // Single interior value.
        final SortedRanges mid = sr.subRangesByPos(3, 3);
        assertNotNull(mid);
        mid.validate();
        assertEquals(1, mid.getCardinality());
        assertTrue(mid.contains(22));

        // A piece of a single range.
        final SortedRanges piece = sr.subRangesByPos(2, 4);
        assertNotNull(piece);
        piece.validate();
        assertEquals(3, piece.getCardinality());
        assertTrue(piece.containsRange(21, 23));

        // Broken initial range through the trailing singleton.
        final SortedRanges tail = sr.subRangesByPos(2, 12);
        assertNotNull(tail);
        tail.validate();
        assertEquals(11, tail.getCardinality());
        assertTrue(tail.containsRange(21, 30));
        assertTrue(tail.contains(50));

        // Negative start clamps to zero.
        final SortedRanges head = sr.subRangesByPos(-5, 2);
        assertNotNull(head);
        head.validate();
        assertEquals(3, head.getCardinality());
        assertTrue(head.contains(10));
        assertTrue(head.containsRange(20, 21));

        // subRangesByKey degenerate inputs.
        assertNull(SortedRanges.makeEmpty().subRangesByKey(0, 10));
        assertNull(sr.subRangesByKey(0, 5)); // entirely before first
        assertNull(sr.subRangesByKey(60, 70)); // entirely after last
        assertNull(sr.subRangesByKey(12, 15)); // lands in a gap
    }

    @Test
    public void testForEachAbort() {
        final SortedRanges sr = makeSmall(); // {10, [20,30], 50}

        // Abort inside a range.
        final ArrayList<Long> seen = new ArrayList<>();
        final boolean completed = sr.forEachLong(v -> {
            seen.add(v);
            return v != 22;
        });
        assertFalse(completed);
        assertEquals(java.util.Arrays.asList(10L, 20L, 21L, 22L), seen);

        // Abort on the leading singleton.
        assertFalse(sr.forEachLong(v -> false));

        // Abort in forEachLongRange after the first range.
        final ArrayList<long[]> ranges = new ArrayList<>();
        final boolean rangesCompleted = sr.forEachLongRange((s, e) -> {
            ranges.add(new long[] {s, e});
            return ranges.size() < 2;
        });
        assertFalse(rangesCompleted);
        assertEquals(2, ranges.size());
        assertArrayEquals(new long[] {10, 10}, ranges.get(0));
        assertArrayEquals(new long[] {20, 30}, ranges.get(1));

        // Full traversal returns true.
        assertTrue(sr.forEachLong(v -> true));
        assertTrue(sr.forEachLongRange((s, e) -> true));
    }

    @Test
    public void testGetKeysForPositions() {
        final SortedRanges sr = makeSmall(); // {10, [20,30], 50}; positions 0..12
        final ArrayList<Long> out = new ArrayList<>();
        sr.getKeysForPositions(LongStream.of(0, 3, 12).iterator(), out::add);
        assertEquals(java.util.Arrays.asList(10L, 22L, 50L), out);

        // Empty positions iterator returns immediately.
        final ArrayList<Long> none = new ArrayList<>();
        sr.getKeysForPositions(LongStream.empty().iterator(), none::add);
        assertTrue(none.isEmpty());
    }

    @Test
    public void testInvertDegenerate() {
        final SortedRanges sr = makeSmall(); // {10, [20,30], 50}; positions 0..12

        // Invert a sub-range of an existing range.
        final OrderedLongSet inv = sr.ixInvertOnNew(SingleRange.make(22, 25), 100);
        assertSameSet("invert range", inv, new TreeSet<>(java.util.Arrays.asList(3L, 4L, 5L, 6L)));
        inv.ixRelease();

        // maxPosition below the key's position: empty result.
        final OrderedLongSet clipped = sr.ixInvertOnNew(SingleRange.make(50, 50), 5);
        assertEquals(0, clipped.ixCardinality());
        clipped.ixRelease();

        // maxPosition below the position of a sub-range of an existing range: also empty.
        final OrderedLongSet clippedRange = sr.ixInvertOnNew(SingleRange.make(25, 26), 3);
        assertEquals(0, clippedRange.ixCardinality());
        clippedRange.ixRelease();

        // Keys not present throw.
        try {
            sr.ixInvertOnNew(SingleRange.make(15, 15), 100);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testUnsafeVariantsSmall() {
        final TreeSet<Long> model = new TreeSet<>();
        SortedRanges sr = SortedRanges.makeSingleRange(10, 10);
        model.add(10L);
        sr = sr.appendUnsafe(20);
        assertNotNull(sr);
        model.add(20L);
        sr = sr.appendRangeUnsafe(30, 35);
        assertNotNull(sr);
        addRangeToModel(model, 30, 35);
        sr = sr.addUnsafe(25);
        assertNotNull(sr);
        model.add(25L);
        sr = sr.addRangeUnsafe(12, 15);
        assertNotNull(sr);
        addRangeToModel(model, 12, 15);
        sr.validate();
        assertSameSet("unsafe small ops", sr, model);

        // Overlapping insert whose union fits stays SortedRanges-backed (union success path of insertImpl).
        final TreeSet<Long> mA = new TreeSet<>();
        final TreeSet<Long> mB = new TreeSet<>();
        final SortedRanges a = buildSingles(0, 100, 3, mA);
        final SortedRanges b = buildSingles(50, 100, 2, mB);
        final OrderedLongSet inserted = a.ixInsert(b);
        assertTrue(inserted instanceof SortedRanges);
        final TreeSet<Long> expected = new TreeSet<>(mA);
        expected.addAll(mB);
        assertSameSet("overlapping insert union success", inserted, expected);
        inserted.ixRelease();
    }

    @Test
    public void testSharedCopyOnWriteMutations() {
        final TreeSet<Long> model = new TreeSet<>();
        final SortedRanges sr = makeSmall();
        model.add(10L);
        addRangeToModel(model, 20, 30);
        model.add(50L);

        sr.acquire(); // shared: mutations must copy
        try {
            final SortedRanges a1 = sr.add(15);
            assertNotNull(a1);
            assertNotSame(sr, a1);
            final TreeSet<Long> m1 = new TreeSet<>(model);
            m1.add(15L);
            assertSameSet("shared add", a1, m1);
            assertSameSet("shared add orig", sr, model);

            final SortedRanges a2 = sr.addRange(12, 15);
            assertNotNull(a2);
            assertNotSame(sr, a2);
            final TreeSet<Long> m2 = new TreeSet<>(model);
            addRangeToModel(m2, 12, 15);
            assertSameSet("shared addRange", a2, m2);
            assertSameSet("shared addRange orig", sr, model);

            final SortedRanges a3 = sr.remove(25);
            assertNotNull(a3);
            assertNotSame(sr, a3);
            final TreeSet<Long> m3 = new TreeSet<>(model);
            m3.remove(25L);
            assertSameSet("shared remove", a3, m3);
            assertSameSet("shared remove orig", sr, model);

            final SortedRanges a4 = sr.removeRange(22, 24);
            assertNotNull(a4);
            assertNotSame(sr, a4);
            final TreeSet<Long> m4 = new TreeSet<>(model);
            removeRangeFromModel(m4, 22, 24);
            assertSameSet("shared removeRange", a4, m4);
            assertSameSet("shared removeRange orig", sr, model);

            final SortedRanges a5 = sr.append(100);
            assertNotNull(a5);
            assertNotSame(sr, a5);
            final TreeSet<Long> m5 = new TreeSet<>(model);
            m5.add(100L);
            assertSameSet("shared append", a5, m5);
            assertSameSet("shared append orig", sr, model);
        } finally {
            sr.release();
        }
    }

    @Test
    public void testRetainRangeOnSharedSet() {
        final SortedRanges sr = makeSmall(); // {10, [20,30], 50}
        sr.acquire();
        try {
            // Result entirely in a gap: subRangesByKey returns null, ixRetainRange falls back and yields empty.
            final OrderedLongSet empty = sr.ixRetainRange(12, 15);
            empty.ixValidate();
            assertEquals(0, empty.ixCardinality());
            empty.ixRelease();

            // Non-empty result via subRangesByKey on the shared set.
            final OrderedLongSet piece = sr.ixRetainRange(20, 25);
            piece.ixValidate();
            assertEquals(6, piece.ixCardinality());
            assertTrue(piece.ixContainsRange(20, 25));
            piece.ixRelease();
        } finally {
            sr.release();
        }
    }

    @Test
    public void testMiscAccessors() {
        final SortedRanges sr = makeSmall(); // {10, [20,30], 50}
        assertTrue(sr.hasMoreThanOneRange());
        assertFalse(SortedRanges.makeSingleElement(5).hasMoreThanOneRange());
        assertFalse(SortedRanges.makeSingleRange(5, 9).hasMoreThanOneRange());

        final String str = sr.toString();
        assertTrue(str, str.contains("10"));
        assertTrue(str, str.contains("20-30"));
        assertTrue(str, str.contains("50"));
        assertTrue(SortedRanges.makeEmpty().toString().contains("{ }"));
        assertTrue(SortedRanges.makeSingleElement(7).toString().contains("7"));

        assertTrue(sr.bytesAllocated() > 0);
        assertTrue(sr.bytesUsed() > 0);
        assertFalse(sr.isDense());

        // Packed-type fits checks.
        SortedRanges packed = new SortedRangesShort(8, 500);
        packed = packed.add(500);
        assertNotNull(packed);
        assertTrue(packed.fits(500));
        assertFalse(packed.fits(499));
        assertTrue(packed.fits(500 + Short.MAX_VALUE));
        assertFalse(packed.fits(501 + Short.MAX_VALUE));
        assertTrue(packed.fits(500, 600));
        assertFalse(packed.fits(499, 600));
        assertFalse(packed.fits(500, 501 + Short.MAX_VALUE));
        assertTrue(packed.fitsForAppend(500 + Short.MAX_VALUE));
        assertFalse(packed.fitsForAppend(501 + Short.MAX_VALUE));
        assertTrue(packed.bytesAllocated() > 0);
        assertTrue(packed.bytesUsed() > 0);

        final TreeSet<Long> intModel = new TreeSet<>();
        final SortedRanges intSr = buildSingles(0, 1L << 16, 300, intModel);
        assertTrue(intSr instanceof SortedRangesInt);
        assertFalse(intSr.isDense());
        assertTrue(intSr.bytesAllocated() > 0);
        assertTrue(intSr.bytesUsed() > 0);

        // contains/containsRange sanity on the small set.
        assertTrue(sr.contains(10));
        assertFalse(sr.contains(11));
        assertTrue(sr.containsRange(20, 30));
        assertFalse(sr.containsRange(20, 31));
        assertFalse(sr.containsRange(10, 11));
    }
}
