//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link SortedRanges#hasMoreThanOneRange()} and the {@link SortedRanges#ixCompact()} collapse it gates.
 *
 * <p>
 * {@code ixCompact} replaces the set with {@code SingleRange.make(first(), last())} when {@code hasMoreThanOneRange()}
 * is false, so a false negative there would fabricate every key between the first and the last -- e.g. reporting
 * {@code {1,3}} as one range would silently add key 2. These tests pin the predicate's whole truth table and assert
 * that compaction never changes membership.
 */
public class SortedRangesCompactTest {

    private static List<Long> keysOf(final OrderedLongSet s) {
        final List<Long> keys = new ArrayList<>();
        s.ixForEachLong(k -> {
            keys.add(k);
            return true;
        });
        return keys;
    }

    private static void assertCompactPreservesMembership(final String m, final SortedRanges sr) {
        final List<Long> before = keysOf(sr);
        final OrderedLongSet compacted = sr.ixCompact();
        assertEquals(m + ": membership", before, keysOf(compacted));
        assertEquals(m + ": cardinality", sr.getCardinality(), compacted.ixCardinality());
        compacted.ixValidate();
    }

    @Test
    public void testHasMoreThanOneRangeTwoSingletons() {
        // Two non-adjacent singletons: two entries, the second positive. This is the shape whose predicate
        // result decides whether ixCompact fabricates the keys in between.
        SortedRanges sr = SortedRanges.makeSingleElement(1);
        sr = sr.add(3);
        assertNotNull(sr);
        assertEquals(2, sr.count());
        assertTrue("{1,3} is two ranges", sr.hasMoreThanOneRange());

        final OrderedLongSet compacted = sr.ixCompact();
        assertFalse("{1,3} must not collapse to a SingleRange", compacted instanceof SingleRange);
        assertEquals(2, compacted.ixCardinality());
        assertEquals(1, compacted.ixFirstKey());
        assertEquals(3, compacted.ixLastKey());
        assertFalse("key 2 must not be invented", compacted.ixContainsRange(2, 2));
        compacted.ixValidate();

        // Widely separated singletons, and singletons at a block boundary.
        for (final long second : new long[] {2 + 1, 100, 65536, 65537, 1L << 40}) {
            SortedRanges two = SortedRanges.makeSingleElement(1);
            two = two.add(second);
            assertNotNull("second=" + second, two);
            final String m = "{1," + second + "}";
            assertTrue(m, two.hasMoreThanOneRange());
            assertCompactPreservesMembership(m, two);
        }
    }

    @Test
    public void testHasMoreThanOneRangeTruthTable() {
        // Empty: no ranges.
        assertFalse(SortedRanges.makeEmpty().hasMoreThanOneRange());
        // One entry: one singleton range.
        assertFalse(SortedRanges.makeSingleElement(5).hasMoreThanOneRange());
        // Two entries, second negative: one range.
        assertFalse(SortedRanges.makeSingleRange(5, 9).hasMoreThanOneRange());
        // Two entries, second positive: two ranges.
        SortedRanges twoSingletons = SortedRanges.makeSingleElement(5);
        twoSingletons = twoSingletons.add(9);
        assertTrue(twoSingletons.hasMoreThanOneRange());
        // Three or more entries is always at least two ranges, in each encoding shape.
        SortedRanges rangeThenSingleton = SortedRanges.makeSingleRange(5, 9);
        rangeThenSingleton = rangeThenSingleton.add(20);
        assertEquals(3, rangeThenSingleton.count());
        assertTrue(rangeThenSingleton.hasMoreThanOneRange());

        SortedRanges singletonThenRange = SortedRanges.makeSingleElement(1);
        singletonThenRange = singletonThenRange.addRange(5, 9);
        assertEquals(3, singletonThenRange.count());
        assertTrue(singletonThenRange.hasMoreThanOneRange());

        SortedRanges threeSingletons = SortedRanges.makeSingleElement(1);
        threeSingletons = threeSingletons.add(5);
        threeSingletons = threeSingletons.add(9);
        assertTrue(threeSingletons.hasMoreThanOneRange());
    }

    @Test
    public void testCompactCollapsesOnlyTrueSingleRanges() {
        // A genuine single range (and a single element) may collapse.
        final SortedRanges oneRange = SortedRanges.makeSingleRange(5, 9);
        final OrderedLongSet collapsedRange = oneRange.ixCompact();
        assertTrue("a true single range may collapse", collapsedRange instanceof SingleRange);
        assertEquals(5, collapsedRange.ixFirstKey());
        assertEquals(9, collapsedRange.ixLastKey());
        assertEquals(5, collapsedRange.ixCardinality());

        final SortedRanges oneElement = SortedRanges.makeSingleElement(7);
        final OrderedLongSet collapsedElement = oneElement.ixCompact();
        assertTrue(collapsedElement instanceof SingleRange);
        assertEquals(1, collapsedElement.ixCardinality());

        assertEquals(OrderedLongSet.EMPTY, SortedRanges.makeEmpty().ixCompact());
    }

    @Test
    public void testCompactPreservesMembershipForManyShapes() {
        // Whatever representation ixCompact picks, the keys must be identical.
        assertCompactPreservesMembership("{1,3}", twoOf(1, 3));
        assertCompactPreservesMembership("{1,3,5}", threeOf(1, 3, 5));
        assertCompactPreservesMembership("{5-9}", SortedRanges.makeSingleRange(5, 9));
        assertCompactPreservesMembership("{5-9,20}", withKey(SortedRanges.makeSingleRange(5, 9), 20));
        assertCompactPreservesMembership("{1,5-9}", rangeAfter(1, 5, 9));
        // Adjacent-but-merged: {5-9} plus 10 is one contiguous range and may legitimately collapse.
        final SortedRanges merged = withKey(SortedRanges.makeSingleRange(5, 9), 10);
        assertFalse("{5-10} is one range", merged.hasMoreThanOneRange());
        assertCompactPreservesMembership("{5-10}", merged);
        // Two ranges separated by exactly one absent key.
        final SortedRanges gapOfOne = withRange(SortedRanges.makeSingleRange(5, 9), 11, 15);
        assertTrue("{5-9,11-15} is two ranges", gapOfOne.hasMoreThanOneRange());
        assertCompactPreservesMembership("{5-9,11-15}", gapOfOne);
    }

    private static SortedRanges twoOf(final long a, final long b) {
        final SortedRanges sr = SortedRanges.makeSingleElement(a).add(b);
        assertNotNull(sr);
        return sr;
    }

    private static SortedRanges threeOf(final long a, final long b, final long c) {
        final SortedRanges sr = twoOf(a, b).add(c);
        assertNotNull(sr);
        return sr;
    }

    private static SortedRanges withKey(final SortedRanges in, final long k) {
        final SortedRanges sr = in.add(k);
        assertNotNull(sr);
        return sr;
    }

    private static SortedRanges withRange(final SortedRanges in, final long first, final long last) {
        final SortedRanges sr = in.addRange(first, last);
        assertNotNull(sr);
        return sr;
    }

    private static SortedRanges rangeAfter(final long single, final long first, final long last) {
        final SortedRanges sr = SortedRanges.makeSingleElement(single).addRange(first, last);
        assertNotNull(sr);
        return sr;
    }
}
