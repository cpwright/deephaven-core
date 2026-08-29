//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import org.junit.Test;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Coverage-focused tests for the {@code RspArray} containment and overlap predicates, concentrating on the negative
 * (false) answers and on the span-shape combinations (full block span vs container vs singleton) that are otherwise
 * exercised only in their "true" direction. These are the branches where a mistake produces a wrong answer rather than
 * an exception.
 */
public class RspArrayPredicateTest {

    private static final long BLK = BLOCK_SIZE;

    private static RspBitmap of(final long... values) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (final long v : values) {
            rb = rb.addUnsafe(v);
        }
        rb.finishMutations();
        return rb;
    }

    private static RspBitmap ofBlocks(final long firstBlock, final long lastBlockInclusive) {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(firstBlock * BLK, (lastBlockInclusive + 1) * BLK - 1);
        return rb;
    }

    /**
     * The receiver's full block span extends past the end of the span in the argument that covers its first block.
     * <p>
     * Reaching the {@code uLess(kend2, kend1) -> false} comparison requires defeating the cardinality short-circuit in
     * {@code subsetOf(RspArray)}: the argument must have a strictly larger cardinality than the receiver, so the extra
     * (disjoint) full block span in {@code r2} below is load bearing.
     */
    @Test
    public void testSubsetOfFullSpanExtendsPastCoveringSpan() {
        RspBitmap r1 = RspBitmap.makeEmpty();
        r1 = r1.addRange(4 * BLK, 6 * BLK - 1); // full blocks 4-5.

        RspBitmap r2 = RspBitmap.makeEmpty();
        r2 = r2.addRange(4 * BLK, 5 * BLK - 1); // full block 4 only...
        r2 = r2.addRange(10 * BLK, 15 * BLK - 1); // ... plus disjoint blocks 10-14, to inflate the cardinality.

        assertTrue("cardinality caches must be populated for this shape",
                r1.isCardinalityCached() && r2.isCardinalityCached());
        assertTrue("the argument must be larger, or the cardinality short-circuit answers first",
                r2.getCardinality() > r1.getCardinality());

        // Block 5 of r1 is entirely missing from r2.
        assertFalse(r1.subsetOf(r2));
        // Sanity: the value that makes the answer false really is absent.
        assertTrue(r1.contains(5 * BLK));
        assertFalse(r2.contains(5 * BLK));

        // The same shape one block shorter is a subset.
        RspBitmap r1Short = RspBitmap.makeEmpty();
        r1Short = r1Short.addRange(4 * BLK, 5 * BLK - 1);
        assertTrue(r1Short.subsetOf(r2));
    }

    /**
     * Full block span in the receiver whose last block key matches the covering span's last block key, with the
     * argument exhausted afterwards. The receiver has one more span, so the answer is false; the argument is a longer
     * full block span so that its cardinality is larger and the short-circuit does not answer first.
     */
    @Test
    public void testSubsetOfArgumentExhaustedWithSpansRemaining() {
        RspBitmap r1 = RspBitmap.makeEmpty();
        r1 = r1.addRange(4 * BLK, 6 * BLK - 1); // full blocks 4-5.
        r1 = r1.addUnsafe(8 * BLK + 1); // ... and an extra span past the argument's end.
        r1.finishMutations();

        final RspBitmap r2 = ofBlocks(2, 5); // full blocks 2-5: one span, ending at the same block as r1's span 0.
        assertEquals(1, r2.size);
        assertTrue(r2.getCardinality() > r1.getCardinality());

        assertFalse(r1.subsetOf(r2));

        // Dropping the extra span makes it a subset (argument exhausted on the last receiver span).
        final RspBitmap r1NoExtra = ofBlocks(4, 5);
        assertTrue(r1NoExtra.subsetOf(r2));
        // And the same span exactly, which also exhausts the argument.
        assertTrue(r2.subsetOf(r2.deepCopy()));
    }

    @Test
    public void testSubsetOfFullSpanAgainstContainerAndSingleton() {
        final RspBitmap fullBlocks = ofBlocks(4, 5);

        // Argument has a container (not a full block span) at the receiver's first block key.
        RspBitmap container = RspBitmap.makeEmpty();
        container = container.addRange(4 * BLK, 4 * BLK + 10);
        container = container.addRange(5 * BLK, 8 * BLK - 1); // extra blocks to keep the cardinality larger.
        assertTrue(container.getCardinality() > fullBlocks.getCardinality());
        assertFalse(fullBlocks.subsetOf(container));

        // Argument has a singleton span at the receiver's first block key.
        RspBitmap singletons = RspBitmap.makeEmpty();
        singletons = singletons.addUnsafe(4 * BLK + 3);
        singletons.finishMutations();
        singletons = singletons.addRange(5 * BLK, 9 * BLK - 1);
        assertTrue(singletons.getCardinality() > fullBlocks.getCardinality());
        assertFalse(fullBlocks.subsetOf(singletons));
    }

    @Test
    public void testSubsetOfSingletonSpans() {
        // Two singleton spans in the same block with different values.
        final RspBitmap r1 = of(BLK + 5);
        final RspBitmap r2 = of(BLK + 9, 5 * BLK + 1);
        assertTrue(r2.getCardinality() > r1.getCardinality());
        assertTrue(r1.size == 1 && r2.size == 2);
        assertFalse(r1.subsetOf(r2));

        // Matching singleton spans.
        final RspBitmap r3 = of(BLK + 9);
        assertTrue(r3.subsetOf(r2));

        // A singleton receiver against a container argument, contained and not contained.
        final RspBitmap containerArg = of(BLK + 1, BLK + 5, BLK + 9);
        assertTrue(of(BLK + 5).subsetOf(containerArg));
        assertFalse(of(BLK + 7).subsetOf(containerArg));

        // A container receiver against a singleton argument in the same block.
        final RspBitmap containerRcv = of(BLK + 1, BLK + 5);
        final RspBitmap singletonArg = of(BLK + 1, 4 * BLK + 1, 7 * BLK + 1);
        assertTrue(singletonArg.getCardinality() > containerRcv.getCardinality());
        assertFalse(containerRcv.subsetOf(singletonArg));
    }

    @Test
    public void testSubsetOfContainerAgainstContainer() {
        final RspBitmap r1 = of(BLK + 1, BLK + 5, BLK + 100);
        final RspBitmap subset = of(BLK + 1, BLK + 5);
        final RspBitmap notSubset = of(BLK + 1, BLK + 5, BLK + 101);
        assertTrue(subset.subsetOf(r1));
        assertFalse(notSubset.subsetOf(r1));
        assertTrue(r1.subsetOf(r1.deepCopy()));
    }

    /** The receiver's block key has no covering span at all in the argument. */
    @Test
    public void testSubsetOfBlockKeyNotPresentInArgument() {
        final RspBitmap r1 = of(BLK + 1);
        final RspBitmap r2 = of(5 * BLK + 1, 6 * BLK + 2);
        assertTrue(r2.getCardinality() > r1.getCardinality());
        assertFalse(r1.subsetOf(r2));

        // Same, with a block key past everything in the argument.
        final RspBitmap r3 = of(20 * BLK + 1);
        assertFalse(r3.subsetOf(r2));
    }

    @Test
    public void testSubsetOfEmptyFastPaths() {
        final RspBitmap empty = RspBitmap.makeEmpty();
        empty.finishMutations();
        final RspBitmap nonEmpty = of(BLK + 1, 3 * BLK + 7);

        assertTrue(empty.subsetOf(empty));
        assertTrue(empty.subsetOf(nonEmpty));
        assertFalse(nonEmpty.subsetOf(empty));
        // The cardinality short-circuit.
        assertFalse(nonEmpty.subsetOf(of(BLK + 1)));
    }

    /**
     * {@code overlaps} orders its arguments so that the array with fewer spans plays the {@code r1} role, so the
     * one-span bitmap below is the one whose container is probed against the other's singleton span.
     */
    @Test
    public void testOverlapsContainerAgainstSingleton() {
        final RspBitmap oneSpanContainer = of(1, 2, 3); // one span, a container in block 0.
        final RspBitmap twoSpans = of(2, BLK + 1, BLK + 2); // singleton span in block 0, container in block 1.
        assertEquals(1, oneSpanContainer.size);
        assertEquals(2, twoSpans.size);

        assertTrue(oneSpanContainer.overlaps(twoSpans));
        assertTrue(twoSpans.overlaps(oneSpanContainer));

        // The non-intersecting twin: the singleton's value is not in the container.
        final RspBitmap disjointSingleton = of(5, BLK + 1, BLK + 2);
        assertFalse(oneSpanContainer.overlaps(disjointSingleton));
        assertFalse(disjointSingleton.overlaps(oneSpanContainer));

        // Singleton against singleton, both directions of the value comparison.
        final RspBitmap s1 = of(2);
        assertTrue(s1.overlaps(twoSpans));
        assertFalse(of(4).overlaps(twoSpans));

        // Container against container in the same block.
        assertTrue(of(3, 4).overlaps(oneSpanContainer));
        assertFalse(of(4, 5).overlaps(oneSpanContainer));
    }

    @Test
    public void testOverlapsFullBlockSpans() {
        final RspBitmap fullSpan = ofBlocks(4, 6);
        // A container inside the full block span's range.
        assertTrue(fullSpan.overlaps(of(5 * BLK + 7, 20 * BLK + 1)));
        // A singleton inside the full block span's range.
        assertTrue(fullSpan.overlaps(of(6 * BLK + 7)));
        // Nothing in the covered blocks.
        assertFalse(fullSpan.overlaps(of(3 * BLK + 7, 7 * BLK + 7)));
        assertFalse(fullSpan.overlaps(of(BLK + 1)));
        // Empty operands.
        final RspBitmap empty = RspBitmap.makeEmpty();
        empty.finishMutations();
        assertFalse(fullSpan.overlaps(empty));
        assertFalse(empty.overlaps(fullSpan));
    }

    /** A query range that spans a block missing between two present full block spans. */
    @Test
    public void testContainsRangeAcrossMissingBlockBetweenFullSpans() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(3 * BLK, 5 * BLK - 1); // full blocks 3-4.
        rb = rb.addRange(6 * BLK, 8 * BLK - 1); // full blocks 6-7; block 5 is missing.
        assertEquals(2, rb.size);

        assertFalse(rb.containsRange(3 * BLK, 7 * BLK - 1));
        assertFalse(rb.containsRange(4 * BLK + 5, 6 * BLK + 5));
        assertFalse(rb.containsRange(5 * BLK, 5 * BLK));
        assertFalse(rb.containsRange(4 * BLK, 5 * BLK));
        // Ranges entirely inside one full block span, or covering it exactly.
        assertTrue(rb.containsRange(3 * BLK, 5 * BLK - 1));
        assertTrue(rb.containsRange(3 * BLK + 7, 4 * BLK + 9));
        assertTrue(rb.containsRange(6 * BLK, 8 * BLK - 1));
        assertTrue(rb.containsRange(7 * BLK - 1, 7 * BLK));
        // A range starting before everything present.
        assertFalse(rb.containsRange(2 * BLK, 3 * BLK));
        // A range past everything present.
        assertFalse(rb.containsRange(8 * BLK, 8 * BLK + 1));
    }

    /**
     * A query range that walks off the end of a full block span and continues into the container in the immediately
     * following block, exercising the "advance pendingStart past the full block span" step.
     */
    @Test
    public void testContainsRangeFromFullSpanIntoNextBlock() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(3 * BLK, 5 * BLK - 1); // full blocks 3-4.
        rb = rb.addRange(5 * BLK, 5 * BLK + 100); // partial block 5.
        assertEquals(2, rb.size);

        assertTrue(rb.containsRange(3 * BLK, 5 * BLK + 100));
        assertTrue(rb.containsRange(4 * BLK + 7, 5 * BLK));
        assertTrue(rb.containsRange(3 * BLK, 5 * BLK + 50));
        assertFalse(rb.containsRange(3 * BLK, 5 * BLK + 101));
        assertFalse(rb.containsRange(4 * BLK, 6 * BLK));

        // Singleton span immediately after a full block span, hit from inside the span.
        RspBitmap rb2 = RspBitmap.makeEmpty();
        rb2 = rb2.addRange(3 * BLK, 5 * BLK - 1); // full blocks 3-4.
        rb2 = rb2.add(5 * BLK); // singleton span in block 5.
        assertEquals(2, rb2.size);
        assertTrue(rb2.containsRange(4 * BLK + 1, 5 * BLK));
        assertFalse(rb2.containsRange(4 * BLK + 1, 5 * BLK + 1));
    }

    /**
     * {@code overlapsRange} against singleton spans that fall outside the queried range, in both directions, including
     * the index-returning form and its "resume from an index" contract.
     */
    @Test
    public void testOverlapsRangeAgainstSingletonSpansOutsideRange() {
        final RspBitmap rb = of(2 * BLK + 10, 6 * BLK + 10, 9 * BLK + 10);
        assertEquals(3, rb.size);

        // Range entirely above / below the singleton in the block it lands in.
        assertFalse(rb.overlapsRange(2 * BLK + 11, 2 * BLK + 500));
        assertFalse(rb.overlapsRange(2 * BLK, 2 * BLK + 9));
        assertFalse(rb.overlapsRange(6 * BLK + 11, 6 * BLK + 4000));
        assertFalse(rb.overlapsRange(6 * BLK, 6 * BLK + 9));
        // Ranges in blocks with nothing at all.
        assertFalse(rb.overlapsRange(3 * BLK, 5 * BLK - 1));
        assertFalse(rb.overlapsRange(0, BLK - 1));
        assertFalse(rb.overlapsRange(10 * BLK, 20 * BLK));
        // A range that crosses two blocks but misses both singletons.
        assertFalse(rb.overlapsRange(2 * BLK + 11, 6 * BLK + 9));

        // True cases: exactly on the singleton, and a range covering a whole block.
        assertTrue(rb.overlapsRange(2 * BLK + 10, 2 * BLK + 10));
        assertTrue(rb.overlapsRange(2 * BLK, 2 * BLK + 10));
        assertTrue(rb.overlapsRange(6 * BLK, 7 * BLK - 1));
        assertTrue(rb.overlapsRange(0, Long.MAX_VALUE));

        // The index-returning form: a non-negative result is the index of an overlapping span; a negative result
        // encodes where to resume.
        assertEquals(1, rb.overlapsRange(0, 6 * BLK + 10, 6 * BLK + 10));
        assertTrue(rb.overlapsRange(0, 3 * BLK, 5 * BLK - 1) < 0);
        assertTrue(rb.overlapsRange(2, 2 * BLK + 10, 2 * BLK + 10) < 0);
        assertTrue(rb.overlapsRange(0, 20 * BLK, 21 * BLK) < 0);
    }
}
