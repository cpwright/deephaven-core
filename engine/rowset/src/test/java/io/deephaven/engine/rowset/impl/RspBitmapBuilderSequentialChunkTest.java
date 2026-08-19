//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.rsp.DisposableRspBitmap;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import org.junit.Test;

import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the chunk-append and container-donation code paths of {@link RspBitmapBuilderSequential} (and its subclass
 * {@link OrderedLongSetBuilderSequential}).
 *
 * <p>
 * The interesting/untested logic lives in {@code RspBitmapBuilderSequential.appendKeyChunkRb(LongChunk, int, int)} —
 * the chunk append path taken <em>after</em> the builder has already switched to an internal {@link RspBitmap} — and in
 * the multi-block branch of {@code flushRangeToPendingContainer}, plus the {@code disposable} flavor of
 * {@code ensureRb()} which produces a {@link DisposableRspBitmap} whose containers are handed out <em>without</em> a
 * copy-on-write mark.
 * </p>
 */
public class RspBitmapBuilderSequentialChunkTest {

    private static final long BS = RspArray.BLOCK_SIZE;
    private static final long BLOCK_LAST = BS - 1;

    // -----------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------

    private static LongChunk<OrderedRowKeys> chunkOf(final long... keys) {
        return LongChunk.chunkWrap(keys);
    }

    private static long[] contiguous(final long first, final int count) {
        final long[] keys = new long[count];
        for (int i = 0; i < count; ++i) {
            keys[i] = first + i;
        }
        return keys;
    }

    /**
     * Keys spread over {@code blocks} consecutive RSP blocks, {@code perBlock} keys in each, {@code stride} apart. With
     * enough total keys this reliably overflows the {@link io.deephaven.engine.rowset.impl.sortedranges.SortedRanges}
     * representation, forcing an {@link RspBitmap} (and therefore {@code ensureRb()}).
     */
    private static long[] rspForcingKeys(final long base, final int blocks, final int perBlock, final int stride) {
        assertTrue((long) perBlock * stride <= BS);
        final long[] keys = new long[blocks * perBlock];
        int ki = 0;
        for (int b = 0; b < blocks; ++b) {
            final long blockBase = base + (long) b * BS;
            for (int i = 0; i < perBlock; ++i) {
                keys[ki++] = blockBase + (long) i * stride;
            }
        }
        return keys;
    }

    private static TreeSet<Long> model(final long... keys) {
        final TreeSet<Long> out = new TreeSet<>();
        for (final long k : keys) {
            out.add(k);
        }
        return out;
    }

    private static void addRange(final TreeSet<Long> model, final long first, final long last) {
        for (long k = first; k <= last; ++k) {
            model.add(k);
        }
    }

    private static TreeSet<Long> collect(final OrderedLongSet ols) {
        final TreeSet<Long> out = new TreeSet<>();
        ols.ixForEachLong((final long k) -> {
            out.add(k);
            return true;
        });
        return out;
    }

    private static void assertMatches(final String msg, final TreeSet<Long> expected, final OrderedLongSet actual) {
        actual.ixValidate(msg);
        assertEquals(msg + ": cardinality", expected.size(), actual.ixCardinality());
        assertEquals(msg + ": keys", expected, collect(actual));
        if (!expected.isEmpty()) {
            assertEquals(msg + ": first", (long) expected.first(), actual.ixFirstKey());
            assertEquals(msg + ": last", (long) expected.last(), actual.ixLastKey());
        }
    }

    private static void assertMatches(final String msg, final TreeSet<Long> expected, final WritableRowSet actual) {
        assertEquals(msg + ": size", expected.size(), actual.size());
        final TreeSet<Long> got = new TreeSet<>();
        actual.forAllRowKeys(got::add);
        assertEquals(msg + ": keys", expected, got);
    }

    /**
     * Feed the same keys to a chunk-appending builder and to a one-key-at-a-time builder, and into the model.
     */
    private static void appendBoth(
            final RspBitmapBuilderSequential chunkBuilder,
            final RspBitmapBuilderSequential keyBuilder,
            final TreeSet<Long> model,
            final long... keys) {
        chunkBuilder.appendOrderedRowKeysChunk(chunkOf(keys), 0, keys.length);
        for (final long k : keys) {
            keyBuilder.appendKey(k);
            model.add(k);
        }
    }

    /**
     * Build via a sequence of {@code appendKey}/{@code appendRange} ops (a 1-long entry is a key, a 2-long entry is a
     * range) and verify the result against a {@link TreeSet} model.
     */
    private static OrderedLongSet checkScript(final String msg, final long[]... ops) {
        final RspBitmapBuilderSequential b = new RspBitmapBuilderSequential();
        final TreeSet<Long> model = new TreeSet<>();
        for (final long[] op : ops) {
            if (op.length == 1) {
                b.appendKey(op[0]);
                model.add(op[0]);
            } else {
                b.appendRange(op[0], op[1]);
                addRange(model, op[0], op[1]);
            }
        }
        final OrderedLongSet ols = b.getOrderedLongSet();
        assertMatches(msg, model, ols);
        return ols;
    }

    private static long[] key(final long k) {
        return new long[] {k};
    }

    private static long[] range(final long first, final long last) {
        return new long[] {first, last};
    }

    // -----------------------------------------------------------------------------------------------------------
    // 1. appendKeyChunkRb: the rb != null chunk append path.
    // -----------------------------------------------------------------------------------------------------------

    /**
     * Sets up a builder that has already materialized its internal {@link RspBitmap}, and that additionally has both a
     * pending range and (once that range is flushed) a pending container -- so the very first chunk append exercises
     * both flushes at the top of {@code appendKeyChunkRb}.
     */
    private static RspBitmapBuilderSequential rbBuilderWithPendingWork(final TreeSet<Long> model) {
        final RspBitmapBuilderSequential b = new RspBitmapBuilderSequential();
        // A multi-block range flush is what calls ensureRb().
        b.appendRange(0, 3 * BS + 5);
        // Non-adjacent: flushes the range above into the RspBitmap and leaves a pending single-key range.
        b.appendKey(4 * BS);
        if (model != null) {
            addRange(model, 0, 3 * BS + 5);
            model.add(4 * BS);
        }
        return b;
    }

    @Test
    public void testAppendKeyChunkRbAllThreeShapes() {
        final TreeSet<Long> model = new TreeSet<>();
        final RspBitmapBuilderSequential chunkBuilder = rbBuilderWithPendingWork(model);
        final RspBitmapBuilderSequential keyBuilder = rbBuilderWithPendingWork(null);

        // Prove we are really about to take the rb != null branch (the field is package visible).
        assertNotNull(chunkBuilder.rb);
        assertTrue(chunkBuilder.pendingStart != -1);

        // (a) length == 1, in the block the builder is currently working on. This call also drives both of the
        // flushes at the top of appendKeyChunkRb: the pending range (key 4*BS), and the pending container that
        // flushing that range leaves behind.
        appendBoth(chunkBuilder, keyBuilder, model, 4 * BS + 10);
        assertEquals(-1, chunkBuilder.pendingStart);
        assertEquals(-1, chunkBuilder.pendingContainerKey);

        // (b) a contiguous run in the current block.
        appendBoth(chunkBuilder, keyBuilder, model, contiguous(4 * BS + 20, 10));

        // (c) scattered keys, spanning two later blocks.
        appendBoth(chunkBuilder, keyBuilder, model,
                5 * BS + 1, 5 * BS + 7, 6 * BS + 3, 6 * BS + 9, 6 * BS + 100);

        // (b') a contiguous run that crosses a block boundary.
        appendBoth(chunkBuilder, keyBuilder, model, contiguous(8 * BS + BLOCK_LAST - 4, 12));

        // (a') a single key, several blocks later.
        appendBoth(chunkBuilder, keyBuilder, model, 12 * BS + 7);

        // (b'') a contiguous run covering a whole block exactly.
        appendBoth(chunkBuilder, keyBuilder, model, contiguous(13 * BS, (int) BS));

        // (c') scattered keys again, so the builder finishes on the scattered path.
        appendBoth(chunkBuilder, keyBuilder, model,
                14 * BS + 1, 14 * BS + 2, 14 * BS + 9, 15 * BS + 40000, 16 * BS);

        final OrderedLongSet fromChunks = chunkBuilder.getOrderedLongSet();
        final OrderedLongSet fromKeys = keyBuilder.getOrderedLongSet();

        assertTrue(fromChunks instanceof RspBitmap);
        assertMatches("fromChunks", model, fromChunks);
        assertMatches("fromKeys", model, fromKeys);
        // Cross-check the two builders against each other, not just against the model.
        assertTrue(fromChunks.ixSubsetOf(fromKeys));
        assertTrue(fromKeys.ixSubsetOf(fromChunks));

        fromChunks.ixRelease();
        fromKeys.ixRelease();
    }

    @Test
    public void testAppendKeyChunkRbHonorsOffsetAndLength() {
        final TreeSet<Long> model = new TreeSet<>();
        final RspBitmapBuilderSequential chunkBuilder = rbBuilderWithPendingWork(model);
        final RspBitmapBuilderSequential keyBuilder = rbBuilderWithPendingWork(null);
        assertNotNull(chunkBuilder.rb);

        // Padding on both sides of the interesting slice; the padding values would break ordering if read.
        final long[] backing = new long[] {
                Long.MIN_VALUE, Long.MIN_VALUE,
                5 * BS + 3, 5 * BS + 4, 5 * BS + 5, // contiguous run
                Long.MIN_VALUE};
        final LongChunk<OrderedRowKeys> chunk = chunkOf(backing);
        chunkBuilder.appendOrderedRowKeysChunk(chunk, 2, 3);
        for (int i = 2; i < 5; ++i) {
            keyBuilder.appendKey(backing[i]);
            model.add(backing[i]);
        }

        // Also a single-key slice, and a scattered slice, out of a WritableLongChunk.
        try (final WritableLongChunk<OrderedRowKeys> wc = WritableLongChunk.makeWritableChunk(8)) {
            wc.fillWithValue(0, 8, Long.MIN_VALUE);
            wc.set(3, 6 * BS + 1);
            chunkBuilder.appendOrderedRowKeysChunk(wc, 3, 1);
            keyBuilder.appendKey(6 * BS + 1);
            model.add(6 * BS + 1);

            wc.set(1, 7 * BS + 5);
            wc.set(2, 7 * BS + 77);
            wc.set(3, 8 * BS + 9);
            chunkBuilder.appendOrderedRowKeysChunk(wc, 1, 3);
            for (int i = 1; i < 4; ++i) {
                keyBuilder.appendKey(wc.get(i));
                model.add(wc.get(i));
            }
        }

        final OrderedLongSet fromChunks = chunkBuilder.getOrderedLongSet();
        final OrderedLongSet fromKeys = keyBuilder.getOrderedLongSet();
        assertMatches("fromChunks", model, fromChunks);
        assertMatches("fromKeys", model, fromKeys);
        assertTrue(fromChunks.ixSubsetOf(fromKeys));
        assertTrue(fromKeys.ixSubsetOf(fromChunks));
        fromChunks.ixRelease();
        fromKeys.ixRelease();
    }

    /**
     * The production entry point: {@link RowSetBuilderSequential#appendOrderedRowKeysChunk}, on a builder that has
     * already spilled from {@code SortedRanges} into an {@link RspBitmap}.
     */
    @Test
    public void testRowSetBuilderSequentialChunkAppendAfterRspSpill() {
        final TreeSet<Long> model = new TreeSet<>();
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        // Enough scattered keys, spanning several blocks, to overflow SortedRanges and materialize the RspBitmap.
        final long[] spill = rspForcingKeys(0, 6, 2000, 32);
        for (final long k : spill) {
            builder.appendKey(k);
            model.add(k);
        }
        assertNotNull(((RspBitmapBuilderSequential) builder).rb);

        // length == 0 is a no-op even in the rb != null state.
        builder.appendOrderedRowKeysChunk(chunkOf(new long[] {Long.MIN_VALUE}), 0, 0);

        final long base = 20 * BS;
        // single key
        builder.appendOrderedRowKeysChunk(chunkOf(base), 0, 1);
        model.add(base);
        // contiguous run
        final long[] run = contiguous(base + 100, 500);
        builder.appendOrderedRowKeysChunk(chunkOf(run), 0, run.length);
        for (final long k : run) {
            model.add(k);
        }
        // scattered
        final long[] scattered = new long[] {
                base + 1000, base + 1001, base + 5000, 21 * BS, 21 * BS + 3, 25 * BS + 12345};
        builder.appendOrderedRowKeysChunk(chunkOf(scattered), 0, scattered.length);
        for (final long k : scattered) {
            model.add(k);
        }

        try (final WritableRowSet rowSet = builder.build()) {
            assertMatches("builderSequential", model, rowSet);
            ((WritableRowSetImpl) rowSet).getInnerSet().ixValidate("builderSequential");
        }
    }

    /**
     * The {@code rb == null} sibling, {@code appendKeyChunk}, for completeness: all three shapes before any spill.
     */
    @Test
    public void testAppendKeyChunkWithoutRsp() {
        final TreeSet<Long> model = new TreeSet<>();
        final OrderedLongSetBuilderSequential b = new OrderedLongSetBuilderSequential();
        assertNull(b.rb);

        b.appendOrderedRowKeysChunk(chunkOf(10), 0, 1);
        model.add(10L);
        final long[] run = contiguous(100, 50);
        b.appendOrderedRowKeysChunk(chunkOf(run), 0, run.length);
        for (final long k : run) {
            model.add(k);
        }
        final long[] scattered = new long[] {1000, 1002, 1004, 5000};
        b.appendOrderedRowKeysChunk(chunkOf(scattered), 0, scattered.length);
        for (final long k : scattered) {
            model.add(k);
        }
        assertNull(b.rb);

        final OrderedLongSet ols = b.getOrderedLongSet();
        assertMatches("appendKeyChunk", model, ols);
        ols.ixRelease();
    }

    // -----------------------------------------------------------------------------------------------------------
    // 2. flushRangeToPendingContainer: the multi-block branch.
    // -----------------------------------------------------------------------------------------------------------

    /**
     * Initial partial block, mid full-block span, and ending container, where the initial partial block does
     * <em>not</em> belong to the pending container's block (so the pending container must be flushed as-is and a fresh
     * initial container appended).
     */
    @Test
    public void testMultiBlockRangeInitialContainerDoesNotMergePendingContainer() {
        // appendKey(10), appendKey(20) leaves a pending container in block 0; the range starts in block 1.
        final OrderedLongSet ols = checkScript("initial container in a different block",
                key(10), key(20), range(BS + 100, 3 * BS));
        assertTrue(ols instanceof RspBitmap);
        ols.ixRelease();
    }

    /** Same, but the pending container is a bare key (pendingContainer == null) in a different block. */
    @Test
    public void testMultiBlockRangeInitialContainerDoesNotMergeBareKey() {
        checkScript("bare pending key in a different block",
                key(10), range(BS + 100, 3 * BS)).ixRelease();
    }

    /** The pending container <em>does</em> belong to the initial partial block, and is a real container. */
    @Test
    public void testMultiBlockRangeInitialContainerMergesPendingContainer() {
        checkScript("initial container merges pending container",
                key(10), key(20), range(100, 2 * BS + 50)).ixRelease();
    }

    /** The pending container does belong to the initial partial block, but is still a bare key. */
    @Test
    public void testMultiBlockRangeInitialContainerMergesBareKey() {
        checkScript("initial container merges bare pending key",
                key(10), range(100, 2 * BS + 50)).ixRelease();
    }

    /** No initial container (range starts on a block boundary) but there is a pending container to flush first. */
    @Test
    public void testMultiBlockRangeNoInitialContainerWithPendingContainer() {
        checkScript("no initial container, pending container flushed",
                key(10), key(20), range(BS, 3 * BS + 5)).ixRelease();
    }

    /** No mid full-block span: two adjacent blocks, partial in each. */
    @Test
    public void testMultiBlockRangeNoMidFullBlockSpan() {
        checkScript("no mid full block span", range(BS + 100, 2 * BS + 50)).ixRelease();
        checkScript("no mid full block span, with pending container",
                key(5), range(BS + 100, 2 * BS + 50)).ixRelease();
    }

    /** No ending container: the range ends exactly on a block boundary. */
    @Test
    public void testMultiBlockRangeNoEndingContainer() {
        checkScript("no ending container", range(BS + 100, 3 * BS - 1)).ixRelease();
        checkScript("no initial and no ending container", range(BS, 3 * BS - 1)).ixRelease();
        checkScript("no initial and no ending container, single full block", range(BS, 2 * BS - 1)).ixRelease();
    }

    /** A single-block range that exactly covers a whole block takes the full-block-span short path. */
    @Test
    public void testSingleBlockFullBlockSpan() {
        checkScript("single full block", range(BS, 2 * BS - 1), key(3 * BS + 7)).ixRelease();
        checkScript("single full block after pending container",
                key(7), range(BS, 2 * BS - 1)).ixRelease();
    }

    /** A longer mixed script, to shake out interactions between the shapes. */
    @Test
    public void testMixedRangeAndKeyScript() {
        checkScript("mixed",
                key(0),
                key(3),
                range(BLOCK_LAST - 2, BS + 2),
                range(2 * BS + 5, 2 * BS + 9),
                range(3 * BS, 5 * BS - 1),
                key(5 * BS + 1),
                range(5 * BS + 3, 8 * BS + 4),
                key(8 * BS + 6),
                range(9 * BS, 9 * BS + BLOCK_LAST),
                key(10 * BS + 65535),
                range(12 * BS, 12 * BS)).ixRelease();
    }

    // -----------------------------------------------------------------------------------------------------------
    // 3. The disposable / container-stealing path.
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void testFromChunkDisposableProducesDisposableRspBitmap() {
        final long[] keys = rspForcingKeys(0, 10, 1000, 61);
        final OrderedLongSet disposable = OrderedLongSet.fromChunk(chunkOf(keys), 0, keys.length, true);
        try {
            assertTrue("expected a DisposableRspBitmap, got " + disposable.getClass().getName(),
                    disposable instanceof DisposableRspBitmap);
            assertMatches("disposable fromChunk", model(keys), disposable);
        } finally {
            disposable.ixRelease();
        }
    }

    @Test
    public void testFromChunkNonDisposableProducesPlainRspBitmap() {
        final long[] keys = rspForcingKeys(0, 10, 1000, 61);
        final OrderedLongSet shared = OrderedLongSet.fromChunk(chunkOf(keys), 0, keys.length, false);
        try {
            assertTrue(shared instanceof RspBitmap);
            assertFalse(shared instanceof DisposableRspBitmap);
            assertMatches("non-disposable fromChunk", model(keys), shared);
        } finally {
            shared.ixRelease();
        }
    }

    /**
     * The production usage pattern for {@code disposable == true}
     * ({@code SortedRanges.ixInsertSecondHalf}/{@code ixRemoveSecondHalf}, {@code RspBitmap.ixRemoveSecondHalf},
     * {@code SingleRange.ixRemoveSecondHalf}): the disposable set is consumed exactly once and then dropped, so the
     * consumer is free to steal its containers (they are not copy-on-write marked). Verify the consumer's result is
     * correct and remains fully and correctly mutable afterwards.
     */
    @Test
    public void testDisposableSetIsConsumedOnceAndConsumerStaysCorrect() {
        final long[] donatedKeys = rspForcingKeys(0, 10, 1000, 61);
        final long[] baseKeys = rspForcingKeys(5 * BS, 10, 1000, 53);

        final OrderedLongSet donated = OrderedLongSet.fromChunk(chunkOf(donatedKeys), 0, donatedKeys.length, true);
        assertTrue(donated instanceof DisposableRspBitmap);
        assertMatches("donated", model(donatedKeys), donated);

        final OrderedLongSet base = OrderedLongSet.fromChunk(chunkOf(baseKeys), 0, baseKeys.length, false);
        assertTrue(base instanceof RspBitmap);

        final TreeSet<Long> expected = model(donatedKeys);
        expected.addAll(model(baseKeys));

        // base may steal donated's containers here; donated must not be touched again.
        OrderedLongSet merged = base.ixInsert(donated);
        assertMatches("merged", expected, merged);

        // The surviving set is still fully mutable.
        final long addedKey = 30 * BS + 12345;
        merged = merged.ixInsert(addedKey);
        expected.add(addedKey);
        merged = merged.ixRemove(donatedKeys[1]);
        expected.remove(donatedKeys[1]);
        merged = merged.ixRemove(baseKeys[baseKeys.length - 1]);
        expected.remove(baseKeys[baseKeys.length - 1]);
        merged = merged.ixInsertRange(40 * BS, 42 * BS + 7);
        addRange(expected, 40 * BS, 42 * BS + 7);
        assertMatches("merged after mutation", expected, merged);

        merged.ixRelease();
    }

    /**
     * Control for the above: with {@code disposable == false} the donated containers <em>are</em> copy-on-write marked,
     * so both sets stay independently mutable.
     */
    @Test
    public void testNonDisposableSetStaysIndependentAfterDonation() {
        final long[] sharedKeys = rspForcingKeys(0, 10, 1000, 61);
        final long[] baseKeys = rspForcingKeys(5 * BS, 10, 1000, 53);

        OrderedLongSet shared = OrderedLongSet.fromChunk(chunkOf(sharedKeys), 0, sharedKeys.length, false);
        assertTrue(shared instanceof RspBitmap);
        assertFalse(shared instanceof DisposableRspBitmap);

        final OrderedLongSet base = OrderedLongSet.fromChunk(chunkOf(baseKeys), 0, baseKeys.length, false);
        final TreeSet<Long> sharedExpected = model(sharedKeys);
        final TreeSet<Long> mergedExpected = model(sharedKeys);
        mergedExpected.addAll(model(baseKeys));

        OrderedLongSet merged = base.ixInsert(shared);
        assertMatches("merged", mergedExpected, merged);
        assertMatches("shared after donation", sharedExpected, shared);

        // Mutate each side with keys the other side does not get; block 0 is present in both, so this exercises
        // copy-on-write of a genuinely shared container.
        shared = shared.ixInsert(1L);
        sharedExpected.add(1L);
        shared = shared.ixRemove(sharedKeys[1]);
        sharedExpected.remove(sharedKeys[1]);

        merged = merged.ixInsert(2L);
        mergedExpected.add(2L);
        merged = merged.ixRemove(sharedKeys[2]);
        mergedExpected.remove(sharedKeys[2]);

        assertMatches("shared after independent mutation", sharedExpected, shared);
        assertMatches("merged after independent mutation", mergedExpected, merged);

        // And explicitly: neither mutation leaked into the other.
        assertFalse(merged.ixContainsRange(1L, 1L));
        assertFalse(shared.ixContainsRange(2L, 2L));
        assertTrue(merged.ixContainsRange(sharedKeys[1], sharedKeys[1]));
        assertTrue(shared.ixContainsRange(sharedKeys[2], sharedKeys[2]));

        shared.ixRelease();
        merged.ixRelease();
    }

    // -----------------------------------------------------------------------------------------------------------
    // 4. Degenerate inputs.
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void testAppendOrderedRowKeysChunkZeroLengthIsNoOp() {
        final OrderedLongSetBuilderSequential b = new OrderedLongSetBuilderSequential();
        // Values that would be rejected as out of order if they were read.
        b.appendOrderedRowKeysChunk(chunkOf(100, 50, 1), 0, 0);
        assertNull(b.rb);
        assertEquals(-1, b.pendingStart);
        assertEquals(-1, b.pendingContainerKey);
        assertSame(OrderedLongSet.EMPTY, b.getOrderedLongSet());
    }

    @Test
    public void testEmptyBuilderYieldsEmpty() {
        assertSame(OrderedLongSet.EMPTY, new OrderedLongSetBuilderSequential().getOrderedLongSet());
        assertSame(OrderedLongSet.EMPTY, new RspBitmapBuilderSequential().getOrderedLongSet());
        try (final WritableRowSet rowSet = RowSetFactory.builderSequential().build()) {
            assertTrue(rowSet.isEmpty());
        }
    }

    @Test
    public void testFromChunkZeroLengthIsEmpty() {
        assertSame(OrderedLongSet.EMPTY, OrderedLongSet.fromChunk(chunkOf(1, 2, 3), 0, 0, true));
        assertSame(OrderedLongSet.EMPTY, OrderedLongSet.fromChunk(chunkOf(1, 2, 3), 0, 0, false));
    }

    // -----------------------------------------------------------------------------------------------------------
    // 5. A few remaining reachable branches in the same class.
    // -----------------------------------------------------------------------------------------------------------

    /** Out-of-order {@code appendKey} is rejected (when the sequential builder check is enabled). */
    @Test
    public void testAppendKeyOutOfOrderThrows() {
        assumeTrue(OrderedLongSet.BuilderSequential.check);
        final RspBitmapBuilderSequential b = new RspBitmapBuilderSequential();
        b.appendRange(10, 20);
        try {
            b.appendKey(20);
            fail("expected an out-of-order key to be rejected");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("Out of order key"));
        }
    }

    /** Out-of-order {@code appendRange} is rejected (when the sequential builder check is enabled). */
    @Test
    public void testAppendRangeOutOfOrderThrows() {
        assumeTrue(OrderedLongSet.BuilderSequential.check);
        final RspBitmapBuilderSequential b = new RspBitmapBuilderSequential();
        b.appendRange(10, 20);
        try {
            b.appendRange(15, 30);
            fail("expected an out-of-order range to be rejected");
        } catch (final IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("Out of order key"));
        }
    }

    /** {@code appendRange} extending the pending range instead of flushing it. */
    @Test
    public void testAppendRangeExtendsPendingRange() {
        checkScript("adjacent ranges coalesce",
                range(0, 10), range(11, 20), key(21), range(22, BS + 3), key(2 * BS)).ixRelease();
    }

    /** {@code setDomain}, both with a real max and with {@link RowSequence#NULL_ROW_KEY}. */
    @Test
    public void testSetDomainHint() {
        final TreeSet<Long> model = new TreeSet<>();
        final OrderedLongSetBuilderSequential withHint = new OrderedLongSetBuilderSequential();
        withHint.setDomain(0, 4 * BS);
        assertEquals(4 * BS, withHint.maxKeyHint);
        final OrderedLongSetBuilderSequential noHint = new OrderedLongSetBuilderSequential();
        noHint.setDomain(0, RowSequence.NULL_ROW_KEY);
        assertEquals(-1, noHint.maxKeyHint);

        for (long k = 0; k <= 4 * BS; k += 7) {
            withHint.appendKey(k);
            noHint.appendKey(k);
            model.add(k);
        }
        final OrderedLongSet a = withHint.getOrderedLongSet();
        final OrderedLongSet b = noHint.getOrderedLongSet();
        assertMatches("with domain hint", model, a);
        assertMatches("without domain hint", model, b);
        a.ixRelease();
        b.ixRelease();
    }

    /** {@code appendOrderedLongSet}: empty input is a no-op. */
    @Test
    public void testAppendOrderedLongSetEmpty() {
        final RspBitmapBuilderSequential b = new RspBitmapBuilderSequential();
        b.appendOrderedLongSet(1000, OrderedLongSet.EMPTY, true);
        assertSame(OrderedLongSet.EMPTY, b.getOrderedLongSet());
    }

    /** {@code appendOrderedLongSet}: the non-{@link RspBitmap} / {@code rb == null} fallback, via range iteration. */
    @Test
    public void testAppendOrderedLongSetRangeIterationFallback() {
        final long shift = 5 * BS;
        final TreeSet<Long> model = new TreeSet<>();
        final RspBitmapBuilderSequential b = new RspBitmapBuilderSequential();

        // rb == null and the argument is not an RspBitmap.
        final OrderedLongSet single = OrderedLongSet.EMPTY.ixInsertRange(3, 9);
        b.appendOrderedLongSet(shift, single, false);
        addRange(model, 3 + shift, 9 + shift);
        assertNull(b.rb);

        // rb == null but the argument *is* an RspBitmap: still takes the fallback.
        final long[] keys = rspForcingKeys(20 * BS, 4, 1000, 61);
        final OrderedLongSet rsp = OrderedLongSet.fromChunk(chunkOf(keys), 0, keys.length, false);
        assertTrue(rsp instanceof RspBitmap);
        b.appendOrderedLongSet(shift, rsp, false);
        for (final long k : keys) {
            model.add(k + shift);
        }

        final OrderedLongSet ans = b.getOrderedLongSet();
        assertMatches("appendOrderedLongSet fallback", model, ans);
        ans.ixRelease();
        single.ixRelease();
        rsp.ixRelease();
    }

    /**
     * {@code appendOrderedLongSet}: the fast path where the builder already has a non-empty {@link RspBitmap} and the
     * argument is an {@link RspBitmap}, so the spans are appended directly. Also drives the pending-range and
     * pending-container flushes at the top of that method.
     *
     * <p>
     * Only {@code acquire == false} is covered here, which is the only value any production caller passes (see
     * {@link BasicRowSetBuilderSequential#appendRowSequenceWithOffset}). {@code acquire == true} is currently broken:
     * {@code RspArray.tryAppendShiftedUnsafeNoWriteCheck} guards its span-copying loop with {@code if (!acquire)} but
     * increments {@code size} unconditionally, so the appended spans are never written and the result is a corrupt
     * {@code RspArray}.
     * </p>
     */
    @Test
    public void testAppendOrderedLongSetAppendsRspSpansDirectly() {
        for (final long shift : new long[] {0, 30 * BS}) {
            final String msg = "shift=" + shift;
            final TreeSet<Long> model = new TreeSet<>();
            final RspBitmapBuilderSequential b = rbBuilderWithPendingWork(model);
            assertNotNull(b.rb);
            assertTrue(b.pendingStart != -1);

            final long[] keys = rspForcingKeys(50 * BS, 4, 1000, 61);
            final OrderedLongSet rsp = OrderedLongSet.fromChunk(chunkOf(keys), 0, keys.length, false);
            assertTrue(rsp instanceof RspBitmap);
            b.appendOrderedLongSet(shift, rsp, false);
            for (final long k : keys) {
                model.add(k + shift);
            }
            // The donor is unchanged by the append itself. (The builder's result may alias the donor's containers,
            // so we check the donor before the builder finishes mutating.)
            assertMatches(msg + " donor", model(keys), rsp);

            final OrderedLongSet ans = b.getOrderedLongSet();
            assertMatches(msg, model, ans);
            ans.ixRelease();
            rsp.ixRelease();
        }
    }
}
