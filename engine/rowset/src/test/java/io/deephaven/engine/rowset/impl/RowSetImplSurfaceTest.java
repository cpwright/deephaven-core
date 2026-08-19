//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.LogicalClockImpl;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Coverage-focused tests for the tracking/writable RowSet implementation surface: previous-value handling, external
 * serialization, resetTo/compact/union/update, interface default methods, and builder helpers.
 */
public class RowSetImplSurfaceTest {

    @Rule
    public final EngineCleanup engineCleanup = new EngineCleanup();

    private static LogicalClockImpl clock() {
        return (LogicalClockImpl) ExecutionContext.getContext().getUpdateGraph().clock();
    }

    @Test
    public void testInitializePreviousValue() {
        final TrackingWritableRowSet ix = RowSetFactory.fromRange(0, 9).toTracking();
        clock().startUpdateCycle();
        ix.insert(100);
        clock().completeUpdateCycle();
        clock().startUpdateCycle();
        ix.insert(200);
        // At this point prev != current.
        assertEquals(11, ix.sizePrev());
        assertEquals(12, ix.size());
        ix.initializePreviousValue();
        // After initializePreviousValue, prev must match current.
        assertEquals(ix.size(), ix.sizePrev());
        assertEquals(ix.firstRowKey(), ix.firstRowKeyPrev());
        assertEquals(ix.lastRowKey(), ix.lastRowKeyPrev());
        try (final WritableRowSet prevCopy = ix.copyPrev()) {
            assertEquals(ix, prevCopy);
        }
        clock().completeUpdateCycle();
    }

    @Test
    public void testGetPrevAndFindPrev() {
        final TrackingWritableRowSet ix = RowSetFactory.fromKeys(1, 3, 5).toTracking();
        // Negative row position always maps to -1.
        assertEquals(-1, ix.getPrev(-1));
        assertEquals(-1, ix.getPrev(-5));

        ix.insert(7);
        // The mutation snapshots prev as of the pre-mutation state {1, 3, 5}.
        assertEquals(5, ix.getPrev(2));
        assertEquals(1, ix.findPrev(3));
        assertTrue(ix.findPrev(2) < 0);
        assertTrue(ix.findPrev(7) < 0);
        assertEquals(3, ix.find(7));

        // Advancing the clock rolls prev forward to the current value.
        clock().startUpdateCycle();
        assertEquals(7, ix.getPrev(3));
        assertEquals(3, ix.findPrev(7));
        assertEquals(-1, ix.getPrev(-1));
        clock().completeUpdateCycle();
    }

    private static final class TestIndexer implements TrackingRowSet.Indexer {
        final TrackingRowSet source;

        TestIndexer(final TrackingRowSet source) {
            this.source = source;
        }
    }

    @Test
    public void testIndexerLazyInitIdentity() {
        final TrackingWritableRowSet ix = RowSetFactory.fromRange(0, 9).toTracking();
        assertNull(ix.indexer());
        final int[] factoryCalls = {0};
        final Function<TrackingRowSet, TestIndexer> factory = rowSet -> {
            ++factoryCalls[0];
            return new TestIndexer(rowSet);
        };
        final TestIndexer indexer1 = ix.indexer(factory);
        final TestIndexer indexer2 = ix.indexer(factory);
        assertSame(indexer1, indexer2);
        assertEquals(1, factoryCalls[0]);
        assertSame(ix, indexer1.source);
        assertSame(indexer1, ix.indexer());
    }

    @Test
    public void testReadExternalResetsPrev() throws IOException, ClassNotFoundException {
        final TrackingWritableRowSet src = RowSetFactory.fromRange(10, 20).toTracking();
        src.insert(100);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            ((TrackingWritableRowSetImpl) src).writeExternal(oos);
        }

        final TrackingWritableRowSetImpl dst =
                (TrackingWritableRowSetImpl) RowSetFactory.fromKeys(1, 2, 3).toTracking();
        clock().startUpdateCycle();
        dst.insert(7); // make prev diverge from current.
        clock().completeUpdateCycle();
        clock().startUpdateCycle();
        dst.insert(9);
        assertEquals(4, dst.sizePrev());
        assertEquals(5, dst.size());

        try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            dst.readExternal(ois);
        }
        // Contents must match the serialized set, and prev must have been reset to the new current value.
        assertEquals(src, dst);
        assertEquals(dst.size(), dst.sizePrev());
        assertEquals(dst.firstRowKey(), dst.firstRowKeyPrev());
        assertEquals(dst.lastRowKey(), dst.lastRowKeyPrev());
        clock().completeUpdateCycle();
    }

    @Test
    public void testResetToIndependence() {
        final WritableRowSet a = RowSetFactory.fromRange(0, 9);
        final WritableRowSet b = RowSetFactory.fromRange(100, 109);
        a.resetTo(b);
        assertEquals(a, b);
        // Mutating the source must not affect the reset target, and vice versa.
        b.insert(500);
        assertEquals(10, a.size());
        assertFalse(a.containsRange(500, 500));
        a.insert(600);
        assertEquals(11, b.size());
        assertFalse(b.containsRange(600, 600));
    }

    @Test
    public void testCompactPreservesMembership() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < 20; ++i) {
            rb = rb.addUnsafe(i * 100_000L);
            rb = rb.addRangeUnsafe(i * 100_000L + 10, i * 100_000L + 12);
        }
        rb.finishMutations();
        try (final WritableRowSetImpl ws = new WritableRowSetImpl(rb);
                final RowSet expected = ws.copy()) {
            ws.compact();
            assertEquals(expected, ws);
            assertEquals(expected.size(), ws.size());
        }
    }

    @Test
    public void testUnionWithSelfReturnsCopy() {
        final WritableRowSet a = RowSetFactory.fromKeys(1, 5, 9);
        try (final WritableRowSet u = a.union(a)) {
            assertNotSame(a, u);
            assertEquals(a, u);
            u.insert(100);
            assertEquals(3, a.size());
            assertFalse(a.containsRange(100, 100));
        }
    }

    @Test
    public void testUpdateSemantics() {
        // Basic update: insert added, remove removed.
        final WritableRowSet a = RowSetFactory.fromRange(0, 9);
        a.update(RowSetFactory.fromRange(20, 24), RowSetFactory.fromRange(5, 7));
        assertEquals(RowSetFactory.fromKeys(0, 1, 2, 3, 4, 8, 9, 20, 21, 22, 23, 24), a);

        // update(this, other) with removed disjoint from this: the receiver re-adds itself, so it must remain
        // unchanged.
        final WritableRowSet b = RowSetFactory.fromRange(0, 9);
        try (final RowSet bOrig = b.copy()) {
            b.update(b, RowSetFactory.fromRange(100, 105));
            assertEquals(bOrig, b);
        }

        // update(this, this): remove everything, then add everything back; the receiver must remain unchanged.
        final WritableRowSet c = RowSetFactory.fromRange(10, 19);
        try (final RowSet cOrig = c.copy()) {
            c.update(c, c);
            assertEquals(cOrig, c);
        }
    }

    @Test
    public void testSubSetForReversePositionsAllPastSize() {
        final WritableRowSet a = RowSetFactory.fromRange(0, 9);
        // Single contiguous range of positions, all past size.
        try (final RowSet positions = RowSetFactory.fromRange(20, 30);
                final WritableRowSet sub = a.subSetForReversePositions(positions)) {
            assertTrue(sub.isEmpty());
        }
        // Non-contiguous positions, all past size.
        try (final RowSet positions = RowSetFactory.fromKeys(15, 22);
                final WritableRowSet sub = a.subSetForReversePositions(positions)) {
            assertTrue(sub.isEmpty());
        }
        // Sanity check: a valid reverse position still works.
        try (final RowSet positions = RowSetFactory.fromKeys(0);
                final WritableRowSet sub = a.subSetForReversePositions(positions)) {
            assertEquals(RowSetFactory.fromKeys(9), sub);
        }
    }

    @Test
    public void testExtractDefault() {
        final WritableRowSet a = RowSetFactory.fromRange(0, 20);
        try (final RowSet other = RowSetFactory.fromKeys(3, 7, 100);
                final WritableRowSet extracted = a.extract(other)) {
            // The returned set is the intersection...
            assertEquals(RowSetFactory.fromKeys(3, 7), extracted);
            // ... and the receiver has been mutated to remove it.
            assertEquals(19, a.size());
            assertFalse(a.containsRange(3, 3));
            assertFalse(a.containsRange(7, 7));
            assertTrue(a.containsRange(0, 2));
            assertTrue(a.containsRange(8, 20));
        }
    }

    @Test
    public void testIsFlat() {
        try (final RowSet empty = RowSetFactory.empty()) {
            assertTrue(empty.isFlat());
        }
        try (final WritableRowSet flat = RowSetFactory.flat(5)) {
            assertTrue(flat.isFlat());
            assertEquals(RowSetFactory.fromRange(0, 4), flat);
        }
        try (final WritableRowSet flat0 = RowSetFactory.flat(0)) {
            assertTrue(flat0.isEmpty());
            assertTrue(flat0.isFlat());
        }
        try (final RowSet notFlat = RowSetFactory.fromRange(1, 5)) {
            assertFalse(notFlat.isFlat());
        }
    }

    @Test
    public void testToRowKeyArrayAndForAllRowKeys() {
        try (final RowSet rs = RowSetFactory.fromKeys(2, 4, 6)) {
            final long[] plain = new long[3];
            rs.toRowKeyArray(plain);
            assertArrayEquals(new long[] {2, 4, 6}, plain);

            final long[] offsetted = new long[5];
            offsetted[0] = -1;
            offsetted[4] = -1;
            rs.toRowKeyArray(offsetted, 1);
            assertArrayEquals(new long[] {-1, 2, 4, 6, -1}, offsetted);

            final List<Long> seen = new ArrayList<>();
            rs.forAllRowKeys(seen::add);
            assertEquals(List.of(2L, 4L, 6L), seen);
        }
    }

    @Test
    public void testRowSetBuilderRandomChunkDefaults() {
        // Unordered long chunk of row keys, including a duplicate.
        try (final WritableLongChunk<RowKeys> chunk = WritableLongChunk.makeWritableChunk(4)) {
            chunk.setSize(0);
            chunk.add(5);
            chunk.add(1);
            chunk.add(9);
            chunk.add(1);
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            builder.addRowKeysChunk(chunk);
            try (final WritableRowSet built = builder.build()) {
                assertEquals(RowSetFactory.fromKeys(1, 5, 9), built);
            }
        }
        // Int chunk of row keys.
        try (final WritableIntChunk<RowKeys> chunk = WritableIntChunk.makeWritableChunk(3)) {
            chunk.setSize(0);
            chunk.add(7);
            chunk.add(3);
            chunk.add(11);
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            builder.addRowKeysChunk(chunk);
            try (final WritableRowSet built = builder.build()) {
                assertEquals(RowSetFactory.fromKeys(3, 7, 11), built);
            }
        }
        // Ordered long chunk, full and offset/length flavors, plus the ordered int flavor.
        try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(4)) {
            chunk.setSize(0);
            chunk.add(10);
            chunk.add(20);
            chunk.add(30);
            chunk.add(40);
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            builder.addOrderedRowKeysChunk(chunk);
            try (final WritableRowSet built = builder.build()) {
                assertEquals(RowSetFactory.fromKeys(10, 20, 30, 40), built);
            }
            final RowSetBuilderRandom builder2 = RowSetFactory.builderRandom();
            builder2.addOrderedRowKeysChunk(chunk, 1, 2);
            try (final WritableRowSet built = builder2.build()) {
                assertEquals(RowSetFactory.fromKeys(20, 30), built);
            }
        }
        try (final WritableIntChunk<OrderedRowKeys> chunk = WritableIntChunk.makeWritableChunk(2)) {
            chunk.setSize(0);
            chunk.add(100);
            chunk.add(200);
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            builder.addOrderedRowKeysChunk(chunk);
            try (final WritableRowSet built = builder.build()) {
                assertEquals(RowSetFactory.fromKeys(100, 200), built);
            }
        }
    }

    @Test
    public void testRangePriorityQueueBuilderResetAndDegenerateRange() {
        final RangePriorityQueueBuilder builder = new RangePriorityQueueBuilder(16);
        builder.addKey(10);
        builder.addRange(20, 25);
        builder.addRange(30, 5); // first > last must be silently ignored.
        assertEquals(2, builder.size());

        final OrderedLongSet first = builder.getOrderedLongSetAndReset();
        try {
            assertEquals(7, first.ixCardinality());
            assertTrue(first.ixContainsRange(10, 10));
            assertTrue(first.ixContainsRange(20, 25));
            assertFalse(first.ixContainsRange(30, 30));
        } finally {
            first.ixRelease();
        }

        // The builder must be reusable after getOrderedLongSetAndReset.
        assertEquals(0, builder.size());
        builder.addRange(100, 101);
        final OrderedLongSet second = builder.getOrderedLongSetAndReset();
        try {
            assertEquals(2, second.ixCardinality());
            assertTrue(second.ixContainsRange(100, 101));
        } finally {
            second.ixRelease();
        }
    }
}
