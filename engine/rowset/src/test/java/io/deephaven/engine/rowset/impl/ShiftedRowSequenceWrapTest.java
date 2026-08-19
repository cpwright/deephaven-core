//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ShiftedRowSequence} wrap/reset composition flattening and empty-sequence accessors. The
 * overflow/saturation advance paths are covered by {@link ShiftedRowSequenceTest}.
 */
public class ShiftedRowSequenceWrapTest {

    private static List<Long> collectKeys(final RowSequence rs) {
        final List<Long> keys = new ArrayList<>();
        rs.forAllRowKeys(keys::add);
        return keys;
    }

    @Test
    public void testWrapZeroShiftReturnsSameInstance() {
        try (final RowSet rowSet = RowSetFactory.fromRange(10, 19)) {
            assertSame(rowSet, ShiftedRowSequence.wrap(rowSet, 0));
        }
    }

    @Test
    public void testNestedWrapFlattensShifts() {
        try (final RowSet rowSet = RowSetFactory.fromRange(10, 19)) {
            final RowSequence w1 = ShiftedRowSequence.wrap(rowSet, 5);
            assertTrue(w1 instanceof ShiftedRowSequence);
            assertEquals(15, w1.firstRowKey());
            assertEquals(24, w1.lastRowKey());

            final RowSequence w2 = ShiftedRowSequence.wrap(w1, 3);
            assertTrue(w2 instanceof ShiftedRowSequence);
            assertEquals(10, w2.size());
            assertEquals(18, w2.firstRowKey());
            assertEquals(27, w2.lastRowKey());
            final List<Long> keys = collectKeys(w2);
            assertEquals(10, keys.size());
            for (int ii = 0; ii < 10; ++ii) {
                assertEquals(18L + ii, keys.get(ii).longValue());
            }

            w2.close();
            w1.close();
        }
    }

    @Test
    public void testNetZeroCompositionReturnsUnwrappedSequence() {
        try (final RowSet rowSet = RowSetFactory.fromRange(10, 19)) {
            final RowSequence w1 = ShiftedRowSequence.wrap(rowSet, 5);
            // Wrapping a ShiftedRowSequence with the negated shift flattens to a total shift of zero and must return
            // the original unwrapped sequence.
            assertSame(rowSet, ShiftedRowSequence.wrap(w1, -5));
            w1.close();
        }
    }

    @Test
    public void testResetFlattensShiftedInput() {
        try (final RowSet rowSet = RowSetFactory.fromRange(10, 19)) {
            final RowSequence w1 = ShiftedRowSequence.wrap(rowSet, 5);
            final ShiftedRowSequence reusable = new ShiftedRowSequence();

            final RowSequence composed = reusable.reset(w1, 3);
            assertSame(reusable, composed);
            assertEquals(18, composed.firstRowKey());
            assertEquals(27, composed.lastRowKey());
            assertEquals(10, composed.size());

            // Reset with a plain (unshifted) sequence.
            reusable.reset(rowSet, 2);
            assertEquals(12, reusable.firstRowKey());
            assertEquals(21, reusable.lastRowKey());

            reusable.close();
            w1.close();
        }
    }

    @Test
    public void testEmptyWrappedSequenceAccessors() {
        try (final RowSet empty = RowSetFactory.empty()) {
            final RowSequence wrapped = ShiftedRowSequence.wrap(empty, 5);
            assertTrue(wrapped instanceof ShiftedRowSequence);
            assertTrue(wrapped.isEmpty());
            assertEquals(0, wrapped.size());
            assertEquals(RowSequence.NULL_ROW_KEY, wrapped.firstRowKey());
            assertEquals(RowSequence.NULL_ROW_KEY, wrapped.lastRowKey());
            wrapped.close();
        }
    }
}
