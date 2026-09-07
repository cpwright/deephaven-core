//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.renderRanges;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * The sequential builder rejects a range whose end precedes its start, and any key below zero, whatever it has
 * accumulated so far. Unlike {@link WritableRowSet#insertRange}, which treats an inverted range as empty, the builder
 * throws: accepting the range would shrink a pending range it happens to be adjacent to, or leave the built rowset with
 * a cardinality inconsistent with its ranges and a negative size, and the caller has almost always mis-computed its
 * bounds. The builder is left usable, so a caller that catches the exception can still build what it appended.
 */
public class SequentialBuilderInvertedRangeTest {

    private static void assertBuilds(final String what, final RowSetBuilderSequential builder, final String expected) {
        try (final WritableRowSet rs = builder.build()) {
            rs.validate(what);
            assertEquals(what, expected, renderRanges(rs));
        }
    }

    private static void assertRejected(final RowSetBuilderSequential builder, final long start, final long end) {
        assertThrows(IllegalArgumentException.class, () -> builder.appendRange(start, end));
    }

    @Test
    public void testAsFirstAppend() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        assertRejected(builder, 10, 5);
        assertBuilds("only an inverted range", builder, "");
    }

    @Test
    public void testAsFirstAppendFollowedByMore() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        assertRejected(builder, 10, 5);
        builder.appendRange(20, 30);
        assertBuilds("inverted first range", builder, "20-30 ");
    }

    @Test
    public void testAfterKey() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendKey(1);
        assertRejected(builder, 10, 5);
        assertBuilds("after a key", builder, "1-1 ");
    }

    @Test
    public void testAfterRange() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(1, 2);
        assertRejected(builder, 10, 5);
        builder.appendKey(12);
        assertBuilds("after a range", builder, "1-2 12-12 ");
    }

    /** The inverted range starts right after the pending one, which is the path that merges adjacent ranges. */
    @Test
    public void testAdjacentToPendingRange() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(1, 9);
        assertRejected(builder, 10, 5);
        assertBuilds("adjacent to the pending range", builder, "1-9 ");
    }

    /** The idiom {@code appendRange(start, start + count - 1)} with a count of zero, between two real ranges. */
    @Test
    public void testEmptyCountIdiom() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(1, 2);
        final long start = 100;
        final long count = 0;
        assertRejected(builder, start, start + count - 1);
        builder.appendRange(100, 105);
        assertBuilds("zero count", builder, "1-2 100-105 ");
    }

    /** Enough ranges to move the builder from sorted ranges to a bitmap under construction. */
    @Test
    public void testInBitmapMode() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final StringBuilder expected = new StringBuilder();
        for (int i = 0; i < 20_000; ++i) {
            builder.appendRange(10L * i, 10L * i + 2);
            expected.append(10L * i).append('-').append(10L * i + 2).append(' ');
        }
        assertRejected(builder, 10_000_000, 9_999_990);
        builder.appendKey(10_000_000);
        expected.append("10000000-10000000 ");
        assertBuilds("bitmap mode", builder, expected.toString());
    }

    @Test
    public void testNegativeKey() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        assertThrows(IllegalArgumentException.class, () -> builder.appendKey(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.appendKey(Long.MIN_VALUE));
        builder.appendKey(0);
        assertBuilds("negative key", builder, "0-0 ");
    }

    @Test
    public void testNegativeRangeStart() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        assertRejected(builder, -1, 5);
        assertRejected(builder, Long.MIN_VALUE, Long.MAX_VALUE);
        builder.appendRange(0, 5);
        assertBuilds("negative range start", builder, "0-5 ");
    }
}
