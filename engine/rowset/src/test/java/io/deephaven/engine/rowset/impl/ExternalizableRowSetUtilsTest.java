//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Round-trip tests for {@link ExternalizableRowSetUtils}, focusing on wide deltas that require the LONG_VALUE encoding
 * in both signs.
 */
public class ExternalizableRowSetUtilsTest {

    private static RowSet roundTrip(final RowSet rowSet) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final DataOutputStream out = new DataOutputStream(baos)) {
            ExternalizableRowSetUtils.writeExternalCompressedDeltas(out, rowSet);
        }
        try (final DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            return ExternalizableRowSetUtils.readExternalCompressedDelta(in);
        }
    }

    @Test
    public void testRoundTripLongValueDeltasBothSigns() throws IOException {
        // The delta from key 0 to 2^40 exceeds Integer.MAX_VALUE (positive LONG_VALUE); the range length of 2^35
        // exceeds Integer.MAX_VALUE as well, forcing a negated (negative) LONG_VALUE for the range end.
        final long rangeStart = 1L << 40;
        final long rangeEnd = rangeStart + (1L << 35);
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendKey(0);
        builder.appendRange(rangeStart, rangeEnd);
        try (final RowSet original = builder.build();
                final RowSet read = roundTrip(original)) {
            assertEquals(original, read);
        }
    }

    @Test
    public void testRoundTripEmpty() throws IOException {
        try (final RowSet original = RowSetFactory.empty();
                final RowSet read = roundTrip(original)) {
            assertTrue(read.isEmpty());
            assertEquals(original, read);
        }
    }

    @Test
    public void testRoundTripMixedValueWidths() throws IOException {
        // Deltas spanning byte, short, and int widths, with both singletons and ranges.
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 4); // byte deltas
        builder.appendKey(1000); // short delta
        builder.appendRange(100_000, 100_010); // int delta start, byte delta end
        builder.appendKey(2_000_000_000L); // int delta
        try (final RowSet original = builder.build();
                final RowSet read = roundTrip(original)) {
            assertEquals(original, read);
        }
    }
}
