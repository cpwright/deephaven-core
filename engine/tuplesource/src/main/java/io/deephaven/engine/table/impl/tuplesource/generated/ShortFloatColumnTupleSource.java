//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ShortFloatTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Short and Float.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ShortFloatColumnTupleSource extends AbstractTupleSource<ShortFloatTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ShortFloatColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<ShortFloatTuple, Short, Float> FACTORY = new Factory();

    private final ColumnSource<Short> columnSource1;
    private final ColumnSource<Float> columnSource2;

    public ShortFloatColumnTupleSource(
            @NotNull final ColumnSource<Short> columnSource1,
            @NotNull final ColumnSource<Float> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final ShortFloatTuple createTuple(final long rowKey) {
        return new ShortFloatTuple(
                columnSource1.getShort(rowKey),
                columnSource2.getFloat(rowKey)
        );
    }

    @Override
    public final ShortFloatTuple createPreviousTuple(final long rowKey) {
        return new ShortFloatTuple(
                columnSource1.getPrevShort(rowKey),
                columnSource2.getPrevFloat(rowKey)
        );
    }

    @Override
    public final ShortFloatTuple createTupleFromValues(@NotNull final Object... values) {
        return new ShortFloatTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Float)values[1])
        );
    }

    @Override
    public final ShortFloatTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ShortFloatTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Float)values[1])
        );
    }

    @Override
    public final int tupleLength() {
        return 2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ShortFloatTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ShortFloatTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ShortFloatTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ShortFloatTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ShortFloatTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ShortFloatTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ShortChunk<? extends Values> chunk1 = chunks[0].asShortChunk();
        FloatChunk<? extends Values> chunk2 = chunks[1].asFloatChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ShortFloatTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ShortFloatTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ShortFloatTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ShortFloatColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<ShortFloatTuple, Short, Float> {

        private Factory() {
        }

        @Override
        public TupleSource<ShortFloatTuple> create(
                @NotNull final ColumnSource<Short> columnSource1,
                @NotNull final ColumnSource<Float> columnSource2
        ) {
            return new ShortFloatColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
