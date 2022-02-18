/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

public abstract class IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase
        extends OperatorAggregationStateManagerOpenAddressedAlternateBase
        implements IncrementalOperatorAggregationStateManager {
    // our state value used when nothing is there
    protected static final int EMPTY_OUTPUT_POSITION = QueryConstants.NULL_INT;

    // the state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation)
    protected ImmutableIntArraySource mainOutputPosition = new ImmutableIntArraySource();

    // the state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation)
    protected ImmutableIntArraySource alternateOutputPosition;

    // used as a row redirection for the output key sources
    protected final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    // how many values are in each state, addressed by output row key
    protected final LongArraySource rowCountSource = new LongArraySource();

    // state variables that exist as part of the update
    protected MutableInt nextOutputPosition;
    protected WritableIntChunk<RowKeys> outputPositions;

    // output alternating column sources
    protected AlternatingColumnSource[] alternatingColumnSources;
    protected int mainInsertMask = 0;

    protected IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase(ColumnSource<?>[] tableKeySources,
            int tableSize,
            double maximumLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        mainOutputPosition.ensureCapacity(tableSize);
    }

    @Override
    protected void newAlternate() {
        alternateOutputPosition = mainOutputPosition;
        mainOutputPosition = new ImmutableIntArraySource();
        mainOutputPosition.ensureCapacity(tableSize);
        if (mainIsAlternate) {
            if (alternatingColumnSources != null) {
                for (int ai = 0; ai < alternatingColumnSources.length; ++ai) {
                    alternatingColumnSources[ai].setSources(alternateKeySources[ai], mainKeySources[ai]);
                }
            }
            mainInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
        } else {
            if (alternatingColumnSources != null) {
                for (int ai = 0; ai < alternatingColumnSources.length; ++ai) {
                    alternatingColumnSources[ai].setSources(mainKeySources[ai], alternateKeySources[ai]);
                }
            }
            mainInsertMask = 0;
        }
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return makeBuildContext(buildSources, maxSize);
    }

    @Override
    public void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.nextOutputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        buildTable((BuildContext) bc, rowSequence, sources, true, this::build);
        this.outputPositions = null;
        this.nextOutputPosition = null;
    }

    @Override
    public void onNextChunk(int size) {
        outputPositionToHashSlot.ensureCapacity(nextOutputPosition.intValue() + size);
        rowCountSource.ensureCapacity(nextOutputPosition.intValue() + size);
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final RowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[mainKeySources.length];
        Assert.eqNull(alternatingColumnSources, "alternatingColumnSources");
        alternatingColumnSources = new AlternatingColumnSource[mainKeySources.length];
        for (int kci = 0; kci < mainKeySources.length; ++kci) {
            final Class<?> dataType =
                    mainKeySources[kci] != null ? mainKeySources[kci].getType() : alternateKeySources[kci].getType();
            final Class<?> componentType = mainKeySources[kci] != null ? mainKeySources[kci].getComponentType()
                    : alternateKeySources[kci].getComponentType();
            if (mainIsAlternate) {
                alternatingColumnSources[kci] = new AlternatingColumnSource<>(dataType, componentType,
                        alternateKeySources[kci], mainKeySources[kci]);
            } else {
                alternatingColumnSources[kci] = new AlternatingColumnSource<>(dataType, componentType,
                        mainKeySources[kci], alternateKeySources[kci]);
            }
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultIndexToHashSlot, alternatingColumnSources[kci]);
        }

        return keyHashTableSources;
    }

    @Override
    public void addForUpdate(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions,
            final WritableIntChunk<RowKeys> reincarnatedPositions) {
        outputPositions.setSize(rowSequence.intSize());
        reincarnatedPositions.setSize(0);
        if (rowSequence.isEmpty()) {
            return;
        }
        this.nextOutputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        buildTable((BuildContext) bc, rowSequence, sources, false,
                ((chunkOk, sourceKeyChunks) -> buildForUpdate(chunkOk, sourceKeyChunks, reincarnatedPositions)));
        this.outputPositions = null;
        this.nextOutputPosition = null;
    }

    protected abstract void buildForUpdate(RowSequence chunkOk, Chunk[] sourceKeyChunks,
            WritableIntChunk<RowKeys> reincarnatedPositions);

    @Override
    public void remove(final SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions, final WritableIntChunk<RowKeys> emptiedPositions) {
        outputPositions.setSize(rowSequence.intSize());
        emptiedPositions.setSize(0);
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        probeTable((ProbeContext) pc, rowSequence, true, sources,
                (chunkOk, sourceKeyChunks) -> IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.this
                        .doRemoveProbe(chunkOk, sourceKeyChunks, emptiedPositions));
        this.outputPositions = null;
    }

    protected abstract void doRemoveProbe(RowSequence chunkOk, Chunk[] sourceKeyChunks,
            WritableIntChunk<RowKeys> emptiedPositions);

    @Override
    public void findModifications(final SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        probeTable((ProbeContext) pc, rowSequence, false, sources,
                IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.this::doModifyProbe);
        this.outputPositions = null;
    }

    protected abstract void doModifyProbe(RowSequence chunkOk, Chunk[] sourceKeyChunks);

    @Override
    public void startTrackingPrevValues() {}

    @Override
    public void setRowSize(int outputPosition, long size) {
        rowCountSource.set(outputPosition, size);
    }
}
