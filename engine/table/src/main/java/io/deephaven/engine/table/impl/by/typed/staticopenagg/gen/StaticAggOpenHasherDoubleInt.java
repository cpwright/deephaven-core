// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.staticopenagg.gen;

import static io.deephaven.util.compare.DoubleComparisons.eq;
import static io.deephaven.util.compare.IntComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.DoubleChunkHasher;
import io.deephaven.chunk.util.hashing.IntChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sort.timsort.IntIntTimsortKernel;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableDoubleArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticAggOpenHasherDoubleInt extends StaticChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private final ImmutableDoubleArraySource mainKeySource0;

    private final ImmutableIntArraySource mainKeySource1;

    public StaticAggOpenHasherDoubleInt(ColumnSource[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableDoubleArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
        this.mainKeySource1 = (ImmutableIntArraySource) super.mainKeySources[1];
        this.mainKeySource1.ensureCapacity(tableSize);
    }

    @Override
    protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final IntChunk<Values> keyChunk1 = sourceKeyChunks[1].asIntChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            int tableLocation = hashToTableLocation(hash);
            final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
            while (true) {
                if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_OUTPUT_POSITION) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    mainKeySource1.set(tableLocation, k1);
                    handler.doMainInsert(tableLocation, chunkPosition);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                    handler.doMainFound(tableLocation, chunkPosition);
                    break;
                } else {
                    Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                    tableLocation = (tableLocation + 1) & (tableSize - 1);
                }
            }
        }
    }

    private static int hash(double k0, int k1) {
        int hash = DoubleChunkHasher.hashInitialSingle(k0);
        hash = IntChunkHasher.hashUpdateSingle(hash, k1);
        return hash;
    }

    @Override
    protected void rehashInternal(HashHandler handler) {
        final int entries = (int)numEntries;
        try (final WritableIntChunk<ChunkPositions> moveMainSource = WritableIntChunk.makeWritableChunk(entries);
        final WritableIntChunk<ChunkPositions> moveMainDest = WritableIntChunk.makeWritableChunk(entries)) {
            moveMainSource.setSize(entries);
            moveMainDest.setSize(entries);
            int startMovePointer = 0;
            int endMovePointer = entries - 1;
            final int oldSize = tableSize >> 1;
            final double[] destArray0 = new double[tableSize];
            final int[] destArray1 = new int[tableSize];
            final int[] destState = new int[tableSize];
            Arrays.fill(destState, EMPTY_OUTPUT_POSITION);
            for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
                if (mainOutputPosition.getUnsafe(sourceBucket) == EMPTY_OUTPUT_POSITION) {
                    continue;
                }
                final double k0 = mainKeySource0.getUnsafe(sourceBucket);
                final int k1 = mainKeySource1.getUnsafe(sourceBucket);
                final int hash = hash(k0, k1);
                int tableLocation = hashToTableLocation(hash);
                final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
                while (true) {
                    if (destState[tableLocation] == EMPTY_OUTPUT_POSITION) {
                        destArray0[tableLocation] = k0;
                        destArray1[tableLocation] = k1;
                        destState[tableLocation] = mainOutputPosition.getUnsafe(sourceBucket);
                        if (sourceBucket != tableLocation) {
                            if (tableLocation < oldSize) {
                                moveMainSource.set(startMovePointer, sourceBucket);
                                moveMainDest.set(startMovePointer++, tableLocation);
                            } else {
                                moveMainSource.set(endMovePointer, sourceBucket);
                                moveMainDest.set(endMovePointer--, tableLocation);
                            }
                        }
                        break;
                    } else {
                        Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                        tableLocation = (tableLocation + 1) & (tableSize - 1);
                    }
                }
            }
            mainKeySource0.setArray(destArray0);
            mainKeySource1.setArray(destArray1);
            mainOutputPosition.setArray(destState);
            for (int ii = endMovePointer + 1; ii < entries; ++ii) {
                handler.doMoveMain(moveMainSource.get(ii), moveMainDest.get(ii));
            }
            moveMainSource.setSize(startMovePointer);;
            moveMainDest.setSize(startMovePointer);;
            try (final IntIntTimsortKernel.IntIntSortKernelContext sortContext = IntIntTimsortKernel.createContext((int)numEntries)) {
                sortContext.sort(moveMainSource, moveMainDest);
            }
            for (int ii = moveMainSource.size() - 1; ii >= 0; --ii) {
                handler.doMoveMain(moveMainSource.get(ii), moveMainDest.get(ii));
            }
        }
    }

    @Override
    public int findPositionForKey(Object key) {
        final Object [] ka = (Object[])key;
        final double k0 = TypeUtils.unbox((Double)ka[0]);
        final int k1 = TypeUtils.unbox((Integer)ka[1]);
        int hash = hash(k0, k1);
        int tableLocation = hashToTableLocation(hash);
        final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (positionValue == EMPTY_OUTPUT_POSITION) {
                return -1;
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                return positionValue;
            }
            Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
            tableLocation = (tableLocation + 1) & (tableSize - 1);
        }
    }
}
