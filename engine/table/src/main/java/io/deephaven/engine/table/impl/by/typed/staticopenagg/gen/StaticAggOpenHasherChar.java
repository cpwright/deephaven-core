// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.staticopenagg.gen;

import static io.deephaven.util.compare.CharComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableCharArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Character;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticAggOpenHasherChar extends StaticChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private final ImmutableCharArraySource mainKeySource0;

    public StaticAggOpenHasherChar(ColumnSource[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableCharArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    @Override
    protected void build(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            int tableLocation = hashToTableLocation(hash);
            final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
            while (true) {
                int tableState = mainOutputPosition.getUnsafe(tableLocation);
                if (tableState == EMPTY_OUTPUT_POSITION) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final int nextOutputPosition = outputPosition.getAndIncrement();
                    outputPositions.set(chunkPosition, nextOutputPosition);
                    mainOutputPosition.set(tableLocation, nextOutputPosition);
                    outputPositionToHashSlot.set(nextOutputPosition, tableLocation);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, tableState);
                    break;
                } else {
                    Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                    tableLocation = (tableLocation + 1) & (tableSize - 1);
                }
            }
        }
    }

    private static int hash(char k0) {
        int hash = CharChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    @Override
    protected void rehashInternal() {
        final int oldSize = tableSize >> 1;
        final char[] destArray0 = new char[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_OUTPUT_POSITION);
        final char [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destArray0);
        final int [] originalStateArray = mainOutputPosition.getArray();
        mainOutputPosition.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            if (originalStateArray[sourceBucket] == EMPTY_OUTPUT_POSITION) {
                continue;
            }
            final char k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            int tableLocation = hashToTableLocation(hash);
            final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
            while (true) {
                if (destState[tableLocation] == EMPTY_OUTPUT_POSITION) {
                    destArray0[tableLocation] = k0;
                    destState[tableLocation] = originalStateArray[sourceBucket];
                    if (sourceBucket != tableLocation) {
                        outputPositionToHashSlot.set(destState[tableLocation], tableLocation);
                    }
                    break;
                } else {
                    Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
                    tableLocation = (tableLocation + 1) & (tableSize - 1);
                }
            }
        }
    }

    @Override
    public int findPositionForKey(Object key) {
        final char k0 = TypeUtils.unbox((Character)key);
        int hash = hash(k0);
        int tableLocation = hashToTableLocation(hash);
        final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1);
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (positionValue == EMPTY_OUTPUT_POSITION) {
                return -1;
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                return positionValue;
            }
            Assert.neq(tableLocation, "tableLocation", lastTableLocation, "lastTableLocation");
            tableLocation = (tableLocation + 1) & (tableSize - 1);
        }
    }
}
