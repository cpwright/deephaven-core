//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.typed.staticopenagg.gen;

import static io.deephaven.util.compare.CharComparisons.eq;
import static io.deephaven.util.compare.DoubleComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.chunk.util.hashing.DoubleChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableCharArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableDoubleArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Character;
import java.lang.Double;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticAggOpenHasherCharDouble extends StaticChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private final ImmutableCharArraySource mainKeySource0;

    private final ImmutableDoubleArraySource mainKeySource1;

    public StaticAggOpenHasherCharDouble(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableCharArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
        this.mainKeySource1 = (ImmutableDoubleArraySource) super.mainKeySources[1];
        this.mainKeySource1.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void build(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final DoubleChunk<Values> keyChunk1 = sourceKeyChunks[1].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final double k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
                if (isStateEmpty(outputPosition)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    mainKeySource1.set(tableLocation, k1);
                    outputPosition = nextOutputPosition.getAndIncrement();
                    outputPositions.set(chunkPosition, outputPosition);
                    mainOutputPosition.set(tableLocation, outputPosition);
                    outputPositionToHashSlot.set(outputPosition, tableLocation);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                    outputPositions.set(chunkPosition, outputPosition);
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    private static int hash(char k0, double k1) {
        int hash = CharChunkHasher.hashInitialSingle(k0);
        hash = DoubleChunkHasher.hashUpdateSingle(hash, k1);
        return hash;
    }

    private static final boolean isStateAvailable(int state) {
        return state == EMPTY_OUTPUT_POSITION;
    }

    private static final boolean isStateEmpty(int state) {
        return state == EMPTY_OUTPUT_POSITION;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final char[] destKeyArray0 = new char[tableSize];
        final double[] destKeyArray1 = new double[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_OUTPUT_POSITION);
        final char [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final double [] originalKeyArray1 = mainKeySource1.getArray();
        mainKeySource1.setArray(destKeyArray1);
        final int [] originalStateArray = mainOutputPosition.getArray();
        mainOutputPosition.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final int currentStateValue = originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final char k0 = originalKeyArray0[sourceBucket];
            final double k1 = originalKeyArray1[sourceBucket];
            final int hash = hash(k0, k1);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (destState[destinationTableLocation] == EMPTY_OUTPUT_POSITION) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destKeyArray1[destinationTableLocation] = k1;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    if (sourceBucket != destinationTableLocation) {
                        outputPositionToHashSlot.set(currentStateValue, destinationTableLocation);
                    }
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }

    @Override
    public int findPositionForKey(Object key) {
        final Object [] ka = (Object[])key;
        final char k0 = TypeUtils.unbox((Character)ka[0]);
        final double k1 = TypeUtils.unbox((Double)ka[1]);
        int hash = hash(k0, k1);
        int tableLocation = hashToTableLocation(hash);
        final int firstTableLocation = tableLocation;
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (positionValue == EMPTY_OUTPUT_POSITION) {
                return UNKNOWN_ROW;
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                return positionValue;
            }
            tableLocation = nextTableLocation(tableLocation);
            Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
        }
    }
}
