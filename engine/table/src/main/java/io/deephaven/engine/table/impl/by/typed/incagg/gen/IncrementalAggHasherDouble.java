//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.typed.incagg.gen;

import static io.deephaven.util.compare.DoubleComparisons.eq;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.DoubleChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import java.lang.Double;
import java.lang.Object;
import java.lang.Override;

final class IncrementalAggHasherDouble extends IncrementalChunkedOperatorAggregationStateManagerTypedBase {
    private final DoubleArraySource mainKeySource0;

    private final DoubleArraySource overflowKeySource0;

    public IncrementalAggHasherDouble(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        this.mainKeySource0 = (DoubleArraySource) super.mainKeySources[0];
        this.overflowKeySource0 = (DoubleArraySource) super.overflowKeySources[0];
    }

    @Override
    protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int tableLocation = hashToTableLocation(tableHashPivot, hash);
            if (isStateEmpty(mainOutputPosition.getUnsafe(tableLocation))) {
                numEntries++;
                mainKeySource0.set(tableLocation, k0);
                handler.doMainInsert(tableLocation, chunkPosition);
            } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                handler.doMainFound(tableLocation, chunkPosition);
            } else {
                int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
                if (!findOverflow(handler, k0, chunkPosition, overflowLocation)) {
                    final int newOverflowLocation = allocateOverflowLocation();
                    overflowKeySource0.set(newOverflowLocation, k0);
                    mainOverflowLocationSource.set(tableLocation, newOverflowLocation);
                    overflowOverflowLocationSource.set(newOverflowLocation, overflowLocation);
                    numEntries++;
                    handler.doOverflowInsert(newOverflowLocation, chunkPosition);
                }
            }
        }
    }

    @Override
    protected void probe(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int tableLocation = hashToTableLocation(tableHashPivot, hash);
            if (isStateEmpty(mainOutputPosition.getUnsafe(tableLocation))) {
                handler.doMissing(chunkPosition);
            } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                handler.doMainFound(tableLocation, chunkPosition);
            } else {
                int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
                if (!findOverflow(handler, k0, chunkPosition, overflowLocation)) {
                    handler.doMissing(chunkPosition);
                }
            }
        }
    }

    private static int hash(double k0) {
        int hash = DoubleChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(int state) {
        return state == EMPTY_OUTPUT_POSITION;
    }

    @Override
    protected void rehashBucket(HashHandler handler, int sourceBucket, int destBucket,
            int bucketsToAdd) {
        final int position = mainOutputPosition.getUnsafe(sourceBucket);
        if (isStateEmpty(position)) {
            return;
        }
        int mainInsertLocation = maybeMoveMainBucket(handler, sourceBucket, destBucket, bucketsToAdd);
        int overflowLocation = mainOverflowLocationSource.getUnsafe(sourceBucket);
        mainOverflowLocationSource.set(sourceBucket, QueryConstants.NULL_INT);
        mainOverflowLocationSource.set(destBucket, QueryConstants.NULL_INT);
        while (overflowLocation != QueryConstants.NULL_INT) {
            final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
            final double overflowKey0 = overflowKeySource0.getUnsafe(overflowLocation);
            final int overflowHash = hash(overflowKey0);
            final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
            if (overflowTableLocation == mainInsertLocation) {
                mainKeySource0.set(mainInsertLocation, overflowKey0);
                mainOutputPosition.set(mainInsertLocation, overflowOutputPosition.getUnsafe(overflowLocation));
                handler.doPromoteOverflow(overflowLocation, mainInsertLocation);
                overflowOutputPosition.set(overflowLocation, QueryConstants.NULL_INT);
                overflowKeySource0.set(overflowLocation, QueryConstants.NULL_DOUBLE);
                freeOverflowLocation(overflowLocation);
                mainInsertLocation = -1;
            } else {
                final int oldOverflowLocation = mainOverflowLocationSource.getUnsafe(overflowTableLocation);
                mainOverflowLocationSource.set(overflowTableLocation, overflowLocation);
                overflowOverflowLocationSource.set(overflowLocation, oldOverflowLocation);
            }
            overflowLocation = nextOverflowLocation;
        }
    }

    private int maybeMoveMainBucket(HashHandler handler, int sourceBucket, int destBucket,
            int bucketsToAdd) {
        final double k0 = mainKeySource0.getUnsafe(sourceBucket);
        final int hash = hash(k0);
        final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash);
        final int mainInsertLocation;
        if (location == sourceBucket) {
            mainInsertLocation = destBucket;
            mainOutputPosition.set(destBucket, EMPTY_OUTPUT_POSITION);
        } else {
            mainInsertLocation = sourceBucket;
            mainOutputPosition.set(destBucket, mainOutputPosition.getUnsafe(sourceBucket));
            mainOutputPosition.set(sourceBucket, EMPTY_OUTPUT_POSITION);
            mainKeySource0.set(destBucket, k0);
            mainKeySource0.set(sourceBucket, QueryConstants.NULL_DOUBLE);
            handler.doMoveMain(sourceBucket, destBucket);
        }
        return mainInsertLocation;
    }

    private boolean findOverflow(HashHandler handler, double k0, int chunkPosition,
            int overflowLocation) {
        while (overflowLocation != QueryConstants.NULL_INT) {
            if (eq(overflowKeySource0.getUnsafe(overflowLocation), k0)) {
                handler.doOverflowFound(overflowLocation, chunkPosition);
                return true;
            }
            overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
        }
        return false;
    }

    @Override
    public int findPositionForKey(Object key) {
        final double k0 = TypeUtils.unbox((Double)key);
        int hash = hash(k0);
        final int tableLocation = hashToTableLocation(tableHashPivot, hash);
        final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
        if (positionValue == EMPTY_OUTPUT_POSITION) {
            return -1;
        }
        if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
            return positionValue;
        }
        int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
        while (overflowLocation != QueryConstants.NULL_INT) {
            if (eq(overflowKeySource0.getUnsafe(overflowLocation), k0)) {
                return overflowOutputPosition.getUnsafe(overflowLocation);
            }
            overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
        }
        return -1;
    }
}
