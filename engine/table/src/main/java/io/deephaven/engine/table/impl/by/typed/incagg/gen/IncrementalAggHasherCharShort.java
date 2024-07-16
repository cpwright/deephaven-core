//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.typed.incagg.gen;

import static io.deephaven.util.compare.CharComparisons.eq;
import static io.deephaven.util.compare.ShortComparisons.eq;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.chunk.util.hashing.ShortChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import java.lang.Character;
import java.lang.Object;
import java.lang.Override;
import java.lang.Short;

final class IncrementalAggHasherCharShort extends IncrementalChunkedOperatorAggregationStateManagerTypedBase {
    private final CharacterArraySource mainKeySource0;

    private final CharacterArraySource overflowKeySource0;

    private final ShortArraySource mainKeySource1;

    private final ShortArraySource overflowKeySource1;

    public IncrementalAggHasherCharShort(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        this.mainKeySource0 = (CharacterArraySource) super.mainKeySources[0];
        this.overflowKeySource0 = (CharacterArraySource) super.overflowKeySources[0];
        this.mainKeySource1 = (ShortArraySource) super.mainKeySources[1];
        this.overflowKeySource1 = (ShortArraySource) super.overflowKeySources[1];
    }

    @Override
    protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final ShortChunk<Values> keyChunk1 = sourceKeyChunks[1].asShortChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final short k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int tableLocation = hashToTableLocation(tableHashPivot, hash);
            if (isStateEmpty(mainOutputPosition.getUnsafe(tableLocation))) {
                numEntries++;
                mainKeySource0.set(tableLocation, k0);
                mainKeySource1.set(tableLocation, k1);
                handler.doMainInsert(tableLocation, chunkPosition);
            } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                handler.doMainFound(tableLocation, chunkPosition);
            } else {
                int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
                if (!findOverflow(handler, k0, k1, chunkPosition, overflowLocation)) {
                    final int newOverflowLocation = allocateOverflowLocation();
                    overflowKeySource0.set(newOverflowLocation, k0);
                    overflowKeySource1.set(newOverflowLocation, k1);
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
        final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
        final ShortChunk<Values> keyChunk1 = sourceKeyChunks[1].asShortChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final char k0 = keyChunk0.get(chunkPosition);
            final short k1 = keyChunk1.get(chunkPosition);
            final int hash = hash(k0, k1);
            final int tableLocation = hashToTableLocation(tableHashPivot, hash);
            if (isStateEmpty(mainOutputPosition.getUnsafe(tableLocation))) {
                handler.doMissing(chunkPosition);
            } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
                handler.doMainFound(tableLocation, chunkPosition);
            } else {
                int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
                if (!findOverflow(handler, k0, k1, chunkPosition, overflowLocation)) {
                    handler.doMissing(chunkPosition);
                }
            }
        }
    }

    private static int hash(char k0, short k1) {
        int hash = CharChunkHasher.hashInitialSingle(k0);
        hash = ShortChunkHasher.hashUpdateSingle(hash, k1);
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
            final char overflowKey0 = overflowKeySource0.getUnsafe(overflowLocation);
            final short overflowKey1 = overflowKeySource1.getUnsafe(overflowLocation);
            final int overflowHash = hash(overflowKey0, overflowKey1);
            final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
            if (overflowTableLocation == mainInsertLocation) {
                mainKeySource0.set(mainInsertLocation, overflowKey0);
                mainKeySource1.set(mainInsertLocation, overflowKey1);
                mainOutputPosition.set(mainInsertLocation, overflowOutputPosition.getUnsafe(overflowLocation));
                handler.doPromoteOverflow(overflowLocation, mainInsertLocation);
                overflowOutputPosition.set(overflowLocation, QueryConstants.NULL_INT);
                overflowKeySource0.set(overflowLocation, QueryConstants.NULL_CHAR);
                overflowKeySource1.set(overflowLocation, QueryConstants.NULL_SHORT);
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
        final char k0 = mainKeySource0.getUnsafe(sourceBucket);
        final short k1 = mainKeySource1.getUnsafe(sourceBucket);
        final int hash = hash(k0, k1);
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
            mainKeySource0.set(sourceBucket, QueryConstants.NULL_CHAR);
            mainKeySource1.set(destBucket, k1);
            mainKeySource1.set(sourceBucket, QueryConstants.NULL_SHORT);
            handler.doMoveMain(sourceBucket, destBucket);
        }
        return mainInsertLocation;
    }

    private boolean findOverflow(HashHandler handler, char k0, short k1, int chunkPosition,
            int overflowLocation) {
        while (overflowLocation != QueryConstants.NULL_INT) {
            if (eq(overflowKeySource0.getUnsafe(overflowLocation), k0) && eq(overflowKeySource1.getUnsafe(overflowLocation), k1)) {
                handler.doOverflowFound(overflowLocation, chunkPosition);
                return true;
            }
            overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
        }
        return false;
    }

    @Override
    public int findPositionForKey(Object key) {
        final Object [] ka = (Object[])key;
        final char k0 = TypeUtils.unbox((Character)ka[0]);
        final short k1 = TypeUtils.unbox((Short)ka[1]);
        int hash = hash(k0, k1);
        final int tableLocation = hashToTableLocation(tableHashPivot, hash);
        final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
        if (positionValue == EMPTY_OUTPUT_POSITION) {
            return -1;
        }
        if (eq(mainKeySource0.getUnsafe(tableLocation), k0) && eq(mainKeySource1.getUnsafe(tableLocation), k1)) {
            return positionValue;
        }
        int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
        while (overflowLocation != QueryConstants.NULL_INT) {
            if (eq(overflowKeySource0.getUnsafe(overflowLocation), k0) && eq(overflowKeySource1.getUnsafe(overflowLocation), k1)) {
                return overflowOutputPosition.getUnsafe(overflowLocation);
            }
            overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
        }
        return -1;
    }
}
