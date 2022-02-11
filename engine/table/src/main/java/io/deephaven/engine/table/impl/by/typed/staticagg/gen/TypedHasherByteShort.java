// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypeChunkedHashFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.staticagg.gen;

import static io.deephaven.util.compare.ByteComparisons.eq;
import static io.deephaven.util.compare.ShortComparisons.eq;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ByteChunkHasher;
import io.deephaven.chunk.util.hashing.ShortChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import java.lang.Byte;
import java.lang.Object;
import java.lang.Override;
import java.lang.Short;

final class TypedHasherByteShort extends StaticChunkedOperatorAggregationStateManagerTypedBase {
  private final ByteArraySource mainKeySource0;

  private final ByteArraySource overflowKeySource0;

  private final ShortArraySource mainKeySource1;

  private final ShortArraySource overflowKeySource1;

  public TypedHasherByteShort(ColumnSource[] tableKeySources, int tableSize,
      double maximumLoadFactor, double targetLoadFactor) {
    super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    this.mainKeySource0 = (ByteArraySource) super.mainKeySources[0];
    this.overflowKeySource0 = (ByteArraySource) super.overflowKeySources[0];
    this.mainKeySource1 = (ShortArraySource) super.mainKeySources[1];
    this.overflowKeySource1 = (ShortArraySource) super.overflowKeySources[1];
  }

  @Override
  protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
    final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
    final ShortChunk<Values> keyChunk1 = sourceKeyChunks[1].asShortChunk();
    for (int chunkPosition = 0; chunkPosition < keyChunk0.size(); ++chunkPosition) {
      final byte v0 = keyChunk0.get(chunkPosition);
      final short v1 = keyChunk1.get(chunkPosition);
      final int hash = hash(v0, v1);
      final int tableLocation = hashToTableLocation(tableHashPivot, hash);
      if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_STATE_VALUE) {
        numEntries++;
        mainKeySource0.set(tableLocation, v0);
        mainKeySource1.set(tableLocation, v1);
        handler.doMainInsert(tableLocation, chunkPosition);
      } else if (eq(mainKeySource0.getUnsafe(tableLocation), v0) && eq(mainKeySource1.getUnsafe(tableLocation), v1)) {
        handler.doMainFound(tableLocation, chunkPosition);
      } else {
        int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
        if (!findOverflow(handler, v0, v1, chunkPosition, overflowLocation)) {
          final int newOverflowLocation = allocateOverflowLocation();
          overflowKeySource0.set(newOverflowLocation, v0);
          overflowKeySource1.set(newOverflowLocation, v1);
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
    final ByteChunk<Values> keyChunk0 = sourceKeyChunks[0].asByteChunk();
    final ShortChunk<Values> keyChunk1 = sourceKeyChunks[1].asShortChunk();
    final int chunkSize = keyChunk0.size();
    for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
      final byte v0 = keyChunk0.get(chunkPosition);
      final short v1 = keyChunk1.get(chunkPosition);
      final int hash = hash(v0, v1);
      final int tableLocation = hashToTableLocation(tableHashPivot, hash);
      if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_STATE_VALUE) {
        handler.doMissing(chunkPosition);
      } else if (eq(mainKeySource0.getUnsafe(tableLocation), v0) && eq(mainKeySource1.getUnsafe(tableLocation), v1)) {
        handler.doMainFound(tableLocation, chunkPosition);
      } else {
        int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
        if (!findOverflow(handler, v0, v1, chunkPosition, overflowLocation)) {
          handler.doMissing(chunkPosition);
        }
      }
    }
  }

  private int hash(byte v0, short v1) {
    int hash = ByteChunkHasher.hashInitialSingle(v0);
    hash = ShortChunkHasher.hashUpdateSingle(hash, v1);
    return hash;
  }

  @Override
  protected void rehashBucket(HashHandler handler, int bucket, int destBucket, int bucketsToAdd) {
    final int position = mainOutputPosition.getUnsafe(bucket);
    if (position == EMPTY_STATE_VALUE) {
      return;
    }
    int mainInsertLocation = maybeMoveMainBucket(handler, bucket, destBucket, bucketsToAdd);
    int overflowLocation = mainOverflowLocationSource.getUnsafe(bucket);
    mainOverflowLocationSource.set(bucket, QueryConstants.NULL_INT);
    mainOverflowLocationSource.set(destBucket, QueryConstants.NULL_INT);
    while (overflowLocation != QueryConstants.NULL_INT) {
      final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
      final byte overflowKey0 = overflowKeySource0.getUnsafe(overflowLocation);
      final short overflowKey1 = overflowKeySource1.getUnsafe(overflowLocation);
      final int overflowHash = hash(overflowKey0, overflowKey1);
      final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
      if (overflowTableLocation == mainInsertLocation) {
        mainKeySource0.set(mainInsertLocation, overflowKey0);
        mainKeySource1.set(mainInsertLocation, overflowKey1);
        mainOutputPosition.set(mainInsertLocation, overflowOutputPosition.getUnsafe(overflowLocation));
        handler.doPromoteOverflow(overflowLocation, mainInsertLocation);
        overflowOutputPosition.set(overflowLocation, QueryConstants.NULL_INT);
        overflowKeySource0.set(overflowLocation, QueryConstants.NULL_BYTE);
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

  private int maybeMoveMainBucket(HashHandler handler, int bucket, int destBucket,
      int bucketsToAdd) {
    final byte v0 = mainKeySource0.getUnsafe(bucket);
    final short v1 = mainKeySource1.getUnsafe(bucket);
    final int hash = hash(v0, v1);
    final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash);
    final int mainInsertLocation;
    if (location == bucket) {
      mainInsertLocation = destBucket;
      mainOutputPosition.set(destBucket, EMPTY_STATE_VALUE);
    } else {
      mainInsertLocation = bucket;
      mainOutputPosition.set(destBucket, mainOutputPosition.getUnsafe(bucket));
      mainOutputPosition.set(bucket, EMPTY_STATE_VALUE);
      mainKeySource0.set(destBucket, v0);
      mainKeySource0.set(bucket, QueryConstants.NULL_BYTE);
      mainKeySource1.set(destBucket, v1);
      mainKeySource1.set(bucket, QueryConstants.NULL_SHORT);
      handler.doMoveMain(bucket, destBucket);
    }
    return mainInsertLocation;
  }

  private boolean findOverflow(HashHandler handler, byte v0, short v1, int chunkPosition,
      int overflowLocation) {
    while (overflowLocation != QueryConstants.NULL_INT) {
      if (eq(overflowKeySource0.getUnsafe(overflowLocation), v0) && eq(overflowKeySource1.getUnsafe(overflowLocation), v1)) {
        handler.doOverflowFound(overflowLocation, chunkPosition);
        return true;
      }
      overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
    }
    return false;
  }

  @Override
  public int findPositionForKey(Object value) {
    final Object [] va = (Object[])value;
    final byte v0 = TypeUtils.unbox((Byte)va[0]);
    final short v1 = TypeUtils.unbox((Short)va[1]);
    int hash = hash(v0, v1);
    final int tableLocation = hashToTableLocation(tableHashPivot, hash);
    final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
    if (positionValue == EMPTY_STATE_VALUE) {
      return -1;
    }
    if (eq(mainKeySource0.getUnsafe(tableLocation), v0) && eq(mainKeySource1.getUnsafe(tableLocation), v1)) {
      return positionValue;
    }
    int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
    while (overflowLocation != QueryConstants.NULL_INT) {
      if (eq(overflowKeySource0.getUnsafe(overflowLocation), v0) && eq(overflowKeySource1.getUnsafe(overflowLocation), v1)) {
        return overflowOutputPosition.getUnsafe(overflowLocation);
      }
      overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);;
    }
    return -1;
  }
}
