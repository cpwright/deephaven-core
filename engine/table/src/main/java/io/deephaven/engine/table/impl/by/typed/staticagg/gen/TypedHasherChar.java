// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypeChunkedHashFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.staticagg.gen;

import static io.deephaven.util.compare.CharComparisons.eq;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.util.QueryConstants;
import java.lang.Object;
import java.lang.Override;

final class TypedHasherChar extends StaticChunkedOperatorAggregationStateManagerTypedBase {
  private final CharacterArraySource mainKeySource0;

  private final CharacterArraySource overflowKeySource0;

  public TypedHasherChar(ColumnSource[] tableKeySources, int tableSize, double maximumLoadFactor,
      double targetLoadFactor) {
    super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    this.mainKeySource0 = (CharacterArraySource) super.mainKeySources[0];
    this.overflowKeySource0 = (CharacterArraySource) super.overflowKeySources[0];
  }

  @Override
  protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
    final CharChunk<Values> keyChunk0 = sourceKeyChunks[0].asCharChunk();
    for (int chunkPosition = 0; chunkPosition < keyChunk0.size(); ++chunkPosition) {
      final char v0 = keyChunk0.get(chunkPosition);
      final int hash = hash(v0);
      final int tableLocation = hashToTableLocation(tableHashPivot, hash);
      if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_STATE_VALUE) {
        numEntries++;
        mainKeySource0.set(tableLocation, v0);
        handler.doMainInsert(tableLocation, chunkPosition);
      } else if (eq(mainKeySource0.getUnsafe(tableLocation), v0)) {
        handler.doMainFound(tableLocation, chunkPosition);
      } else {
        int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
        if (!findOverflow(handler, v0, chunkPosition, overflowLocation)) {
          final int newOverflowLocation = allocateOverflowLocation();
          overflowKeySource0.set(newOverflowLocation, v0);
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
    final int chunkSize = keyChunk0.size();
    for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
      final char v0 = keyChunk0.get(chunkPosition);
      final int hash = hash(v0);
      final int tableLocation = hashToTableLocation(tableHashPivot, hash);
      if (mainOutputPosition.getUnsafe(tableLocation) == EMPTY_STATE_VALUE) {
        handler.doMissing(chunkPosition);
      } else if (eq(mainKeySource0.getUnsafe(tableLocation), v0)) {
        handler.doMainFound(tableLocation, chunkPosition);
      } else {
        int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
        if (!findOverflow(handler, v0, chunkPosition, overflowLocation)) {
          handler.doMissing(chunkPosition);
        }
      }
    }
  }

  private int hash(char v0) {
    int hash = CharChunkHasher.hashInitialSingle(v0);
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
      final char overflowKey0 = overflowKeySource0.getUnsafe(overflowLocation);
      final int overflowHash = hash(overflowKey0);
      final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
      if (overflowTableLocation == mainInsertLocation) {
        mainKeySource0.set(mainInsertLocation, overflowKey0);
        mainOutputPosition.set(mainInsertLocation, overflowOutputPosition.getUnsafe(overflowLocation));
        handler.doPromoteOverflow(overflowLocation, mainInsertLocation);
        overflowOutputPosition.set(overflowLocation, QueryConstants.NULL_INT);
        overflowKeySource0.set(overflowLocation, QueryConstants.NULL_CHAR);
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
    final char v0 = mainKeySource0.getUnsafe(bucket);
    final int hash = hash(v0);
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
      mainKeySource0.set(bucket, QueryConstants.NULL_CHAR);
      handler.doMoveMain(bucket, destBucket);
    }
    return mainInsertLocation;
  }

  private boolean findOverflow(HashHandler handler, char v0, int chunkPosition,
      int overflowLocation) {
    while (overflowLocation != QueryConstants.NULL_INT) {
      if (eq(overflowKeySource0.getUnsafe(overflowLocation), v0)) {
        handler.doOverflowFound(overflowLocation, chunkPosition);
        return true;
      }
      overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
    }
    return false;
  }

  @Override
  public int findPositionForKey(Object value) {
    final char v0 = (char)value;
    int hash = hash(v0);
    final int tableLocation = hashToTableLocation(tableHashPivot, hash);
    final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
    if (positionValue == EMPTY_STATE_VALUE) {
      return -1;
    }
    if (eq(mainKeySource0.getUnsafe(tableLocation), v0)) {
      return positionValue;
    }
    int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation);
    while (overflowLocation != QueryConstants.NULL_INT) {
      if (eq(overflowKeySource0.getUnsafe(overflowLocation), v0)) {
        return overflowOutputPosition.getUnsafe(overflowLocation);
      }
      overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);;
    }
    return -1;
  }
}
