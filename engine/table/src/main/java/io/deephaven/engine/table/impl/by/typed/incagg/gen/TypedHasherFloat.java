// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypeChunkedHashFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.incagg.gen;

import static io.deephaven.util.compare.FloatComparisons.eq;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.FloatChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.util.QueryConstants;
import java.lang.Object;
import java.lang.Override;

public final class TypedHasherFloat extends IncrementalChunkedOperatorAggregationStateManagerTypedBase {
  private final FloatArraySource keySource0;

  private final FloatArraySource overflowKeySource0;

  public TypedHasherFloat(ColumnSource[] tableKeySources, int tableSize, double maximumLoadFactor,
      double targetLoadFactor) {
    super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    this.keySource0 = (FloatArraySource) super.keySources[0];
    this.overflowKeySource0 = (FloatArraySource) super.overflowKeySources[0];
  }

  @Override
  protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
    final FloatChunk<Values> keyChunk0 = sourceKeyChunks[0].asFloatChunk();
    for (int chunkPosition = 0; chunkPosition < keyChunk0.size(); ++chunkPosition) {
      final float v0 = keyChunk0.get(chunkPosition);
      final int hash = hash(v0);
      final int tableLocation = hashToTableLocation(tableHashPivot, hash);
      if (stateSource.getUnsafe(tableLocation) == EMPTY_RIGHT_VALUE) {
        numEntries++;
        keySource0.set(tableLocation, v0);
        handler.doMainInsert(tableLocation, chunkPosition);
      } else if (eq(keySource0.getUnsafe(tableLocation), v0)) {
        handler.doMainFound(tableLocation, chunkPosition);
      } else {
        int overflowLocation = overflowLocationSource.getUnsafe(tableLocation);
        if (!findOverflow(handler, v0, chunkPosition, overflowLocation)) {
          final int newOverflowLocation = allocateOverflowLocation();
          overflowKeySource0.set(newOverflowLocation, v0);
          overflowLocationSource.set(tableLocation, newOverflowLocation);
          overflowOverflowLocationSource.set(newOverflowLocation, overflowLocation);
          numEntries++;
          handler.doOverflowInsert(newOverflowLocation, chunkPosition);
        }
      }
    }
  }

  private int hash(float v0) {
    int hash = FloatChunkHasher.hashInitialSingle(v0);
    return hash;
  }

  @Override
  protected void rehashBucket(HashHandler handler, int bucket, int destBucket, int bucketsToAdd) {
    final int position = stateSource.getUnsafe(bucket);
    if (position == EMPTY_RIGHT_VALUE) {
      return;
    }
    int mainInsertLocation = maybeMoveMainBucket(handler, bucket, destBucket, bucketsToAdd);
    int overflowLocation = overflowLocationSource.getUnsafe(bucket);
    overflowLocationSource.set(bucket, QueryConstants.NULL_INT);
    overflowLocationSource.set(destBucket, QueryConstants.NULL_INT);
    while (overflowLocation != QueryConstants.NULL_INT) {
      final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
      final float overflowKey0 = overflowKeySource0.getUnsafe(overflowLocation);
      final int overflowHash = hash(overflowKey0);
      final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
      if (overflowTableLocation == mainInsertLocation) {
        keySource0.set(mainInsertLocation, overflowKey0);
        stateSource.set(mainInsertLocation, overflowStateSource.getUnsafe(overflowLocation));
        handler.promoteOverflow(overflowLocation, mainInsertLocation);
        overflowStateSource.set(overflowLocation, QueryConstants.NULL_INT);
        overflowKeySource0.set(overflowLocation, QueryConstants.NULL_FLOAT);
        freeOverflowLocation(overflowLocation);
        mainInsertLocation = -1;
      } else {
        final int oldOverflowLocation = overflowLocationSource.getUnsafe(overflowTableLocation);
        overflowLocationSource.set(overflowTableLocation, overflowLocation);
        overflowOverflowLocationSource.set(overflowLocation, oldOverflowLocation);
      }
      overflowLocation = nextOverflowLocation;
    }
  }

  private int maybeMoveMainBucket(HashHandler handler, int bucket, int destBucket,
      int bucketsToAdd) {
    final float v0 = keySource0.getUnsafe(bucket);
    final int hash = hash(v0);
    final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash);
    final int mainInsertLocation;
    if (location == bucket) {
      mainInsertLocation = destBucket;
      stateSource.set(destBucket, EMPTY_RIGHT_VALUE);
    } else {
      mainInsertLocation = bucket;
      stateSource.set(destBucket, stateSource.getUnsafe(bucket));
      stateSource.set(bucket, EMPTY_RIGHT_VALUE);
      keySource0.set(destBucket, v0);
      keySource0.set(bucket, QueryConstants.NULL_FLOAT);
      handler.moveMain(bucket, destBucket);
    }
    return mainInsertLocation;
  }

  private boolean findOverflow(HashHandler handler, float v0, int chunkPosition,
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
    final float v0 = (float)value;
    int hash = hash(v0);
    final int tableLocation = hashToTableLocation(tableHashPivot, hash);
    final int positionValue = stateSource.getUnsafe(tableLocation);
    if (positionValue == EMPTY_RIGHT_VALUE) {
      return -1;
    }
    if (eq(keySource0.getUnsafe(tableLocation), v0)) {
      return positionValue;
    }
    int overflowLocation = overflowLocationSource.getUnsafe(tableLocation);
    while (overflowLocation != QueryConstants.NULL_INT) {
      if (eq(overflowKeySource0.getUnsafe(overflowLocation), v0)) {
        return overflowStateSource.getUnsafe(overflowLocation);
      }
      overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);;
    }
    return -1;
  }
}
