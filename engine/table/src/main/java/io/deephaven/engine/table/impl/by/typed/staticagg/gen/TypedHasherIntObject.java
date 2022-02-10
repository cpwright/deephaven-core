// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypeChunkedHashFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.staticagg.gen;

import static io.deephaven.util.compare.IntComparisons.eq;
import static io.deephaven.util.compare.ObjectComparisons.eq;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.IntChunkHasher;
import io.deephaven.chunk.util.hashing.ObjectChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.util.QueryConstants;
import java.lang.Object;
import java.lang.Override;

final class TypedHasherIntObject extends StaticChunkedOperatorAggregationStateManagerTypedBase {
  private final IntegerArraySource keySource0;

  private final IntegerArraySource overflowKeySource0;

  private final ObjectArraySource keySource1;

  private final ObjectArraySource overflowKeySource1;

  public TypedHasherIntObject(ColumnSource[] tableKeySources, int tableSize,
      double maximumLoadFactor, double targetLoadFactor) {
    super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    this.keySource0 = (IntegerArraySource) super.keySources[0];
    this.overflowKeySource0 = (IntegerArraySource) super.overflowKeySources[0];
    this.keySource1 = (ObjectArraySource) super.keySources[1];
    this.overflowKeySource1 = (ObjectArraySource) super.overflowKeySources[1];
  }

  @Override
  protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
    final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
    final ObjectChunk<Object, Values> keyChunk1 = sourceKeyChunks[1].asObjectChunk();
    for (int chunkPosition = 0; chunkPosition < keyChunk0.size(); ++chunkPosition) {
      final int v0 = keyChunk0.get(chunkPosition);
      final Object v1 = keyChunk1.get(chunkPosition);
      final int hash = hash(v0, v1);
      final int tableLocation = hashToTableLocation(tableHashPivot, hash);
      if (stateSource.getUnsafe(tableLocation) == EMPTY_STATE_VALUE) {
        numEntries++;
        keySource0.set(tableLocation, v0);
        keySource1.set(tableLocation, v1);
        handler.doMainInsert(tableLocation, chunkPosition);
      } else if (eq(keySource0.getUnsafe(tableLocation), v0) && eq(keySource1.getUnsafe(tableLocation), v1)) {
        handler.doMainFound(tableLocation, chunkPosition);
      } else {
        int overflowLocation = overflowLocationSource.getUnsafe(tableLocation);
        if (!findOverflow(handler, v0, v1, chunkPosition, overflowLocation)) {
          final int newOverflowLocation = allocateOverflowLocation();
          overflowKeySource0.set(newOverflowLocation, v0);
          overflowKeySource1.set(newOverflowLocation, v1);
          overflowLocationSource.set(tableLocation, newOverflowLocation);
          overflowOverflowLocationSource.set(newOverflowLocation, overflowLocation);
          numEntries++;
          handler.doOverflowInsert(newOverflowLocation, chunkPosition);
        }
      }
    }
  }

  @Override
  protected void probe(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
    final IntChunk<Values> keyChunk0 = sourceKeyChunks[0].asIntChunk();
    final ObjectChunk<Object, Values> keyChunk1 = sourceKeyChunks[1].asObjectChunk();
    for (int chunkPosition = 0; chunkPosition < keyChunk0.size(); ++chunkPosition) {
      final int v0 = keyChunk0.get(chunkPosition);
      final Object v1 = keyChunk1.get(chunkPosition);
      final int hash = hash(v0, v1);
      final int tableLocation = hashToTableLocation(tableHashPivot, hash);
      if (stateSource.getUnsafe(tableLocation) == EMPTY_STATE_VALUE) {
        handler.doMissing(chunkPosition);
      } else if (eq(keySource0.getUnsafe(tableLocation), v0) && eq(keySource1.getUnsafe(tableLocation), v1)) {
        handler.doMainFound(tableLocation, chunkPosition);
      } else {
        int overflowLocation = overflowLocationSource.getUnsafe(tableLocation);
        if (!findOverflow(handler, v0, v1, chunkPosition, overflowLocation)) {
          handler.doMissing(chunkPosition);
        }
      }
    }
  }

  private int hash(int v0, Object v1) {
    int hash = IntChunkHasher.hashInitialSingle(v0);
    hash = ObjectChunkHasher.hashUpdateSingle(hash, v1);
    return hash;
  }

  @Override
  protected void rehashBucket(HashHandler handler, int bucket, int destBucket, int bucketsToAdd) {
    final int position = stateSource.getUnsafe(bucket);
    if (position == EMPTY_STATE_VALUE) {
      return;
    }
    int mainInsertLocation = maybeMoveMainBucket(handler, bucket, destBucket, bucketsToAdd);
    int overflowLocation = overflowLocationSource.getUnsafe(bucket);
    overflowLocationSource.set(bucket, QueryConstants.NULL_INT);
    overflowLocationSource.set(destBucket, QueryConstants.NULL_INT);
    while (overflowLocation != QueryConstants.NULL_INT) {
      final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);
      final int overflowKey0 = overflowKeySource0.getUnsafe(overflowLocation);
      final Object overflowKey1 = overflowKeySource1.getUnsafe(overflowLocation);
      final int overflowHash = hash(overflowKey0, overflowKey1);
      final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
      if (overflowTableLocation == mainInsertLocation) {
        keySource0.set(mainInsertLocation, overflowKey0);
        keySource1.set(mainInsertLocation, overflowKey1);
        stateSource.set(mainInsertLocation, overflowStateSource.getUnsafe(overflowLocation));
        handler.promoteOverflow(overflowLocation, mainInsertLocation);
        overflowStateSource.set(overflowLocation, QueryConstants.NULL_INT);
        overflowKeySource0.set(overflowLocation, QueryConstants.NULL_INT);
        overflowKeySource1.set(overflowLocation, null);
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
    final int v0 = keySource0.getUnsafe(bucket);
    final Object v1 = keySource1.getUnsafe(bucket);
    final int hash = hash(v0, v1);
    final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash);
    final int mainInsertLocation;
    if (location == bucket) {
      mainInsertLocation = destBucket;
      stateSource.set(destBucket, EMPTY_STATE_VALUE);
    } else {
      mainInsertLocation = bucket;
      stateSource.set(destBucket, stateSource.getUnsafe(bucket));
      stateSource.set(bucket, EMPTY_STATE_VALUE);
      keySource0.set(destBucket, v0);
      keySource0.set(bucket, QueryConstants.NULL_INT);
      keySource1.set(destBucket, v1);
      keySource1.set(bucket, null);
      handler.moveMain(bucket, destBucket);
    }
    return mainInsertLocation;
  }

  private boolean findOverflow(HashHandler handler, int v0, Object v1, int chunkPosition,
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
    final int v0 = (int)va[0];
    final Object v1 = (Object)va[1];
    int hash = hash(v0, v1);
    final int tableLocation = hashToTableLocation(tableHashPivot, hash);
    final int positionValue = stateSource.getUnsafe(tableLocation);
    if (positionValue == EMPTY_STATE_VALUE) {
      return -1;
    }
    if (eq(keySource0.getUnsafe(tableLocation), v0) && eq(keySource1.getUnsafe(tableLocation), v1)) {
      return positionValue;
    }
    int overflowLocation = overflowLocationSource.getUnsafe(tableLocation);
    while (overflowLocation != QueryConstants.NULL_INT) {
      if (eq(overflowKeySource0.getUnsafe(overflowLocation), v0) && eq(overflowKeySource1.getUnsafe(overflowLocation), v1)) {
        return overflowStateSource.getUnsafe(overflowLocation);
      }
      overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);;
    }
    return -1;
  }
}
