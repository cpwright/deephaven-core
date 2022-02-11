// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypeChunkedHashFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.incagg.gen;

import static io.deephaven.util.compare.DoubleComparisons.eq;
import static io.deephaven.util.compare.IntComparisons.eq;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.DoubleChunkHasher;
import io.deephaven.chunk.util.hashing.IntChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Object;
import java.lang.Override;

final class TypedHasherDoubleInt extends IncrementalChunkedOperatorAggregationStateManagerTypedBase {
  private final DoubleArraySource mainKeySource0;

  private final DoubleArraySource overflowKeySource0;

  private final IntegerArraySource mainKeySource1;

  private final IntegerArraySource overflowKeySource1;

  public TypedHasherDoubleInt(ColumnSource[] tableKeySources, int tableSize,
      double maximumLoadFactor, double targetLoadFactor) {
    super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    this.mainKeySource0 = (DoubleArraySource) super.mainKeySources[0];
    this.overflowKeySource0 = (DoubleArraySource) super.overflowKeySources[0];
    this.mainKeySource1 = (IntegerArraySource) super.mainKeySources[1];
    this.overflowKeySource1 = (IntegerArraySource) super.overflowKeySources[1];
  }

  @Override
  protected void build(HashHandler handler, RowSequence rowSequence, Chunk[] sourceKeyChunks) {
    final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
    final IntChunk<Values> keyChunk1 = sourceKeyChunks[1].asIntChunk();
    for (int chunkPosition = 0; chunkPosition < keyChunk0.size(); ++chunkPosition) {
      final double v0 = keyChunk0.get(chunkPosition);
      final int v1 = keyChunk1.get(chunkPosition);
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
    final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
    final IntChunk<Values> keyChunk1 = sourceKeyChunks[1].asIntChunk();
    final int chunkSize = keyChunk0.size();
    for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
      final double v0 = keyChunk0.get(chunkPosition);
      final int v1 = keyChunk1.get(chunkPosition);
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

  private int hash(double v0, int v1) {
    int hash = DoubleChunkHasher.hashInitialSingle(v0);
    hash = IntChunkHasher.hashUpdateSingle(hash, v1);
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
      final double overflowKey0 = overflowKeySource0.getUnsafe(overflowLocation);
      final int overflowKey1 = overflowKeySource1.getUnsafe(overflowLocation);
      final int overflowHash = hash(overflowKey0, overflowKey1);
      final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash);
      if (overflowTableLocation == mainInsertLocation) {
        mainKeySource0.set(mainInsertLocation, overflowKey0);
        mainKeySource1.set(mainInsertLocation, overflowKey1);
        mainOutputPosition.set(mainInsertLocation, overflowOutputPosition.getUnsafe(overflowLocation));
        handler.doPromoteOverflow(overflowLocation, mainInsertLocation);
        overflowOutputPosition.set(overflowLocation, QueryConstants.NULL_INT);
        overflowKeySource0.set(overflowLocation, QueryConstants.NULL_DOUBLE);
        overflowKeySource1.set(overflowLocation, QueryConstants.NULL_INT);
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
    final double v0 = mainKeySource0.getUnsafe(bucket);
    final int v1 = mainKeySource1.getUnsafe(bucket);
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
      mainKeySource0.set(bucket, QueryConstants.NULL_DOUBLE);
      mainKeySource1.set(destBucket, v1);
      mainKeySource1.set(bucket, QueryConstants.NULL_INT);
      handler.doMoveMain(bucket, destBucket);
    }
    return mainInsertLocation;
  }

  private boolean findOverflow(HashHandler handler, double v0, int v1, int chunkPosition,
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
    final double v0 = TypeUtils.unbox((Double)va[0]);
    final int v1 = TypeUtils.unbox((Integer)va[1]);
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
