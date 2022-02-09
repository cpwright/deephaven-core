// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.replicators.ReplicateTypedHashers
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.incagg.gen;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerTypedBase;
import java.util.Arrays;

/**
 * The TypedHashDispatcher returns a pre-generated and precompiled hasher instance suitable for the provided column sources, or null if there is not a precompiled hasher suitable for the specified sources. */
public class TypedHashDispatcher {
  private TypedHashDispatcher() {
    // static use only
  }

  public static IncrementalChunkedOperatorAggregationStateManagerTypedBase dispatch(ColumnSource[] tableKeySources,
      int tableSize, double maximumLoadFactor, double targetLoadFactor) {
    final ChunkType[] chunkTypes = Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);;
    if (chunkTypes.length == 1) {
      return dispatchSingle(chunkTypes[0], tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    }
    if (chunkTypes.length == 2) {
      return dispatchDouble(chunkTypes[0], chunkTypes[1], tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    }
    return null;
  }

  private static IncrementalChunkedOperatorAggregationStateManagerTypedBase dispatchSingle(ChunkType chunkType,
      ColumnSource[] tableKeySources, int tableSize, double maximumLoadFactor,
      double targetLoadFactor) {
    switch (chunkType) {
      default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType);
      case Char:return new TypedHasherChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      case Byte:return new TypedHasherByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      case Short:return new TypedHasherShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      case Int:return new TypedHasherInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      case Long:return new TypedHasherLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      case Float:return new TypedHasherFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      case Double:return new TypedHasherDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      case Object:return new TypedHasherObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    }
  }

  private static IncrementalChunkedOperatorAggregationStateManagerTypedBase dispatchDouble(ChunkType chunkType0,
      ChunkType chunkType1, ColumnSource[] tableKeySources, int tableSize, double maximumLoadFactor,
      double targetLoadFactor) {
    switch (chunkType0) {
      default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType0);
      case Char:switch (chunkType1) {
        default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
        case Char:return new TypedHasherCharChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Byte:return new TypedHasherCharByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Short:return new TypedHasherCharShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Int:return new TypedHasherCharInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Long:return new TypedHasherCharLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Float:return new TypedHasherCharFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Double:return new TypedHasherCharDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Object:return new TypedHasherCharObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      }
      case Byte:switch (chunkType1) {
        default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
        case Char:return new TypedHasherByteChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Byte:return new TypedHasherByteByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Short:return new TypedHasherByteShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Int:return new TypedHasherByteInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Long:return new TypedHasherByteLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Float:return new TypedHasherByteFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Double:return new TypedHasherByteDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Object:return new TypedHasherByteObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      }
      case Short:switch (chunkType1) {
        default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
        case Char:return new TypedHasherShortChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Byte:return new TypedHasherShortByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Short:return new TypedHasherShortShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Int:return new TypedHasherShortInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Long:return new TypedHasherShortLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Float:return new TypedHasherShortFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Double:return new TypedHasherShortDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Object:return new TypedHasherShortObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      }
      case Int:switch (chunkType1) {
        default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
        case Char:return new TypedHasherIntChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Byte:return new TypedHasherIntByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Short:return new TypedHasherIntShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Int:return new TypedHasherIntInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Long:return new TypedHasherIntLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Float:return new TypedHasherIntFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Double:return new TypedHasherIntDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Object:return new TypedHasherIntObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      }
      case Long:switch (chunkType1) {
        default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
        case Char:return new TypedHasherLongChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Byte:return new TypedHasherLongByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Short:return new TypedHasherLongShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Int:return new TypedHasherLongInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Long:return new TypedHasherLongLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Float:return new TypedHasherLongFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Double:return new TypedHasherLongDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Object:return new TypedHasherLongObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      }
      case Float:switch (chunkType1) {
        default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
        case Char:return new TypedHasherFloatChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Byte:return new TypedHasherFloatByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Short:return new TypedHasherFloatShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Int:return new TypedHasherFloatInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Long:return new TypedHasherFloatLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Float:return new TypedHasherFloatFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Double:return new TypedHasherFloatDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Object:return new TypedHasherFloatObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      }
      case Double:switch (chunkType1) {
        default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
        case Char:return new TypedHasherDoubleChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Byte:return new TypedHasherDoubleByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Short:return new TypedHasherDoubleShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Int:return new TypedHasherDoubleInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Long:return new TypedHasherDoubleLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Float:return new TypedHasherDoubleFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Double:return new TypedHasherDoubleDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Object:return new TypedHasherDoubleObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      }
      case Object:switch (chunkType1) {
        default:throw new UnsupportedOperationException("Invalid chunk type for typed hashers: " + chunkType1);
        case Char:return new TypedHasherObjectChar(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Byte:return new TypedHasherObjectByte(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Short:return new TypedHasherObjectShort(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Int:return new TypedHasherObjectInt(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Long:return new TypedHasherObjectLong(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Float:return new TypedHasherObjectFloat(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Double:return new TypedHasherObjectDouble(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        case Object:return new TypedHasherObjectObject(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
      }
    }
  }
}
