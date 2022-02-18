// DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY io.deephaven.engine.table.impl.by.typed.TypedHasherFactory
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed.incopenagg.gen;

import static io.deephaven.util.compare.DoubleComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.DoubleChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableDoubleArraySource;
import io.deephaven.util.type.TypeUtils;
import java.lang.Double;
import java.lang.Object;
import java.lang.Override;

final class IncrementalAggOpenHasherDouble extends IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase {
    private ImmutableDoubleArraySource mainKeySource0;

    private ImmutableDoubleArraySource alternateKeySource0;

    public IncrementalAggOpenHasherDouble(ColumnSource[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableDoubleArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    private int alternateNextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (alternateTableSize - 1);
    }

    protected void build(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
                if (outputPosition == EMPTY_OUTPUT_POSITION) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        outputPosition = alternateOutputPosition.getUnsafe(alternateTableLocation);
                        if (outputPosition == EMPTY_OUTPUT_POSITION) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            outputPositions.set(chunkPosition, outputPosition);
                            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
                            rowCountSource.set(outputPosition, oldRowCount + 1);
                            Assert.gtZero(oldRowCount, "oldRowCount");
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = nextTableLocation(alternateTableLocation);
                            Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                        }
                    }
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    outputPosition = nextOutputPosition.getAndIncrement();
                    outputPositions.set(chunkPosition, outputPosition);
                    mainOutputPosition.set(tableLocation, outputPosition);
                    outputPositionToHashSlot.set(outputPosition, tableLocation);
                    outputPositionToHashSlot.set(outputPosition, mainInsertMask | tableLocation);
                    rowCountSource.set(outputPosition, 1L);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, outputPosition);
                    final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
                    rowCountSource.set(outputPosition, oldRowCount + 1);
                    Assert.gtZero(oldRowCount, "oldRowCount");
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void buildForUpdate(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            WritableIntChunk<RowKeys> reincarnatedPositions) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            MAIN_SEARCH: while (true) {
                int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
                if (outputPosition == EMPTY_OUTPUT_POSITION) {
                    final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                    int alternateTableLocation = firstAlternateTableLocation;
                    while (alternateTableLocation < rehashPointer) {
                        outputPosition = alternateOutputPosition.getUnsafe(alternateTableLocation);
                        if (outputPosition == EMPTY_OUTPUT_POSITION) {
                            break;
                        } else if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            outputPositions.set(chunkPosition, outputPosition);
                            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
                            rowCountSource.set(outputPosition, oldRowCount + 1);
                            if (oldRowCount == 0) {
                                reincarnatedPositions.add(outputPosition);
                            }
                            break MAIN_SEARCH;
                        } else {
                            alternateTableLocation = nextTableLocation(alternateTableLocation);
                            Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                        }
                    }
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    outputPosition = nextOutputPosition.getAndIncrement();
                    outputPositions.set(chunkPosition, outputPosition);
                    mainOutputPosition.set(tableLocation, outputPosition);
                    outputPositionToHashSlot.set(outputPosition, tableLocation);
                    outputPositionToHashSlot.set(outputPosition, mainInsertMask | tableLocation);
                    rowCountSource.set(outputPosition, 1L);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, outputPosition);
                    final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
                    rowCountSource.set(outputPosition, oldRowCount + 1);
                    if (oldRowCount == 0) {
                        reincarnatedPositions.add(outputPosition);
                    }
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    protected void doRemoveProbe(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            WritableIntChunk<RowKeys> emptiedPositions) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            int outputPosition;
            while ((outputPosition = mainOutputPosition.getUnsafe(tableLocation)) != EMPTY_OUTPUT_POSITION) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, outputPosition);
                    final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
                    Assert.gtZero(oldRowCount, "oldRowCount");
                    if (oldRowCount == 1) {
                        emptiedPositions.add(outputPosition);
                    }
                    rowCountSource.set(outputPosition, oldRowCount - 1);
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
                final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                boolean alternateFound = false;
                if (firstAlternateTableLocation < rehashPointer) {
                    int alternateTableLocation = firstAlternateTableLocation;
                    while ((outputPosition = alternateOutputPosition.getUnsafe(alternateTableLocation)) != EMPTY_OUTPUT_POSITION) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            outputPositions.set(chunkPosition, outputPosition);
                            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
                            Assert.gtZero(oldRowCount, "oldRowCount");
                            if (oldRowCount == 1) {
                                emptiedPositions.add(outputPosition);
                            }
                            rowCountSource.set(outputPosition, oldRowCount - 1);
                            alternateFound = true;
                            break;
                        }
                        alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                        Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                    }
                }
                if (!alternateFound) {
                    throw new IllegalStateException("Missing value in probe");
                }
            }
        }
    }

    protected void doModifyProbe(RowSequence rowSequence, Chunk[] sourceKeyChunks) {
        final DoubleChunk<Values> keyChunk0 = sourceKeyChunks[0].asDoubleChunk();
        final int chunkSize = keyChunk0.size();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final double k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            boolean found = false;
            int tableLocation = firstTableLocation;
            int outputPosition;
            while ((outputPosition = mainOutputPosition.getUnsafe(tableLocation)) != EMPTY_OUTPUT_POSITION) {
                if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    outputPositions.set(chunkPosition, outputPosition);
                    found = true;
                    break;
                }
                tableLocation = nextTableLocation(tableLocation);
                Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
            }
            if (!found) {
                final int firstAlternateTableLocation = hashToTableLocationAlternate(hash);
                boolean alternateFound = false;
                if (firstAlternateTableLocation < rehashPointer) {
                    int alternateTableLocation = firstAlternateTableLocation;
                    while ((outputPosition = alternateOutputPosition.getUnsafe(alternateTableLocation)) != EMPTY_OUTPUT_POSITION) {
                        if (eq(alternateKeySource0.getUnsafe(alternateTableLocation), k0)) {
                            outputPositions.set(chunkPosition, outputPosition);
                            alternateFound = true;
                            break;
                        }
                        alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                        Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                    }
                }
                if (!alternateFound) {
                    throw new IllegalStateException("Missing value in probe");
                }
            }
        }
    }

    private static int hash(double k0) {
        int hash = DoubleChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private boolean migrateOneLocation(int locationToMigrate) {
        final int currentStateValue = alternateOutputPosition.getUnsafe(locationToMigrate);
        if (currentStateValue == EMPTY_OUTPUT_POSITION) {
            return false;
        }
        final double k0 = alternateKeySource0.getUnsafe(locationToMigrate);
        final int hash = hash(k0);
        int destinationLocation = hashToTableLocation(hash);
        while (mainOutputPosition.getUnsafe(destinationLocation) != EMPTY_OUTPUT_POSITION) {
            destinationLocation = nextTableLocation(destinationLocation);
        }
        mainKeySource0.set(destinationLocation, k0);
        mainOutputPosition.set(destinationLocation, currentStateValue);
        outputPositionToHashSlot.set(currentStateValue, mainInsertMask | destinationLocation);
        alternateOutputPosition.set(locationToMigrate, EMPTY_OUTPUT_POSITION);
        return true;
    }

    @Override
    protected void rehashInternalPartial(int targetRehashPointer) {
        while (rehashPointer > targetRehashPointer) {
            migrateOneLocation(--rehashPointer);
        }
    }

    @Override
    protected void newAlternate() {
        super.newAlternate();
        this.mainKeySource0 = (ImmutableDoubleArraySource)super.mainKeySources[0];
        this.alternateKeySource0 = (ImmutableDoubleArraySource)super.alternateKeySources[0];
    }

    @Override
    protected void clearAlternate() {
        super.clearAlternate();
        this.alternateOutputPosition = null;
        this.alternateKeySource0 = null;
    }

    @Override
    protected void migrateFront() {
        int location = 0;;
        while (migrateOneLocation(location++));
    }

    @Override
    public int findPositionForKey(Object key) {
        final double k0 = TypeUtils.unbox((Double)key);
        int hash = hash(k0);
        int tableLocation = hashToTableLocation(hash);
        final int firstTableLocation = tableLocation;
        while (true) {
            final int positionValue = mainOutputPosition.getUnsafe(tableLocation);
            if (positionValue == EMPTY_OUTPUT_POSITION) {
                int alternateTableLocation = hashToTableLocationAlternate(hash);
                if (alternateTableLocation >= rehashPointer) {
                    return -1;
                }
                final int firstAlternateTableLocation = alternateTableLocation;
                while (true) {
                    final int alternatePositionValue = mainOutputPosition.getUnsafe(alternateTableLocation);
                    if (alternatePositionValue == EMPTY_OUTPUT_POSITION) {
                        return -1;
                    }
                    if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                        return alternatePositionValue;
                    }
                    alternateTableLocation = alternateNextTableLocation(alternateTableLocation);
                    Assert.neq(alternateTableLocation, "alternateTableLocation", firstAlternateTableLocation, "firstAlternateTableLocation");
                }
            }
            if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                return positionValue;
            }
            tableLocation = nextTableLocation(tableLocation);
            Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
        }
    }
}
