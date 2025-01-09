//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.stats.Counter;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.Value;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ForkJoinPoolOperationInitializer;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.dataindex.AbstractDataIndex;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.MultiSourceFunctionalColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;

/**
 * DataIndex that accumulates the individual per-{@link TableLocation} data indexes of a {@link Table} backed by a
 * {@link RegionedColumnSourceManager}.
 *
 * @implNote This implementation is responsible for ensuring that the provided table accounts for the relative positions
 *           of individual table locations in the provided table of indices. Work to coalesce the index table is
 *           deferred until the first call to {@link #table()}. Refreshing inputs/indexes are not supported at this time
 *           due to concurrency limitations (w.r.t. the UpdateGraph) of the underlying table operations used to compute
 *           the merged index table, as well as a lack of use cases beyond "new static partitions are added to a live
 *           source table".
 */
@InternalUseOnly
class MergedDataIndex extends AbstractDataIndex {
    public static final Value loadIndexAndShift = Stats.makeItem("DataIndex", "loadAndShift", Counter.FACTORY,
            "Duration in nanos of loading and shifting a data index").getValue();
    public static final Value loadSelect = Stats.makeItem("DataIndex", "loadSelect", Counter.FACTORY,
            "Duration in nanos of loading and shifting a data index").getValue();
    public static final Value buildTable = Stats
            .makeItem("DataIndex", "buildTable", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value loadWallTime =
            Stats.makeItem("DataIndex", "loadWallTime", Counter.FACTORY, "Duration in nanos of building an index")
                    .getValue();
    public static final Value union =
            Stats.makeItem("DataIndex", "union", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value grouping = Stats
            .makeItem("DataIndex", "grouping", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value unionSize =
            Stats.makeItem("DataIndex", "mergedSize", Counter.FACTORY, "Size of merged data indices").getValue();
    public static final Value groupedSize =
            Stats.makeItem("DataIndex", "mergedSize", Counter.FACTORY, "Size of grouped data indices").getValue();
    public static final Value mergeAndCleanup =
            Stats.makeItem("DataIndex", "mergeAndCleanup", Counter.FACTORY, "Duration in nanos of building an index")
                    .getValue();
    public static final Value lazyGeneration =
            Stats.makeItem("DataIndex", "lazyGeneration", Counter.FACTORY, "Duration in nanos of building an index")
                    .getValue();
    public static final Value lazyFetch = Stats
            .makeItem("DataIndex", "lazyFetch", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value cachedFetch =
            Stats.makeItem("DataIndex", "cachedFetch", Counter.FACTORY, "How often a fetch results in a cached result")
                    .getValue();

    private static final String LOCATION_DATA_INDEX_TABLE_COLUMN_NAME = "__DataIndexTable";

    private final List<String> keyColumnNames;
    private final RegionedColumnSourceManager columnSourceManager;

    private final Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn;

    /**
     * The lookup function for the index table. Note that this is always set before {@link #indexTable}.
     */
    private AggregationRowLookup lookupFunction;

    /**
     * The index table. Note that we use this as a barrier to ensure {@link #lookupFunction} is visible.
     */
    private volatile Table indexTable;

    /**
     * A lazy version of the table, note this value is never set if indexTable is set. A lazy table can be converted to
     * an indexTable by selecting the rowset column.
     */
    private volatile Table lazyTable;

    /**
     * Whether this index is known to be corrupt.
     */
    private volatile boolean isCorrupt;

    /**
     * Whether this index is valid. {@code null} means we don't know, yet.
     */
    private volatile Boolean isValid;

    MergedDataIndex(
            @NotNull final String[] keyColumnNames,
            @NotNull final ColumnSource<?>[] keySources,
            @NotNull final RegionedColumnSourceManager columnSourceManager) {
        Require.eq(keyColumnNames.length, "keyColumnNames.length", keySources.length, "keySources.length");
        Require.elementsNeqNull(keyColumnNames, "keyColumnNames");
        Require.elementsNeqNull(keySources, "keySources");

        this.keyColumnNames = List.of(keyColumnNames);
        this.columnSourceManager = columnSourceManager;

        // Create an in-order reverse lookup map for the key column names
        keyColumnNamesByIndexedColumn = Collections.unmodifiableMap(IntStream.range(0, keySources.length).sequential()
                .collect(LinkedHashMap::new, (m, i) -> m.put(keySources[i], keyColumnNames[i]), Assert::neverInvoked));
        if (keyColumnNamesByIndexedColumn.size() != keySources.length) {
            throw new IllegalArgumentException(String.format("Duplicate key sources found in %s for %s",
                    Arrays.toString(keySources), Arrays.toString(keyColumnNames)));
        }

        if (columnSourceManager.locationTable().isRefreshing()) {
            throw new UnsupportedOperationException("Refreshing location tables are not currently supported");
        }

        // Defer the actual index table creation until it is needed
    }

    @Override
    @NotNull
    public List<String> keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
        return keyColumnNamesByIndexedColumn;
    }

    @Override
    @NotNull
    public Table table(final DataIndexOptions options) {
        Table localIndexTable;
        if ((localIndexTable = indexTable) != null) {
            return localIndexTable;
        }
        final boolean lazyRowsetMerge = options.operationUsesPartialTable();
        if (lazyRowsetMerge) {
            if ((localIndexTable = lazyTable) != null) {
                return localIndexTable;
            }
        }
        synchronized (this) {
            if ((localIndexTable = indexTable) != null) {
                return localIndexTable;
            } else if (lazyRowsetMerge) {
                if ((localIndexTable = lazyTable) != null) {
                    return localIndexTable;
                }
            }
            try {
                return QueryPerformanceRecorder.withNugget(
                        String.format("Merge Data Indexes [%s]", String.join(", ", keyColumnNames)),
                        ForkJoinPoolOperationInitializer.ensureParallelizable(() -> buildTable(lazyRowsetMerge)));
            } catch (Throwable t) {
                isCorrupt = true;
                throw t;
            }
        }
    }

    /**
     * The RowSetCacher is a bit of a hack that allows us to avoid reading actual rowsets from disk until they are
     * actually required for a query operation. We are breaking engine rules in that we reference the source
     * ColumnSource directly and do not have correct dependencies encoded in a select. MergedDataIndexes are only
     * permitted for a static table, so we can get away with this.
     *
     * Once a RowSet has been written, we write down the result into an ObjectArraySource so that it need not be read
     * repeatedly.
     */
    private static class RowsetCacher {
        final ColumnSource<ObjectVector<RowSet>> source;
        final AtomicReferenceArray<Object> results;

        private RowsetCacher(final ColumnSource<ObjectVector<RowSet>> source, final int capacity) {
            this.source = source;
            this.results = new AtomicReferenceArray<>(capacity);
        }

        // TODO: locking and not redo the work
        RowSet get(final long rowKey) {
            final long t0 = System.nanoTime();
            try {
                if (rowKey < 0 || rowKey >= results.length()) {
                    return null;
                }

                int iRowKey = (int) rowKey;
                do {
                    final Object localResult = results.get(iRowKey);
                    if (localResult instanceof RowSet) {
                        cachedFetch.sample(1);
                        return (RowSet) localResult;
                    }

                    if (localResult != null) {
                        synchronized (localResult) {
                            // don't care to do anything, we are just waiting for the barrier to be done
                        }
                        continue;
                    }

                    // we need to create our own placeholder, and synchronize on it first
                    final Object placeholder = new Object();
                    // noinspection SynchronizationOnLocalVariableOrMethodParameter
                    synchronized (placeholder) {
                        if (!results.compareAndSet(iRowKey, null, placeholder)) {
                            // we must try again, someone else has claimed the placeholder
                            continue;
                        }
                        // it is our responsibility to get the right answer
                        final ObjectVector<RowSet> inputRowsets = source.get(rowKey);
                        Assert.neqNull(inputRowsets, "inputRowsets");
                        assert inputRowsets != null;

                        // need to get the value and set it into our own value
                        final RowSet computedResult = mergeRowSets(rowKey, inputRowsets);
                        if (!results.compareAndSet(iRowKey, placeholder, computedResult)) {
                            throw new IllegalStateException("another thread changed our cache placeholder!");
                        }

                        // close the components; we are never going to look at them again
                        inputRowsets.forEach(RowSet::close);

                        return computedResult;
                    }
                } while (true);
            } finally {
                final long t1 = System.nanoTime();
                lazyFetch.sample(t1 - t0);
            }
        }
    }

    private Table buildTable(final boolean lazyRowsetMerge) {
        if (lazyTable != null) {
            if (lazyRowsetMerge) {
                return lazyTable;
            } else {
                indexTable = lazyTable.select();
                lazyTable = null;
                return indexTable;
            }
        }

        final long t0 = System.nanoTime();
        try {
            final Table locationTable = columnSourceManager.locationTable().coalesce();

            // Perform a parallelizable update to produce coalesced location index tables with their row sets shifted by
            // the appropriate region offset. The row sets are not forced into memory; but keys are to enable efficient
            // grouping. The rowsets are read into memory as part of the {@link #mergeRowSets} call.
            final String[] keyColumnNamesArray = keyColumnNames.toArray(String[]::new);
            final Table locationDataIndexes = locationTable
                    .update(List.of(SelectColumn.ofStateless(new FunctionalColumn<>(
                            columnSourceManager.locationColumnName(), TableLocation.class,
                            LOCATION_DATA_INDEX_TABLE_COLUMN_NAME, Table.class,
                            (final long locationRowKey, final TableLocation location) -> loadIndexTableAndShiftRowSets(
                                    locationRowKey, location, keyColumnNamesArray)))))
                    .dropColumns(columnSourceManager.locationColumnName());

            final long t2 = System.nanoTime();
            loadWallTime.sample(t2 - t0);

            // Merge all the location index tables into a single table
            final Table mergedDataIndexes = PartitionedTableFactory.of(locationDataIndexes).merge();
            final long t2_5 = System.nanoTime();
            union.sample(t2_5 - t2);
            unionSize.sample(mergedDataIndexes.size());

            // Group the merged data indexes by the keys
            final Table groupedByKeyColumns = mergedDataIndexes.groupBy(keyColumnNamesArray);

            final long t3 = System.nanoTime();
            grouping.sample(t3 - t2_5);
            groupedSize.sample(groupedByKeyColumns.size());


            final Table combined;
            if (lazyRowsetMerge) {
                final ColumnSource<ObjectVector<RowSet>> vectorColumnSource =
                        groupedByKeyColumns.getColumnSource(ROW_SET_COLUMN_NAME);

                final RowsetCacher rowsetCacher = new RowsetCacher(vectorColumnSource, groupedByKeyColumns.size());
                // need to do something better with a magic holder that looks at the rowset column source and lazily
                // merges them instead of this version that actually wants to have an input and doesn't cache anything
                combined = groupedByKeyColumns
                        .view(List.of(SelectColumn.ofStateless(new MultiSourceFunctionalColumn<>(List.of(),
                                ROW_SET_COLUMN_NAME, RowSet.class, (k, v) -> rowsetCacher.get(k)))));
                final long t4 = System.nanoTime();
                lazyGeneration.sample(t4 - t3);
            } else {
                // Combine the row sets from each group into a single row set
                final List<SelectColumn> mergeFunction = List.of(SelectColumn.ofStateless(new FunctionalColumn<>(
                        ROW_SET_COLUMN_NAME, ObjectVector.class,
                        ROW_SET_COLUMN_NAME, RowSet.class,
                        MergedDataIndex::mergeRowSets)));

                combined = groupedByKeyColumns.update(mergeFunction);
                // Cleanup after ourselves
                try (final CloseableIterator<RowSet> rowSets =
                        mergedDataIndexes.objectColumnIterator(ROW_SET_COLUMN_NAME)) {
                    rowSets.forEachRemaining(SafeCloseable::close);
                }
                final long t4 = System.nanoTime();
                mergeAndCleanup.sample(t4 - t3);
            }
            Assert.assertion(combined.isFlat(), "combined.isFlat()");
            Assert.eq(groupedByKeyColumns.size(), "groupedByKeyColumns.size()", combined.size(), "combined.size()");

            lookupFunction = AggregationProcessor.getRowLookup(groupedByKeyColumns);

            if (lazyRowsetMerge) {
                lazyTable = combined;
            } else {
                indexTable = combined;
            }

            return combined;
        } finally {
            final long t1 = System.nanoTime();
            buildTable.sample(t1 - t0);
        }
    }

    private static Table loadIndexTableAndShiftRowSets(
            final long locationRowKey,
            @NotNull final TableLocation location,
            @NotNull final String[] keyColumnNames) {
        final long t0 = System.nanoTime();
        try {
            final BasicDataIndex dataIndex = location.getDataIndex(keyColumnNames);
            if (dataIndex == null) {
                throw new UncheckedDeephavenException(String.format("Failed to load data index [%s] for location %s",
                        String.join(", ", keyColumnNames), location));
            }
            final Table indexTable = dataIndex.table();
            final long shiftAmount = RegionedColumnSource.getFirstRowKey(Math.toIntExact(locationRowKey));
            final Table coalesced = indexTable.coalesce();

            final long t2 = System.nanoTime();
            try {
                // pull the key columns into memory while we are parallel;
                // the rowset column shift need not occur until we perform the rowset merge operation - which is either
                // lazy or part of an update [which itself can be parallel].
                return coalesced.update(keyColumnNames)
                        .updateView(List.of(SelectColumn.ofStateless(new FunctionalColumn<>(
                                dataIndex.rowSetColumnName(), RowSet.class,
                                ROW_SET_COLUMN_NAME, RowSet.class,
                                (final RowSet rowSet) -> rowSet.shift(shiftAmount)))));
            } finally {
                final long t3 = System.nanoTime();
                loadSelect.sample(t3 - t2);
            }
        } finally {
            final long t1 = System.nanoTime();
            loadIndexAndShift.sample(t1 - t0);
        }
    }

    private static RowSet mergeRowSets(
            @SuppressWarnings("unused") final long unusedRowKey,
            @NotNull final ObjectVector<RowSet> keyRowSets) {
        if (keyRowSets.size() == 1) {
            return keyRowSets.get(0).copy();
        }
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        try (final CloseableIterator<RowSet> rowSets = keyRowSets.iterator()) {
            rowSets.forEachRemaining(builder::appendRowSequence);
        }
        return builder.build();
    }

    @Override
    @NotNull
    public RowKeyLookup rowKeyLookup(final DataIndexOptions options) {
        table(options);
        return (final Object key, final boolean usePrev) -> {
            // Pass the object to the aggregation lookup, then return the resulting row position (which is also the row
            // key).
            final int keyRowPosition = lookupFunction.get(key);
            if (keyRowPosition == lookupFunction.noEntryValue()) {
                return RowSequence.NULL_ROW_KEY;
            }
            return keyRowPosition;
        };
    }

    @Override
    public boolean isRefreshing() {
        return false;
    }

    @Override
    public boolean isValid() {
        if (isCorrupt) {
            return false;
        }
        if (isValid != null) {
            return isValid;
        }
        final String[] keyColumnNamesArray = keyColumnNames.toArray(String[]::new);
        try (final CloseableIterator<TableLocation> locations =
                columnSourceManager.locationTable().objectColumnIterator(columnSourceManager.locationColumnName())) {
            return isValid = locations.stream().parallel().allMatch(l -> l.hasDataIndex(keyColumnNamesArray));
        }
    }
}
