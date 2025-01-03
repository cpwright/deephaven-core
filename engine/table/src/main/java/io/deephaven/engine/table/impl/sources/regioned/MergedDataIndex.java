//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
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
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.util.*;
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
    public static final Value loadIndexAndShift = Stats.makeItem("DataIndex", "loadAndShift", Counter.FACTORY, "Duration in nanos of loading and shifting a data index").getValue();
    public static final Value buildTable = Stats.makeItem("DataIndex", "buildTable", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value loadWallTime = Stats.makeItem("DataIndex", "loadWallTime", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value union = Stats.makeItem("DataIndex", "union", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value select = Stats.makeItem("DataIndex", "select", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value grouping = Stats.makeItem("DataIndex", "grouping", Counter.FACTORY, "Duration in nanos of building an index").getValue();
    public static final Value unionSize = Stats.makeItem("DataIndex", "mergedSize", Counter.FACTORY, "Size of merged data indices").getValue();
    public static final Value groupedSize = Stats.makeItem("DataIndex", "mergedSize", Counter.FACTORY, "Size of grouped data indices").getValue();
    public static final Value mergeAndCleanup = Stats.makeItem("DataIndex", "mergeAndCleanup", Counter.FACTORY, "Duration in nanos of building an index").getValue();

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
    public Table table() {
        Table localIndexTable;
        if ((localIndexTable = indexTable) != null) {
            return localIndexTable;
        }
        synchronized (this) {
            if ((localIndexTable = indexTable) != null) {
                return localIndexTable;
            }
            try {
                return QueryPerformanceRecorder.withNugget(
                        String.format("Merge Data Indexes [%s]", String.join(", ", keyColumnNames)),
                        ForkJoinPoolOperationInitializer.ensureParallelizable(this::buildTable));
            } catch (Throwable t) {
                isCorrupt = true;
                throw t;
            }
        }
    }

    private Table buildTable() {
        final long t0 = System.nanoTime();
        try {
            final Table locationTable = columnSourceManager.locationTable().coalesce();

            // Perform a parallelizable update to produce coalesced location index tables with their row sets shifted by
            // the appropriate region offset.
            // This potentially loads many small row sets into memory, but it avoids the risk of re-materializing row set
            // pages during the accumulation phase.
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
            final PartitionedTable transform = PartitionedTableFactory.of(locationDataIndexes).transform(t -> t.update(keyColumnNamesArray));

            final long t2_25 = System.nanoTime();
            select.sample(t2_25 - t2);

            final Table mergedDataIndexes = transform.merge();
            final long t2_5 = System.nanoTime();
            union.sample(t2_5 - t2_25);
            unionSize.sample(mergedDataIndexes.size());

            // Group the merged data indexes by the keys
            final Table groupedByKeyColumns = mergedDataIndexes.groupBy(keyColumnNamesArray);

            final long t3 = System.nanoTime();
            grouping.sample(t3 - t2_5);
            groupedSize.sample(groupedByKeyColumns.size());

            // Combine the row sets from each group into a single row set
            final Table combined = groupedByKeyColumns
                    .updateView(List.of(SelectColumn.ofStateless(new FunctionalColumn<>(
                            ROW_SET_COLUMN_NAME, ObjectVector.class,
                            ROW_SET_COLUMN_NAME, RowSet.class,
                            this::mergeRowSets))));
            Assert.assertion(combined.isFlat(), "combined.isFlat()");
            Assert.eq(groupedByKeyColumns.size(), "groupedByKeyColumns.size()", combined.size(), "combined.size()");

            // Cleanup after ourselves
//            try (final CloseableIterator<RowSet> rowSets = mergedDataIndexes.objectColumnIterator(ROW_SET_COLUMN_NAME)) {
//                rowSets.forEachRemaining(SafeCloseable::close);
//            }

            lookupFunction = AggregationProcessor.getRowLookup(groupedByKeyColumns);
            indexTable = combined;

            final long t4 = System.nanoTime();
            mergeAndCleanup.sample(t4 - t3);

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
            if (shiftAmount == 0) {
                return coalesced.updateView(List.of(SelectColumn.ofStateless(Selectable.of(ColumnName.of(ROW_SET_COLUMN_NAME), ColumnName.of(dataIndex.rowSetColumnName())))));
            }
            return coalesced.updateView(List.of(SelectColumn.ofStateless(new FunctionalColumn<>(
                    dataIndex.rowSetColumnName(), RowSet.class,
                    ROW_SET_COLUMN_NAME, RowSet.class,
                    (final RowSet rowSet) -> rowSet.shift(shiftAmount)))));
        } finally {
            final long t1 = System.nanoTime();
            loadIndexAndShift.sample(t1 - t0);
        }
    }

    private RowSet mergeRowSets(
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
    public RowKeyLookup rowKeyLookup() {
        table();
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
//            return isValid = locations.stream().parallel().allMatch(l -> l.hasDataIndex(keyColumnNamesArray));
            while (locations.hasNext()) {
                if (!locations.next().hasDataIndex(keyColumnNamesArray)) {
                    return isValid = false;
                }
            }
        }
        return isValid = true;
    }
}
