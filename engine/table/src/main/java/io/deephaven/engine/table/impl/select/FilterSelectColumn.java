//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterSelectColumn<S> implements SelectColumn {

    // We don't actually care to do anything, but cannot return null
    private static final Formula.FillContext FILL_CONTEXT_INSTANCE = new Formula.FillContext() {
    };

    @NotNull private final String destName;
    @NotNull private final WhereFilter filter;

    private RowSet rowSet;
    private Table tableToFilter;

    public FilterSelectColumn(@NotNull final String destName, @NotNull final WhereFilter filter) {
        this.destName = destName;
        this.filter = filter;
    }

    @Override
    public String toString() {
        return "filter(" + filter + ')';
    }

    @Override
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        this.rowSet = rowSet;
        this.tableToFilter = new QueryTable(rowSet, columnsOfInterest);
        return filter.getColumns();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        final List<ColumnHeader<?>> columnHeaders = new ArrayList<>();
        for (Map.Entry<String, ColumnDefinition<?>> entry : columnDefinitionMap.entrySet()) {
            columnHeaders.add(ColumnHeader.of(entry.getKey(), entry.getValue().getDataType()));
        }
        final TableDefinition fromColumns = TableDefinition.from(columnHeaders);
        // TODO: Compilation processor!
        filter.init(fromColumns);

        if (!filter.getColumnArrays().isEmpty()) {
            throw new UncheckedTableException("Cannot use a filter with column Vectors (_ syntax) in select, view, update, or updateView: " + filter);
        }
        if (filter.hasVirtualRowVariables()) {
            throw new UncheckedTableException("Cannot use a filter with virtual row variables (i, ii, or k) in select, view, update, or updateView: " + filter);
        }

        return filter.getColumns();
    }

    @Override
    public Class<?> getReturnedType() {
        return Boolean.class;
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return null;
    }

    @Override
    public List<String> getColumns() {
        return filter.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        return List.of();
    }

    @Override
    public boolean hasVirtualRowVariables() {
        return filter.hasVirtualRowVariables();
    }

    @NotNull
    @Override
    public ColumnSource<Boolean> getDataView() {
        return new ViewColumnSource<>(Boolean.class, new Formula(null) {

            @Override
            public Boolean getBoolean(long rowKey) {
                return filter.filter(RowSetFactory.fromKeys(rowKey), rowSet, tableToFilter, false).containsRange(rowKey, rowKey);
            }

            @Override
            public Boolean getPrevBoolean(long rowKey) {
                return filter.filter(RowSetFactory.fromKeys(rowKey), rowSet, tableToFilter, true).containsRange(rowKey, rowKey);
            }

            @Override
            public Object get(final long rowKey) {
                return TypeUtils.box(getBoolean(rowKey));
            }

            @Override
            public Object getPrev(final long rowKey) {
                return TypeUtils.box(getPrevBoolean(rowKey));
            }

            @Override
            public ChunkType getChunkType() {
                return ChunkType.Object;
            }

            @Override
            public FillContext makeFillContext(final int chunkCapacity) {
                return FILL_CONTEXT_INSTANCE;
            }

            @Override
            public void fillChunk(
                    @NotNull final FillContext fillContext,
                    @NotNull final WritableChunk<? super Values> destination,
                    @NotNull final RowSequence rowSequence) {
                doFill(rowSequence, destination, false);
            }

            private void doFill(@NotNull RowSequence rowSequence, WritableChunk<? super Values> destination, boolean usePrev) {
                final WritableObjectChunk<Boolean, ?> booleanDestination = destination.asWritableObjectChunk();
                try (final RowSet inputRowSet = rowSequence.asRowSet();
                    final RowSet filtered = filter.filter(inputRowSet, rowSet, tableToFilter, usePrev);
                     final RowSet.Iterator inputIt = inputRowSet.iterator();
                     final RowSet.Iterator trueIt = filtered.iterator()) {
                    long nextTrue = trueIt.hasNext() ? trueIt.nextLong() : -1;
                    int offset = 0;
                    while (nextTrue >= 0 && inputIt.hasNext()) {
                        final long nextInput = inputIt.nextLong();
                        final boolean found = nextInput == nextTrue;
                        booleanDestination.set(offset++, found);
                        if (found) {
                            nextTrue = trueIt.hasNext() ? trueIt.nextLong() : -1;
                        }
                    }
                    // fill everything else up with false, because nothing else can match
                    booleanDestination.fillWithBoxedValue(offset, booleanDestination.size() - offset, false);
                }
            }

            @Override
            public void fillPrevChunk(
                    @NotNull final FillContext fillContext,
                    @NotNull final WritableChunk<? super Values> destination,
                    @NotNull final RowSequence rowSequence) {
                doFill(rowSequence, destination, true);
            }
        }, false);
    }

    @NotNull
    @Override
    public ColumnSource<?> getLazyView() {
        return getDataView();
    }

    @Override
    public String getName() {
        return destName;
    }

    @Override
    public MatchPair getMatchPair() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final WritableColumnSource<?> newDestInstance(final long size) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(size, Boolean.class);
    }

    @Override
    public final WritableColumnSource<?> newFlatDestInstance(final long size) {
        return InMemoryColumnSource.getImmutableMemoryColumnSource(size, Boolean.class, null);
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean isStateless() {
        return false;
    }

    @Override
    public FilterSelectColumn<S> copy() {
        return new FilterSelectColumn<>(destName, filter.copy());
    }
}