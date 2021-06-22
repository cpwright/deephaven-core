package io.deephaven.qst;

import io.deephaven.qst.manager.ExportedTable;
import io.deephaven.qst.todo.TableSize;
import java.util.OptionalLong;
import java.util.function.LongConsumer;

public enum TableSizeDb implements TableSize {
    INSTANCE;

    @Override
    public final OptionalLong size(ExportedTable export) {
        return OptionalLong.of(ExportManagerDb.cast(export).size());
    }

    @Override
    public final long awaitSize(ExportedTable export) {
        return ExportManagerDb.cast(export).size();
    }

    @Override
    public final TableSizeSubscription addListener(ExportedTable export, TableSizeListener consumer) {
        throw new UnsupportedOperationException("TODO");
    }
}
