package io.deephaven.client.impl;

import io.deephaven.qst.manager.ExportedTable;
import io.deephaven.qst.todo.TableSize;
import java.util.OptionalLong;

public enum TableSizeImpl implements TableSize {
    INSTANCE;

    @Override
    public final OptionalLong size(ExportedTable export) {
        return Export.cast(export).size();
    }

    @Override
    public final long awaitSize(ExportedTable export) throws InterruptedException {
        return Export.cast(export).awaitSize();
    }

    @Override
    public final TableSizeSubscription addListener(ExportedTable export,
        TableSizeListener consumer) {
        return Export.cast(export).addListener(consumer);
    }
}
