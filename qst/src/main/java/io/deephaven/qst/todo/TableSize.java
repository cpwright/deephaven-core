package io.deephaven.qst.todo;

import io.deephaven.qst.manager.ExportedTable;
import java.io.Closeable;
import java.util.OptionalLong;

public interface TableSize {

    interface TableSizeListener {
        void onSize(long size);
    }

    interface TableSizeSubscription extends Closeable {

        @Override
        void close();
    }

    OptionalLong size(ExportedTable export);

    long awaitSize(ExportedTable export) throws InterruptedException;

    TableSizeSubscription addListener(ExportedTable export, TableSizeListener consumer);
}