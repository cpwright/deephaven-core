package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportManagerClientBase.State;
import io.deephaven.qst.manager.ExportedTable;
import io.deephaven.qst.table.Table;
import io.deephaven.qst.todo.TableSize.TableSizeListener;
import io.deephaven.qst.todo.TableSize.TableSizeSubscription;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArraySet;

class Export implements ExportedTable {

    public static Export cast(ExportedTable table) {
        if (!(table instanceof Export)) {
            throw new IllegalArgumentException(
                String.format("Table is not instance of %s", Export.class));
        }
        return (Export) table;
    }

    private final ExportManagerClientBase.State state;
    private final CopyOnWriteArraySet<TableSizeListener> listeners;
    private boolean released;

    Export(ExportManagerClientBase.State state) {
        this.state = Objects.requireNonNull(state);
        this.listeners = new CopyOnWriteArraySet<>();
        this.released = false;
        state.onNewExport(this);
    }

    public final OptionalLong size() {
        return state.size();
    }

    public final long awaitSize() throws InterruptedException {
        return state.awaitSize();
    }

    void notifySizeChange(long size) {
        for (TableSizeListener listener : listeners) {
            listener.onSize(size);
        }
    }

    public final TableSizeSubscription addListener(TableSizeListener consumer) {
        listeners.add(consumer);
        // slightly racy, may be double notified
        size().ifPresent(consumer::onSize);
        return () -> listeners.remove(consumer);
    }

    public synchronized final boolean isReleased() {
        return released;
    }

    public final long ticket() {
        return state.ticket();
    }

    final State state() {
        return state;
    }

    @Override
    public final Table table() {
        return state.table();
    }

    @Override
    public synchronized final ExportedTable newRef() {
        if (released) {
            throw new IllegalStateException("Should not take newRef after release");
        }
        return state.newRef();
    }

    @Override
    public synchronized final void release() {
        if (released) {
            return;
        }
        state.onReleased(this);
        released = true;
    }

    @Override
    public final String toString() {
        return "ExportedTableImpl{" + "ticket=" + state.ticket() + '}';
    }
}
