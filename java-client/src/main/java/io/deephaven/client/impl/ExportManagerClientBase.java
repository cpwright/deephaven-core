package io.deephaven.client.impl;

import static io.deephaven.client.impl.BatchTableRequestBuilder.byteStringToLong;
import static io.deephaven.client.impl.BatchTableRequestBuilder.longToByteString;

import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.manager.ExportManager;
import io.deephaven.qst.table.Table;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExportManagerClientBase implements ExportManager {

    private static final Logger log = LoggerFactory.getLogger(ExportManagerClientBase.class);

    class State {

        private final Table table;
        private final long ticket;
        // Note: consider keeping around state machine for the various transitions the ticket
        // goes through. Would allow slightly better BatchTableRequestBuilder signature and impl

        private int localRefs; // note: all release ops go through the ExportManager which will
                               // guard this

        private final Object sizeObj = new Object();
        private long size;

        private final CopyOnWriteArraySet<Export> children;

        State(Table table, long ticket) {
            this.table = Objects.requireNonNull(table);
            this.ticket = ticket;
            this.localRefs = 1;
            this.size = -1;
            this.children = new CopyOnWriteArraySet<>();
            exports.put(table, this);
        }

        void onNewExport(Export export) {
            children.add(export);
        }

        void onReleased(Export export) {
            decRef();
            children.remove(export);
        }

        boolean hasTicket(long ticket) {
            return this.ticket == ticket;
        }

        ExportManagerClientBase manager() {
            return ExportManagerClientBase.this;
        }

        Table table() {
            return table;
        }

        long ticket() {
            return ticket;
        }

        void incRef() {
            synchronized (manager()) {
                if (localRefs <= 0) {
                    throw new IllegalStateException();
                }
                localRefs += 1;
            }
        }

        void decRef() {
            synchronized (manager()) {
                if (localRefs <= 0) {
                    throw new IllegalStateException();
                }
                localRefs -= 1;
                if (localRefs == 0) {
                    exports.remove(table);
                    executeRelease(Ticket.newBuilder().setId(longToByteString(ticket)).build());
                }
            }
        }

        Export newRef() {
            incRef();
            return new Export(this);
        }

        void notifySizeChange(long size) {
            synchronized (sizeObj) {
                this.size = size;
                sizeObj.notifyAll();
            }
            for (Export child : children) {
                child.notifySizeChange(size);
            }
        }

        OptionalLong size() {
            long localSize;
            synchronized (sizeObj) {
                localSize = size;
            }
            if (localSize >= 0) {
                return OptionalLong.of(localSize);
            }
            return OptionalLong.empty();
        }

        long awaitSize() throws InterruptedException {
            long localSize;
            synchronized (sizeObj) {
                while ((localSize = size) < 0) {
                    sizeObj.wait();
                }
            }
            return localSize;
        }
    }

    private final Map<Table, State> exports;
    private long nextTicket;

    private final AtomicReference<ExportedTableUpdateMessageHandler> etumHandler;

    public ExportManagerClientBase() {
        this.exports = new HashMap<>();
        this.etumHandler = new AtomicReference<>();
        this.nextTicket = 1L;
    }

    protected abstract void execute(BatchTableRequest batchTableRequest);

    protected abstract void executeRelease(Ticket ticket);

    protected abstract void execute(ExportedTableUpdatesRequest request,
        StreamObserver<ExportedTableUpdateMessage> observer);

    public final boolean startExportedTableUpdateMessages() {
        final ExportedTableUpdateMessageHandler handler = new ExportedTableUpdateMessageHandler();
        if (!etumHandler.compareAndSet(null, handler)) {
            return false;
        }
        execute(ExportedTableUpdatesRequest.newBuilder().build(), handler);
        return true;
    }

    public final boolean stopExportedTableUpdateMessages() {
        final ExportedTableUpdateMessageHandler handler = etumHandler.getAndSet(null);
        if (handler == null) {
            return false;
        }
        handler.clientCallStream
            .cancel("Client requested cancel, stopExportedTableUpdateMessages()", null);
        return true;
    }

    @Override
    public final synchronized Export export(Table table) {
        return export(Collections.singleton(table)).get(0);
    }

    @Override
    public final synchronized List<Export> export(Collection<Table> tables) {
        List<Export> results = new ArrayList<>(tables.size());
        Set<State> newStates = new HashSet<>(tables.size());
        for (Table table : tables) {

            final Optional<State> existing = lookup(table);
            if (existing.isPresent()) {
                final Export newRef = existing.get().newRef();
                results.add(newRef);
                continue;
            }

            final long ticket = nextTicket++;
            final State state = new State(table, ticket);
            final Export newExport = new Export(state);
            newStates.add(state);
            results.add(newExport);
        }
        if (newStates.isEmpty()) {
            return results;
        }

        final BatchTableRequest request = BatchTableRequestBuilder.build(exports, newStates);

        execute(request); // todo: handle async success / failure

        return results;
    }

    private Optional<State> lookup(Table table) {
        return Optional.ofNullable(exports.get(table));
    }

    // this request / response is globally scoped - not specific to a single export...
    private class ExportedTableUpdateMessageHandler
        implements ClientResponseObserver<ExportedTableUpdatesRequest, ExportedTableUpdateMessage> {

        private ClientCallStreamObserver<?> clientCallStream;

        @Override
        public final void beforeStart(
            ClientCallStreamObserver<ExportedTableUpdatesRequest> requestStream) {
            clientCallStream = requestStream;
        }

        @Override
        public final void onNext(ExportedTableUpdateMessage value) {
            final long ticket = byteStringToLong(value.getExportId().getId());
            // todo: maintain Map<Ticket, State>?
            for (State state : exports.values()) {
                if (!state.hasTicket(ticket)) {
                    continue;
                }
                state.notifySizeChange(value.getSize());
                return;
            }
            // todo: log warning, not found?
        }

        @Override
        public final void onError(Throwable t) {
            log.error("Export table update onError", t);
        }

        @Override
        public final void onCompleted() {
            // todo
        }
    }
}
