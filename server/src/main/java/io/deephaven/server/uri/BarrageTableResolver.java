//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.uri;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.client.impl.*;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.server.session.SessionFactoryCreator;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.FieldUri;
import io.deephaven.uri.QueryScopeUri;
import io.deephaven.uri.RemoteUri;
import io.deephaven.uri.StructuredUri.Visitor;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolversInstance;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * The barrage table resolver is able to resolve {@link RemoteUri remote URIs} into {@link Table tables}.
 *
 * <p>
 * For more advanced use cases, see {@link BarrageSession}.
 *
 * @see RemoteUri remote URI format
 */
@Singleton
public final class BarrageTableResolver implements UriResolver {

    public static final Integer MAX_INBOUND_MESSAGE_SIZE =
            Configuration.getInstance().getIntegerWithDefault(
                    "BarrageTableResolver.maxInboundMessageSize",
                    100 * 1024 * 1024); // 100MB default limit

    /**
     * The default options, which uses {@link BarrageSubscriptionOptions#useDeephavenNulls()}.
     */
    public static final BarrageSubscriptionOptions SUB_OPTIONS = BarrageSubscriptionOptions.builder()
            .useDeephavenNulls(true)
            .build();

    public static final BarrageSnapshotOptions SNAP_OPTIONS = BarrageSnapshotOptions.builder()
            .useDeephavenNulls(true)
            .build();

    private static final Set<String> SCHEMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(DeephavenUri.SECURE_SCHEME, DeephavenUri.PLAINTEXT_SCHEME)));

    public static BarrageTableResolver get() {
        return UriResolversInstance.get().find(BarrageTableResolver.class).get();
    }

    private final SessionFactoryCreator sessionFactoryCreator;
    private final Map<DeephavenTarget, BarrageSession> sessions;

    @Inject
    public BarrageTableResolver(SessionFactoryCreator sessionFactoryCreator) {
        this.sessionFactoryCreator = Objects.requireNonNull(sessionFactoryCreator);
        this.sessions = new ConcurrentHashMap<>();
    }

    @Override
    public Set<String> schemes() {
        return SCHEMES;
    }

    @Override
    public boolean isResolvable(URI uri) {
        // Note: we are lying right now when we say this supports remote proxied uri - but we are doing that so callers
        // can get a more specific exception with a link to the issue number.
        return RemoteUri.isWellFormed(uri);
    }

    @Override
    public Table resolve(URI uri) throws InterruptedException {
        try {
            return subscribe(RemoteUri.of(uri)).get();
        } catch (TableHandleException e) {
            throw e.asUnchecked();
        } catch (ExecutionException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    /**
     * Create a full-subscription to the remote URI. Uses {@link #SUB_OPTIONS}.
     *
     * @param remoteUri the remote URI
     * @return the subscribed table
     */
    public Future<Table> subscribe(RemoteUri remoteUri) throws InterruptedException, TableHandleException {
        final DeephavenTarget target = remoteUri.target();
        final TableSpec table = RemoteResolver.of(remoteUri);
        return subscribe(target, table, SUB_OPTIONS);
    }

    /**
     * Create a full-subscription to the {@code table} via the {@code targetUri}. Uses {@link #SUB_OPTIONS}.
     *
     * @param targetUri the target URI
     * @param table the table spec
     * @return the subscribed table
     */
    public Future<Table> subscribe(String targetUri, TableSpec table)
            throws TableHandleException, InterruptedException {
        return subscribe(DeephavenTarget.of(URI.create(targetUri)), table, SUB_OPTIONS);
    }

    /**
     * Create a full-subscription to the {@code table} via the {@code target}.
     *
     * @param target the target
     * @param table the table
     * @param options the options
     * @return the subscribed table
     */
    public Future<Table> subscribe(DeephavenTarget target, TableSpec table, BarrageSubscriptionOptions options)
            throws TableHandleException, InterruptedException {
        final BarrageSession session = session(target);
        final BarrageSubscription sub = session.subscribe(table, options);
        return sub.entireTable();
    }

    /**
     * Create a partial table subscription to the {@code table} via the {@code targetUri}. Uses {@link #SUB_OPTIONS}.
     *
     * @param targetUri the target URI
     * @param table the table spec
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     * @return the subscribed table
     */
    public Future<Table> subscribe(String targetUri, TableSpec table, RowSet viewport, BitSet columns)
            throws TableHandleException, InterruptedException {
        return subscribe(DeephavenTarget.of(URI.create(targetUri)), table, SUB_OPTIONS, viewport, columns, false);
    }

    /**
     * Create a partial table subscription to the {@code table} via the {@code targetUri}. Uses {@link #SUB_OPTIONS}.
     *
     * @param targetUri the target URI
     * @param table the table spec
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     * @param reverseViewport Whether to treat {@code viewport} as offsets from {@link Table#size()} rather than
     *        {@code 0}
     * @return the subscribed table
     */
    public Future<Table> subscribe(String targetUri, TableSpec table, RowSet viewport, BitSet columns,
            boolean reverseViewport)
            throws TableHandleException, InterruptedException {
        return subscribe(DeephavenTarget.of(URI.create(targetUri)), table, SUB_OPTIONS, viewport, columns,
                reverseViewport);
    }

    /**
     * Create a partial table subscription to the {@code table} via the {@code target}.
     *
     * @param target the target
     * @param table the table
     * @param options the options
     * @param viewport the position-space viewport to use for the subscription
     * @param columns the columns to include in the subscription
     * @param reverseViewport Whether to treat {@code viewport} as offsets from {@link Table#size()} rather than
     *        {@code 0}
     * @return the subscribed table
     */
    public Future<Table> subscribe(DeephavenTarget target, TableSpec table, BarrageSubscriptionOptions options,
            RowSet viewport,
            BitSet columns, boolean reverseViewport)
            throws TableHandleException, InterruptedException {
        final BarrageSession session = session(target);
        final BarrageSubscription sub = session.subscribe(table, options);
        return sub.partialTable(viewport, columns, reverseViewport);
    }

    /// NOTE: the following snapshot functions use the `BarrageSubscription` snapshot functions which leverage
    /// `BarrageSubscriptionRequest` and row deltas to remain consistent while the snapshot is being created.
    /// This is the most efficient way (for both server and client) to collect a consistent snapshot of a refreshing
    /// table.

    /**
     * Create a full snapshot of the remote URI. Uses {@link #SUB_OPTIONS}.
     *
     * @param remoteUri the remote URI
     * @return the table to snapshot
     */
    public Future<Table> snapshot(RemoteUri remoteUri) throws InterruptedException, TableHandleException {
        final DeephavenTarget target = remoteUri.target();
        final TableSpec table = RemoteResolver.of(remoteUri);
        return snapshot(target, table, SUB_OPTIONS);
    }

    /**
     * Create a full snapshot to the {@code table} via the {@code targetUri}. Uses {@link #SUB_OPTIONS}.
     *
     * @param targetUri the target URI
     * @param table the table spec
     * @return the table to snapshot
     */
    public Future<Table> snapshot(String targetUri, TableSpec table) throws TableHandleException, InterruptedException {
        return snapshot(DeephavenTarget.of(URI.create(targetUri)), table, SUB_OPTIONS);
    }

    /**
     * Create a full snapshot to the {@code table} via the {@code target}.
     *
     * @param target the target
     * @param table the table
     * @param options the options
     * @return the table to snapshot
     */
    public Future<Table> snapshot(DeephavenTarget target, TableSpec table, BarrageSubscriptionOptions options)
            throws TableHandleException, InterruptedException {
        final BarrageSession session = session(target);
        return session.subscribe(table, options).snapshotEntireTable();
    }

    /**
     * Create a partial table snapshot to the {@code table} via the {@code targetUri}. Uses {@link #SUB_OPTIONS}.
     *
     * @param targetUri the target URI
     * @param table the table spec
     * @param viewport the position-space viewport to use for the snapshot
     * @param columns the columns to include in the snapshot
     * @return the table to snapshot
     */
    public Future<Table> snapshot(String targetUri, TableSpec table, RowSet viewport, BitSet columns)
            throws TableHandleException, InterruptedException {
        return snapshot(DeephavenTarget.of(URI.create(targetUri)), table, SUB_OPTIONS, viewport, columns, false);
    }

    /**
     * Create a partial table snapshot to the {@code table} via the {@code targetUri}. Uses {@link #SUB_OPTIONS}.
     *
     * @param targetUri the target URI
     * @param table the table spec
     * @param viewport the position-space viewport to use for the snapshot
     * @param columns the columns to include in the snapshot
     * @param reverseViewport Whether to treat {@code viewport} as offsets from {@link Table#size()} rather than
     *        {@code 0}
     * @return the table to snapshot
     */
    public Future<Table> snapshot(String targetUri, TableSpec table, RowSet viewport, BitSet columns,
            boolean reverseViewport)
            throws TableHandleException, InterruptedException {
        return snapshot(DeephavenTarget.of(URI.create(targetUri)), table, SUB_OPTIONS, viewport, columns,
                reverseViewport);
    }

    /**
     * Create a partial table snapshot to the {@code table} via the {@code target}.
     *
     * @param target the target
     * @param table the table
     * @param options the options
     * @param viewport the position-space viewport to use for the snapshot
     * @param columns the columns to include in the snapshot
     * @param reverseViewport Whether to treat {@code viewport} as offsets from {@link Table#size()} rather than
     *        {@code 0}
     * @return the table to snapshot
     */
    public Future<Table> snapshot(DeephavenTarget target, TableSpec table, BarrageSubscriptionOptions options,
            RowSet viewport,
            BitSet columns, boolean reverseViewport)
            throws TableHandleException, InterruptedException {
        final BarrageSession session = session(target);
        return session.subscribe(table, options).snapshotPartialTable(viewport, columns, reverseViewport);
    }

    private BarrageSession session(DeephavenTarget target) {
        // TODO (deephaven-core#1482): BarrageTableResolver cleanup
        return sessions.computeIfAbsent(target, this::newSession);
    }

    private BarrageSession newSession(DeephavenTarget target) {
        return newSession(ClientConfig.builder()
                .target(target)
                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
                .build());
    }

    private BarrageSession newSession(ClientConfig config) {
        // TODO(deephaven-core#3421): DH URI / BarrageTableResolver authentication support
        return sessionFactoryCreator.barrageFactory(config).newBarrageSession();
    }

    static class RemoteResolver implements Visitor {

        public static TableSpec of(RemoteUri remoteUri) {
            return remoteUri.uri().walk(new RemoteResolver(remoteUri.target())).out();
        }

        private final DeephavenTarget target;
        private TableSpec out;

        public RemoteResolver(DeephavenTarget target) {
            this.target = Objects.requireNonNull(target);
        }

        public TableSpec out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(FieldUri fieldUri) {
            out = TicketTable.fromApplicationField(target.host(), fieldUri.fieldName());
        }

        @Override
        public void visit(ApplicationUri applicationField) {
            out = TicketTable.fromApplicationField(applicationField.applicationId(), applicationField.fieldName());
        }

        @Override
        public void visit(QueryScopeUri queryScope) {
            out = TicketTable.fromQueryScopeField(queryScope.variableName());
        }

        @Override
        public void visit(RemoteUri remoteUri) {
            // TODO(deephaven-core#1483): Support resolve URI via gRPC / QST
            throw new UnsupportedOperationException(
                    "Proxying not supported yet, see https://github.com/deephaven/deephaven-core/issues/1483");
        }

        @Override
        public void visit(URI customUri) {
            // TODO(deephaven-core#1483): Support resolve URI via gRPC / QST
            throw new UnsupportedOperationException(
                    "Remote custom URIs not supported yet, see https://github.com/deephaven/deephaven-core/issues/1483");
        }
    }
}
