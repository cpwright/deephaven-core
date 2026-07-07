//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.cache.RetentionCache;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.table.impl.util.AsyncErrorLogger;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.util.systemicmarking.SystemicObject;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.Utils;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.io.IOException;

/**
 * This class is used for ShiftAwareListeners that represent "leaf" nodes in the update propagation tree.
 *
 * It provides an optional retention cache, to prevent listeners from being garbage collected.
 *
 * For creating internally ticking table nodes, instead use {@link BaseTable.ListenerImpl}
 */
public abstract class InstrumentedTableUpdateListenerAdapter extends InstrumentedTableUpdateListener
        implements SystemicObject<InstrumentedTableUpdateListenerAdapter> {

    private static final RetentionCache<InstrumentedTableUpdateListenerAdapter> RETENTION_CACHE =
            new RetentionCache<>();

    private final boolean retain;

    /**
     * Whether this listener is systemically important. As a leaf of the update propagation tree there is no downstream
     * object to consult, so we capture the systemic state of the thread that created us (see
     * {@link SystemicObjectTracker}).
     */
    private volatile boolean systemic = SystemicObjectTracker.isSystemicThread();

    /**
     * The creating thread's {@link AuthContext}, reinstalled around {@link #onUpdate(TableUpdate)} execution; null if
     * this listener does not capture the auth context.
     */
    @Nullable
    private final AuthContext authContext;

    @ReferentialIntegrity
    protected final Table source;

    /**
     * Create an instrumented listener for source. No description is provided. The {@link AuthContext} from the current
     * {@link ExecutionContext} is captured and reinstalled around {@link #onUpdate(TableUpdate)} execution.
     *
     * @param source The source table this listener will subscribe to - needed for preserving referential integrity
     * @param retain Whether a hard reference to this listener should be maintained to prevent it from being collected.
     *        In most scenarios, it's better to specify {@code false} and keep a reference in the calling code.
     */
    public InstrumentedTableUpdateListenerAdapter(@NotNull final Table source, final boolean retain) {
        this(null, source, retain);
    }

    /**
     * Create an instrumented listener for source. The {@link AuthContext} from the current {@link ExecutionContext} is
     * captured and reinstalled around {@link #onUpdate(TableUpdate)} execution.
     *
     * @param description A description for the UpdatePerformanceTracker to append to its entry description
     * @param source The source table this listener will subscribe to - needed for preserving referential integrity
     * @param retain Whether a hard reference to this listener should be maintained to prevent it from being collected.
     *        In most scenarios, it's better to specify {@code false} and keep a reference in the calling code.
     */
    public InstrumentedTableUpdateListenerAdapter(@Nullable final String description, @NotNull final Table source,
            final boolean retain) {
        this(description, source, retain, true);
    }

    /**
     * @param description A description for the UpdatePerformanceTracker to append to its entry description
     * @param source The source table this listener will subscribe to - needed for preserving referential integrity
     * @param retain Whether a hard reference to this listener should be maintained to prevent it from being collected.
     *        In most scenarios, it's better to specify {@code false} and keep a reference in the calling code.
     * @param captureAuthContext Whether to capture the {@link AuthContext} from the current {@link ExecutionContext}
     *        and reinstall it around {@link #onUpdate(TableUpdate)} execution. User listeners should capture the auth
     *        context so their callbacks execute with the creator's permissions; listeners that execute no user code (or
     *        restore an entire execution context of their own) may pass {@code false} to avoid the context switch.
     */
    public InstrumentedTableUpdateListenerAdapter(@Nullable final String description, @NotNull final Table source,
            final boolean retain, final boolean captureAuthContext) {
        super(description, false, () -> {
            if (source instanceof HasParentPerformanceIds) {
                return ((HasParentPerformanceIds) source).parentPerformanceEntryIds().toArray();
            }
            return null;
        });
        this.authContext = captureAuthContext ? ExecutionContext.getContext().getAuthContext() : null;
        this.source = Require.neqNull(source, "source");
        if (this.retain = retain) {
            RETENTION_CACHE.retain(this);
            if (Liveness.DEBUG_MODE_ENABLED) {
                Liveness.log.info().append("LivenessDebug: InstrumentedTableUpdateListenerAdapter ")
                        .append(Utils.REFERENT_FORMATTER, this)
                        .append(" created with retention enabled").endl();
            }
        }
        manage(source);
    }

    @Override
    public Notification getNotification(final TableUpdate update) {
        if (authContext == null) {
            return super.getNotification(update);
        }
        return new Notification(update) {
            @Override
            public void run() {
                try (final SafeCloseable ignored =
                        ExecutionContext.getContext().withAuthContext(authContext).open()) {
                    super.run();
                }
            }
        };
    }

    @Override
    public abstract void onUpdate(TableUpdate upstream);

    /**
     * Called when the source table produces an error
     *
     * @param originalException the original throwable that caused this error
     * @param sourceEntry the performance tracker entry that was active when the error occurred
     */
    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        AsyncErrorLogger.log(DateTimeUtils.nowMillisResolution(), sourceEntry, sourceEntry, originalException);

        // Secondary notification to client error monitoring, only for systemic listeners
        if (SystemicObjectTracker.isSystemic(this)) {
            try {
                AsyncClientErrorNotifier.reportError(originalException);
            } catch (IOException e) {
                final UncheckedTableException uncheckedTableException =
                        new UncheckedTableException(
                                "Exception while delivering async client error notification for "
                                        + sourceEntry.toString(),
                                originalException);
                uncheckedTableException.addSuppressed(e);
                throw uncheckedTableException;
            }
        }
    }

    @Override
    public boolean isSystemicObject() {
        return systemic;
    }

    @Override
    public InstrumentedTableUpdateListenerAdapter markSystemic() {
        systemic = true;
        return this;
    }

    @Override
    public boolean canExecute(final long step) {
        return source.satisfied(step);
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    protected void destroy() {
        super.destroy();
        source.removeUpdateListener(this);
        if (retain) {
            RETENTION_CACHE.forget(this);
        }
    }
}
