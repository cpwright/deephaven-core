/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.updategraph.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.util.StepUpdater;
import io.deephaven.engine.updategraph.*;
import io.deephaven.engine.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.hotspot.JvmIntrospectionContext;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.SimpleReferenceManager;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import io.deephaven.util.locks.AwareFunctionalLock;
import io.deephaven.util.process.ProcessEnvironment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * TODO: write an explanation.
 */
public abstract class BaseUpdateGraph implements UpdateGraph, LogOutputAppendable {
    public static final String DEFAULT_UPDATE_GRAPH_NAME = "DEFAULT";
    
    /**
     * If the provided update graph is a {@link PeriodicUpdateGraph} then create a PerformanceEntry using the given
     * description. Otherwise, return null.
     *
     * @param updateGraph The update graph to create a performance entry for.
     * @param description The description for the performance entry.
     * @return The performance entry, or null if the update graph is not a {@link PeriodicUpdateGraph}.
     */
    @Nullable
    public static PerformanceEntry createUpdatePerformanceEntry(
            final UpdateGraph updateGraph,
            final String description) {
        if (updateGraph instanceof BaseUpdateGraph) {
            final BaseUpdateGraph aug = (BaseUpdateGraph) updateGraph;
            if (aug.updatePerformanceTracker != null) {
                return aug.updatePerformanceTracker.getEntry(description);
            }
            throw new IllegalStateException("Cannot create a performance entry for a PeriodicUpdateGraph that has "
                    + "not been completely constructed.");
        }
        return null;
    }

    private static final KeyedObjectHashMap<String, UpdateGraph> INSTANCES = new KeyedObjectHashMap<>(
            new KeyedObjectKey.BasicAdapter<>(UpdateGraph::getName));

    private final Logger log;

    /**
     * Update sources that are part of this PeriodicUpdateGraph.
     */
    private final SimpleReferenceManager<Runnable, UpdateSourceRefreshNotification> sources =
            new SimpleReferenceManager<>(UpdateSourceRefreshNotification::new);

    /**
     * Recorder for updates source satisfaction as a phase of notification processing.
     */
    private volatile long sourcesLastSatisfiedStep;

    /**
     * The queue of non-terminal notifications to process.
     */
    final IntrusiveDoublyLinkedQueue<Notification> pendingNormalNotifications =
            new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

    /**
     * The queue of terminal notifications to process.
     */
    final IntrusiveDoublyLinkedQueue<Notification> terminalNotifications =
            new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

    volatile boolean running = true;

    public static final String MINIMUM_CYCLE_DURATION_TO_LOG_MILLIS_PROP =
            "PeriodicUpdateGraph.minimumCycleDurationToLogMillis";
    public static final long DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS = TimeUnit.MILLISECONDS.toNanos(
            Configuration.getInstance().getIntegerWithDefault(MINIMUM_CYCLE_DURATION_TO_LOG_MILLIS_PROP, 25));
    private final long minimumCycleDurationToLogNanos;

    /** when to next flush the performance tracker; initializes to zero to force a flush on start */
    private long nextUpdatePerformanceTrackerFlushTime = 0;

    /**
     * How many cycles we have not logged, but were non-zero.
     */
    long suppressedCycles = 0;
    long suppressedCyclesTotalNanos = 0;
    long suppressedCyclesTotalSafePointTimeMillis = 0;

    /**
     * Accumulated UpdateGraph exclusive lock waits for the current cycle (or previous, if idle).
     */
    private long currentCycleLockWaitTotalNanos = 0;

    public static class AccumulatedCycleStats {
        /**
         * Number of cycles run.
         */
        public int cycles = 0;
        /**
         * Number of cycles run not exceeding their time budget.
         */
        public int cyclesOnBudget = 0;
        /**
         * Accumulated safepoints over all cycles.
         */
        public int safePoints = 0;
        /**
         * Accumulated safepoint time over all cycles.
         */
        public long safePointPauseTimeMillis = 0L;

        public int[] cycleTimesMicros = new int[32];
        public static final int MAX_DOUBLING_LEN = 1024;

        synchronized void accumulate(
                final long targetCycleDurationMillis,
                final long cycleTimeNanos,
                final long safePoints,
                final long safePointPauseTimeMillis) {
            final boolean onBudget = targetCycleDurationMillis * 1000 * 1000 >= cycleTimeNanos;
            if (onBudget) {
                ++cyclesOnBudget;
            }
            this.safePoints += safePoints;
            this.safePointPauseTimeMillis += safePointPauseTimeMillis;
            if (cycles >= cycleTimesMicros.length) {
                final int newLen;
                if (cycleTimesMicros.length < MAX_DOUBLING_LEN) {
                    newLen = cycleTimesMicros.length * 2;
                } else {
                    newLen = cycleTimesMicros.length + MAX_DOUBLING_LEN;
                }
                cycleTimesMicros = Arrays.copyOf(cycleTimesMicros, newLen);
            }
            cycleTimesMicros[cycles] = (int) ((cycleTimeNanos + 500) / 1_000);
            ++cycles;
        }

        public synchronized void take(final AccumulatedCycleStats out) {
            out.cycles = cycles;
            out.cyclesOnBudget = cyclesOnBudget;
            out.safePoints = safePoints;
            out.safePointPauseTimeMillis = safePointPauseTimeMillis;
            if (out.cycleTimesMicros.length < cycleTimesMicros.length) {
                out.cycleTimesMicros = new int[cycleTimesMicros.length];
            }
            System.arraycopy(cycleTimesMicros, 0, out.cycleTimesMicros, 0, cycles);
            cycles = 0;
            cyclesOnBudget = 0;
            safePoints = 0;
            safePointPauseTimeMillis = 0;
        }
    }

    public final AccumulatedCycleStats accumulatedCycleStats = new AccumulatedCycleStats();

    /**
     * Abstracts away the processing of non-terminal notifications.
     */
    NotificationProcessor notificationProcessor;

    /**
     * Facilitate GC Introspection during refresh cycles.
     */
    private final JvmIntrospectionContext jvmIntrospectionContext;

    /**
     * The {@link LivenessScope} that should be on top of the {@link LivenessScopeStack} for all run and notification
     * processing. Only non-null while some thread is in {@link #doRefresh(Runnable)}.
     */
    volatile LivenessScope refreshScope;

    /**
     * Is this one of the threads engaged in notification processing? (Either the solitary run thread, or one of the
     * pooled threads it uses in some configurations)
     */
    final ThreadLocal<Boolean> isUpdateThread = ThreadLocal.withInitial(() -> false);

    private final ThreadLocal<Boolean> serialTableOperationsSafe = ThreadLocal.withInitial(() -> false);

    final LogicalClockImpl logicalClock = new LogicalClockImpl();

    /**
     * Encapsulates locking support.
     */
    private final UpdateGraphLock lock;

    /**
     * When PeriodicUpdateGraph.printDependencyInformation is set to true, the PeriodicUpdateGraph will print debug
     * information for each notification that has dependency information; as well as which notifications have been
     * completed and are outstanding.
     */
    private final boolean printDependencyInformation =
            Configuration.getInstance().getBooleanWithDefault("PeriodicUpdateGraph.printDependencyInformation", false);

    private final String name;

    final UpdatePerformanceTracker updatePerformanceTracker;



    public BaseUpdateGraph(
            final String name,
            final boolean allowUnitTestMode,
            final Logger log,
            long minimumCycleDurationToLogNanos) {
        this.name = name;
        this.log = log;
        this.minimumCycleDurationToLogNanos = minimumCycleDurationToLogNanos;
        notificationProcessor = PoisonedNotificationProcessor.INSTANCE;
        jvmIntrospectionContext = new JvmIntrospectionContext();
        lock = UpdateGraphLock.create(this, allowUnitTestMode);
        updatePerformanceTracker = new UpdatePerformanceTracker(this);
    }

    public String getName() {
        return name;
    }

    public UpdateGraph getUpdateGraph() {
        return this;
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public LogicalClock clock() {
        return logicalClock;
    }
    // region Accessors for the shared and exclusive locks

    /**
     * <p>
     * Get the shared lock for this {@link PeriodicUpdateGraph}.
     * <p>
     * Using this lock will prevent run processing from proceeding concurrently, but will allow other read-only
     * processing to proceed.
     * <p>
     * The shared lock implementation is expected to support reentrance.
     * <p>
     * This lock does <em>not</em> support {@link java.util.concurrent.locks.Lock#newCondition()}. Use the exclusive
     * lock if you need to wait on events that are driven by run processing.
     *
     * @return The shared lock for this {@link PeriodicUpdateGraph}
     */
    public AwareFunctionalLock sharedLock() {
        return lock.sharedLock();
    }

    /**
     * <p>
     * Get the exclusive lock for this {@link PeriodicUpdateGraph}.
     * <p>
     * Using this lock will prevent run or read-only processing from proceeding concurrently.
     * <p>
     * The exclusive lock implementation is expected to support reentrance.
     * <p>
     * Note that using the exclusive lock while the shared lock is held by the current thread will result in exceptions,
     * as lock upgrade is not supported.
     * <p>
     * This lock does support {@link java.util.concurrent.locks.Lock#newCondition()}.
     *
     * @return The exclusive lock for this {@link PeriodicUpdateGraph}
     */
    public AwareFunctionalLock exclusiveLock() {
        return lock.exclusiveLock();
    }

    // endregion Accessors for the shared and exclusive locks

    /**
     * Test if this thread is part of our run thread executor service.
     *
     * @return whether this is one of our run threads.
     */
    @Override
    public boolean currentThreadProcessesUpdates() {
        return isUpdateThread.get();
    }

    @Override
    public boolean serialTableOperationsSafe() {
        return serialTableOperationsSafe.get();
    }

    @Override
    public boolean setSerialTableOperationsSafe(final boolean newValue) {
        final boolean old = serialTableOperationsSafe.get();
        serialTableOperationsSafe.set(newValue);
        return old;
    }


    /**
     * Add a table to the list of tables to run and mark it as {@link DynamicNode#setRefreshing(boolean) refreshing} if
     * it was a {@link DynamicNode}.
     *
     * @param updateSource The table to be added to the run list
     */
    @Override
    public void addSource(@NotNull final Runnable updateSource) {
        if (!running) {
            throw new IllegalStateException("PeriodicUpdateGraph is no longer running");
        }

        if (updateSource instanceof DynamicNode) {
            ((DynamicNode) updateSource).setRefreshing(true);
        }

        // if we are in unit test mode we never want to start the UpdateGraph
        sources.add(updateSource);
    }


    @Override
    public void removeSource(@NotNull final Runnable updateSource) {
        sources.remove(updateSource);
    }

    /**
     * Remove a collection of sources from the list of refreshing sources.
     *
     * @implNote This will <i>not</i> set the sources as {@link DynamicNode#setRefreshing(boolean) non-refreshing}.
     * @param sourcesToRemove The sources to remove from the list of refreshing sources
     */
    public void removeSources(final Collection<Runnable> sourcesToRemove) {
        sources.removeAll(sourcesToRemove);
    }

    /**
     * Return the number of valid sources.
     *
     * @return the number of valid sources
     */
    public int sourceCount() {
        return sources.size();
    }

    /**
     * Enqueue a notification to be flushed according to its priority. Non-terminal notifications should only be
     * enqueued during the updating phase of a cycle. That is, they should be enqueued from an update source or
     * subsequent notification delivery.
     *
     * @param notification The notification to enqueue
     * @see NotificationQueue.Notification#isTerminal()
     * @see LogicalClock.State
     */
    @Override
    public void addNotification(@NotNull final Notification notification) {
        if (notification.isTerminal()) {
            synchronized (terminalNotifications) {
                terminalNotifications.offer(notification);
            }
        } else {
            logDependencies().append(Thread.currentThread().getName()).append(": Adding notification ")
                    .append(notification).endl();
            synchronized (pendingNormalNotifications) {
                Assert.eq(logicalClock.currentState(), "logicalClock.currentState()",
                        LogicalClock.State.Updating, "LogicalClock.State.Updating");
                pendingNormalNotifications.offer(notification);
            }
            notificationProcessor.onNotificationAdded();
        }
    }

    @Override
    public boolean maybeAddNotification(@NotNull final Notification notification, final long deliveryStep) {
        if (notification.isTerminal()) {
            throw new IllegalArgumentException("Notification must not be terminal");
        }
        logDependencies().append(Thread.currentThread().getName()).append(": Adding notification ").append(notification)
                .append(" if step is ").append(deliveryStep).endl();
        final boolean added;
        synchronized (pendingNormalNotifications) {
            // Note that the clock is advanced to idle under the pendingNormalNotifications lock, after which point no
            // further normal notifications will be processed on this cycle.
            final long logicalClockValue = logicalClock.currentValue();
            if (LogicalClock.getState(logicalClockValue) == LogicalClock.State.Updating
                    && LogicalClock.getStep(logicalClockValue) == deliveryStep) {
                pendingNormalNotifications.offer(notification);
                added = true;
            } else {
                added = false;
            }
        }
        if (added) {
            notificationProcessor.onNotificationAdded();
        }
        return added;
    }

    @Override
    public boolean satisfied(final long step) {
        StepUpdater.checkForOlderStep(step, sourcesLastSatisfiedStep);
        return sourcesLastSatisfiedStep == step;
    }

    /**
     * Enqueue a collection of notifications to be flushed.
     *
     * @param notifications The notification to enqueue
     *
     * @see #addNotification(Notification)
     */
    @Override
    public void addNotifications(@NotNull final Collection<? extends Notification> notifications) {
        synchronized (pendingNormalNotifications) {
            synchronized (terminalNotifications) {
                notifications.forEach(this::addNotification);
            }
        }
    }

    /**
     * @return Whether this UpdateGraph has a mechanism that supports refreshing
     */
    @Override
    public boolean supportsRefreshing() {
        return true;
    }

    @TestUseOnly
    void resetForUnitTests(final boolean after, final List<String> errors) {
        sources.clear();
        notificationProcessor.shutdown();
        synchronized (pendingNormalNotifications) {
            pendingNormalNotifications.clear();
        }
        isUpdateThread.remove();
        synchronized (terminalNotifications) {
            terminalNotifications.clear();
        }
        logicalClock.resetForUnitTests();
        sourcesLastSatisfiedStep = logicalClock.currentStep();

        refreshScope = null;
        if (after) {
            LivenessManager stackTop;
            while ((stackTop = LivenessScopeStack.peek()) instanceof LivenessScope) {
                LivenessScopeStack.pop((LivenessScope) stackTop);
            }
            CleanupReferenceProcessorInstance.resetAllForUnitTests();
        }

        ensureUnlocked("unit test reset thread", errors);
    }

    @TestUseOnly
    void resetLock() {
        lock.reset();
    }

    /**
     * Flush all non-terminal notifications, complete the logical clock update cycle, then flush all terminal
     * notifications.
     *
     * @param check that update sources have not yet been satisfied (false in unit test mode)
     */
    void flushNotificationsAndCompleteCycle(boolean check) {
        // We cannot proceed with normal notifications, nor are we satisfied, until all update source refresh
        // notifications have been processed. Note that non-update source notifications that require dependency
        // satisfaction are delivered first to the pendingNormalNotifications queue, and hence will not be processed
        // until we advance to the flush* methods.
        // TODO: If and when we properly integrate update sources into the dependency tracking system, we can
        // discontinue this distinct phase, along with the requirement to treat the UpdateGraph itself as a Dependency.
        // Until then, we must delay the beginning of "normal" notification processing until all update sources are
        // done. See IDS-8039.
        notificationProcessor.doAllWork();

        updateSourcesLastSatisfiedStep(check);

        flushNormalNotificationsAndCompleteCycle();
        flushTerminalNotifications();
        synchronized (pendingNormalNotifications) {
            Assert.assertion(pendingNormalNotifications.isEmpty(), "pendingNormalNotifications.isEmpty()");
        }
    }

    void updateSourcesLastSatisfiedStep(boolean check) {
        if (check && sourcesLastSatisfiedStep >= logicalClock.currentStep()) {
            throw new IllegalStateException("Already marked sources as satisfied!");
        }
        sourcesLastSatisfiedStep = logicalClock.currentStep();
    }

    /**
     * Flush all non-terminal {@link Notification notifications} from the queue.
     */
    private void flushNormalNotificationsAndCompleteCycle() {
        final IntrusiveDoublyLinkedQueue<Notification> pendingToEvaluate =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
        while (true) {
            final int outstandingCountAtStart = notificationProcessor.outstandingNotificationsCount();
            notificationProcessor.beforeNotificationsDrained();
            synchronized (pendingNormalNotifications) {
                pendingToEvaluate.transferAfterTailFrom(pendingNormalNotifications);
                if (outstandingCountAtStart == 0 && pendingToEvaluate.isEmpty()) {
                    // We complete the cycle here before releasing the lock on pendingNotifications, so that
                    // maybeAddNotification can detect scenarios where the notification cannot be delivered on the
                    // desired step.
                    logicalClock.completeUpdateCycle();
                    break;
                }
            }
            logDependencies().append(Thread.currentThread().getName())
                    .append(": Notification queue size=").append(pendingToEvaluate.size())
                    .append(", outstanding=").append(outstandingCountAtStart)
                    .endl();

            boolean nothingBecameSatisfied = true;
            for (final Iterator<Notification> it = pendingToEvaluate.iterator(); it.hasNext();) {
                final Notification notification = it.next();

                Assert.eqFalse(notification.isTerminal(), "notification.isTerminal()");
                Assert.eqFalse(notification.mustExecuteWithUpdateGraphLock(),
                        "notification.mustExecuteWithUpdateGraphLock()");

                final boolean satisfied = notification.canExecute(sourcesLastSatisfiedStep);
                if (satisfied) {
                    nothingBecameSatisfied = false;
                    it.remove();
                    logDependencies().append(Thread.currentThread().getName())
                            .append(": Submitting to notification processor ").append(notification).endl();
                    notificationProcessor.submit(notification);
                } else {
                    logDependencies().append(Thread.currentThread().getName()).append(": Unmet dependencies for ")
                            .append(notification).endl();
                }
            }
            if (outstandingCountAtStart == 0 && nothingBecameSatisfied) {
                throw new IllegalStateException(
                        "No outstanding notifications, yet the notification queue is not empty!");
            }
            if (notificationProcessor.outstandingNotificationsCount() > 0) {
                notificationProcessor.doWork();
            }
        }
        synchronized (pendingNormalNotifications) {
            Assert.eqZero(pendingNormalNotifications.size() + pendingToEvaluate.size(),
                    "pendingNormalNotifications.size() + pendingToEvaluate.size()");
        }
    }

    /**
     * Flush all {@link Notification#isTerminal() terminal} {@link Notification notifications} from the queue.
     *
     * @implNote Any notification that may have been queued while the clock's state is Updating must be invoked during
     *           this cycle's Idle phase.
     */
    private void flushTerminalNotifications() {
        synchronized (terminalNotifications) {
            for (final Iterator<Notification> it = terminalNotifications.iterator(); it.hasNext();) {
                final Notification notification = it.next();
                Assert.assertion(notification.isTerminal(), "notification.isTerminal()");

                if (!notification.mustExecuteWithUpdateGraphLock()) {
                    it.remove();
                    // for the single threaded queue case; this enqueues the notification;
                    // for the executor service case, this causes the notification to be kicked off
                    notificationProcessor.submit(notification);
                }
            }
        }

        // run the notifications that must be run on this thread
        while (true) {
            final Notification notificationForThisThread;
            synchronized (terminalNotifications) {
                notificationForThisThread = terminalNotifications.poll();
            }
            if (notificationForThisThread == null) {
                break;
            }
            runNotification(notificationForThisThread);
        }

        // We can not proceed until all of the terminal notifications have executed.
        notificationProcessor.doAllWork();
    }

    /**
     * Abstract away the details of satisfied notification processing.
     */
    interface NotificationProcessor {

        /**
         * Submit a satisfied notification for processing.
         *
         * @param notification The notification
         */
        void submit(@NotNull NotificationQueue.Notification notification);

        /**
         * Submit a queue of satisfied notification for processing.
         *
         * @param notifications The queue of notifications to
         *        {@link IntrusiveDoublyLinkedQueue#transferAfterTailFrom(IntrusiveDoublyLinkedQueue) transfer} from.
         *        Will become empty as a result of successful completion
         */
        void submitAll(@NotNull IntrusiveDoublyLinkedQueue<Notification> notifications);

        /**
         * Query the number of outstanding notifications submitted to this processor.
         *
         * @return The number of outstanding notifications
         */
        int outstandingNotificationsCount();

        /**
         * <p>
         * Do work (or in the multi-threaded case, wait for some work to have happened).
         * <p>
         * Caller must know that work is outstanding.
         */
        void doWork();

        /**
         * Do all outstanding work.
         */
        void doAllWork();

        /**
         * Shutdown this notification processor (for unit tests).
         */
        void shutdown();

        /**
         * Called after a pending notification is added.
         */
        void onNotificationAdded();

        /**
         * Called before pending notifications are drained.
         */
        void beforeNotificationsDrained();
    }

    void runNotification(@NotNull final Notification notification) {
        logDependencies().append(Thread.currentThread().getName()).append(": Executing ").append(notification).endl();

        final LivenessScope scope;
        final boolean releaseScopeOnClose;
        if (notification.isTerminal()) {
            // Terminal notifications can't create new notifications, so they have no need to participate in a shared
            // run scope.
            scope = new LivenessScope();
            releaseScopeOnClose = true;
        } else {
            // Non-terminal notifications must use a shared run scope.
            Assert.neqNull(refreshScope, "refreshScope");
            scope = refreshScope == LivenessScopeStack.peek() ? null : refreshScope;
            releaseScopeOnClose = false;
        }

        try (final SafeCloseable ignored = scope == null ? null : LivenessScopeStack.open(scope, releaseScopeOnClose)) {
            notification.run();
            logDependencies().append(Thread.currentThread().getName()).append(": Completed ").append(notification)
                    .endl();
        } catch (final Exception e) {
            log.error().append(Thread.currentThread().getName())
                    .append(": Exception while executing PeriodicUpdateGraph notification: ").append(notification)
                    .append(": ").append(e).endl();
            ProcessEnvironment.getGlobalFatalErrorReporter()
                    .report("Exception while processing PeriodicUpdateGraph notification", e);
        }
    }

    class QueueNotificationProcessor implements NotificationProcessor {

        final IntrusiveDoublyLinkedQueue<Notification> satisfiedNotifications =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

        @Override
        public void submit(@NotNull final Notification notification) {
            satisfiedNotifications.offer(notification);
        }

        @Override
        public void submitAll(@NotNull IntrusiveDoublyLinkedQueue<Notification> notifications) {
            satisfiedNotifications.transferAfterTailFrom(notifications);
        }

        @Override
        public int outstandingNotificationsCount() {
            return satisfiedNotifications.size();
        }

        @Override
        public void doWork() {
            Notification satisfiedNotification;
            while ((satisfiedNotification = satisfiedNotifications.poll()) != null) {
                runNotification(satisfiedNotification);
            }
        }

        @Override
        public void doAllWork() {
            doWork();
        }

        @Override
        public void shutdown() {
            satisfiedNotifications.clear();
        }

        @Override
        public void onNotificationAdded() {}

        @Override
        public void beforeNotificationsDrained() {}
    }


    static LogEntry appendAsMillisFromNanos(final LogEntry entry, final long nanos) {
        if (nanos > 0) {
            return entry.appendDouble(nanos / 1_000_000.0, 3);
        }
        return entry.append(0);
    }

    /**
     * Iterate over all monitored tables and run them.
     */
    void refreshTablesAndFlushNotifications() {
        final long startTimeNanos = System.nanoTime();

        currentCycleLockWaitTotalNanos = 0;
        jvmIntrospectionContext.startSample();

        if (sources.isEmpty()) {
            exclusiveLock().doLocked(this::flushTerminalNotifications);
        } else {
            refreshAllTables();

        }

        jvmIntrospectionContext.endSample();
        final long cycleTimeNanos = System.nanoTime() - startTimeNanos;
        computeStatsAndLogCycle(cycleTimeNanos);
    }

    private void computeStatsAndLogCycle(final long cycleTimeNanos) {
        final long safePointPauseTimeMillis = jvmIntrospectionContext.deltaSafePointPausesTimeMillis();
        accumulatedCycleStats.accumulate(
                getTargetCycleDurationMillis(),
                cycleTimeNanos,
                jvmIntrospectionContext.deltaSafePointPausesCount(),
                safePointPauseTimeMillis);
        if (cycleTimeNanos >= minimumCycleDurationToLogNanos) {
            if (suppressedCycles > 0) {
                logSuppressedCycles();
            }
            final double cycleTimeMillis = cycleTimeNanos / 1_000_000.0;
            LogEntry entry = log.info().append(getName())
                    .append(": Update Graph Processor cycleTime=").appendDouble(cycleTimeMillis, 3);
            if (jvmIntrospectionContext.hasSafePointData()) {
                final long safePointSyncTimeMillis = jvmIntrospectionContext.deltaSafePointSyncTimeMillis();
                entry = entry
                        .append("ms, safePointTime=")
                        .append(safePointPauseTimeMillis)
                        .append("ms, safePointTimePct=");
                if (safePointPauseTimeMillis > 0 && cycleTimeMillis > 0.0) {
                    final double safePointTimePct = 100.0 * safePointPauseTimeMillis / cycleTimeMillis;
                    entry = entry.appendDouble(safePointTimePct, 2);
                } else {
                    entry = entry.append("0");
                }
                entry = entry.append("%, safePointSyncTime=").append(safePointSyncTimeMillis);
            }
            entry = entry.append("ms, lockWaitTime=");
            entry = appendAsMillisFromNanos(entry, currentCycleLockWaitTotalNanos);
            entry = logCycleExtra(entry);
            entry.append("ms").endl();
            return;
        }
        if (cycleTimeNanos > 0) {
            ++suppressedCycles;
            suppressedCyclesTotalNanos += cycleTimeNanos;
            suppressedCyclesTotalSafePointTimeMillis += safePointPauseTimeMillis;
            if (suppressedCyclesTotalNanos >= minimumCycleDurationToLogNanos) {
                logSuppressedCycles();
            }
        }
    }

    protected abstract LogEntry logCycleExtra(LogEntry entry);

    /**
     * Get the target duration of an update cycle, including the updating phase and the idle phase. This is also the
     * target interval between the start of one cycle and the start of the next.
     *
     * @return The target cycle duration
     */
    public long getTargetCycleDurationMillis() {
        return 0;
    }

    private void logSuppressedCycles() {
        LogEntry entry = log.info()
                .append("Minimal Update Graph Processor cycle times: ")
                .appendDouble((double) (suppressedCyclesTotalNanos) / 1_000_000.0, 3).append("ms / ")
                .append(suppressedCycles).append(" cycles = ")
                .appendDouble(
                        (double) suppressedCyclesTotalNanos / (double) suppressedCycles / 1_000_000.0, 3)
                .append("ms/cycle average)");
        if (jvmIntrospectionContext.hasSafePointData()) {
            entry = entry
                    .append(", safePointTime=")
                    .append(suppressedCyclesTotalSafePointTimeMillis)
                    .append("ms");
        }
        entry.endl();
        suppressedCycles = suppressedCyclesTotalNanos = 0;
        suppressedCyclesTotalSafePointTimeMillis = 0;
    }


    void checkUpdatePerformanceFlush(long now, long checkTime) {
        if (checkTime >= nextUpdatePerformanceTrackerFlushTime) {
            nextUpdatePerformanceTrackerFlushTime = now + UpdatePerformanceTracker.REPORT_INTERVAL_MILLIS;
            try {
                try (final SafeCloseable ignored = openContextForUpdatePerformanceTracker()) {
                    updatePerformanceTracker.flush();
                }
            } catch (Exception err) {
                log.error().append("Error flushing UpdatePerformanceTracker: ").append(err).endl();
            }
        }
    }

    /**
     * In unit tests it can be convenient to force the update performance tracker to flush, without waiting for the
     * complete REPORT_INTERVAL_MILLIS to elapse.
     */
    @TestUseOnly
    public void resetNextFlushTime() {
        nextUpdatePerformanceTrackerFlushTime = 0;
    }

    /**
     * The UpdatePerformanceTracker requires a common update graph for all operations, to avoid spanning update graphs.
     * @return a context suitable for operating on the updatePerformanceTracker.
     */
    static SafeCloseable openContextForUpdatePerformanceTracker() {
        return ExecutionContext.getContext().withUpdateGraph(BaseUpdateGraph.getInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)).open();
    }

    /**
     * Refresh all the update sources within an {@link LogicalClock update cycle} after the UpdateGraph has been locked.
     * At the end of the updates all {@link Notification notifications} will be flushed.
     */
    void refreshAllTables() {
        doRefresh(() -> sources.forEach((final UpdateSourceRefreshNotification updateSourceNotification,
                final Runnable unused) -> notificationProcessor.submit(updateSourceNotification)));
    }

    /**
     * Perform a run cycle, using {@code refreshFunction} to ensure the desired update sources are refreshed at the
     * start.
     *
     * @param refreshFunction Function to submit one or more {@link UpdateSourceRefreshNotification update source
     *        refresh notifications} to the {@link NotificationProcessor notification processor} or run them directly.
     */
    private void doRefresh(@NotNull final Runnable refreshFunction) {
        final long lockStartTimeNanos = System.nanoTime();
        exclusiveLock().doLocked(() -> {
            currentCycleLockWaitTotalNanos += System.nanoTime() - lockStartTimeNanos;
            if (!preRefresh()) {
                return;
            }
            synchronized (pendingNormalNotifications) {
                Assert.eqZero(pendingNormalNotifications.size(), "pendingNormalNotifications.size()");
            }
            Assert.eqNull(refreshScope, "refreshScope");
            refreshScope = new LivenessScope();
            final long updatingCycleValue = logicalClock.startUpdateCycle();
            logDependencies().append("Beginning PeriodicUpdateGraph cycle step=")
                    .append(logicalClock.currentStep()).endl();
            try (final SafeCloseable ignored = LivenessScopeStack.open(refreshScope, true)) {
                refreshFunction.run();
                flushNotificationsAndCompleteCycle(true);
            } finally {
                logicalClock.ensureUpdateCycleCompleted(updatingCycleValue);
                refreshScope = null;
            }
            logDependencies().append("Completed PeriodicUpdateGraph cycle step=")
                    .append(logicalClock.currentStep()).endl();
        });
    }

    /**
     * Pre-refresh is called after the lock is taken, but before refresh processing is initiated.
     *
     * <p>If this update graph is no longer running, returns false to prevent further processing.</p>
     *
     * @return true if the refresh cycle should be processed, if false, the refresh cycle is not executed.
     */
    boolean preRefresh() {
        return running;
    }

    /**
     * Re-usable class for adapting update sources to {@link Notification}s.
     */
    static final class UpdateSourceRefreshNotification extends AbstractNotification
            implements SimpleReference<Runnable> {

        private final WeakReference<Runnable> updateSourceRef;

        private UpdateSourceRefreshNotification(@NotNull final Runnable updateSource) {
            super(false);
            updateSourceRef = new WeakReference<>(updateSource);
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append("UpdateSourceRefreshNotification{").append(System.identityHashCode(this))
                    .append(", for UpdateSource{").append(System.identityHashCode(get())).append("}}");
        }

        @Override
        public boolean canExecute(final long step) {
            return true;
        }

        @Override
        public void run() {
            final Runnable updateSource = updateSourceRef.get();
            if (updateSource == null) {
                return;
            }
            updateSource.run();
        }

        @Override
        public Runnable get() {
            // NB: Arguably we should make get() and clear() synchronized.
            return updateSourceRef.get();
        }

        @Override
        public void clear() {
            updateSourceRef.clear();
        }
    }

    public void takeAccumulatedCycleStats(AccumulatedCycleStats updateGraphAccumCycleStats) {
        accumulatedCycleStats.take(updateGraphAccumCycleStats);
    }

    public LogEntry logDependencies() {
        if (printDependencyInformation) {
            return log.info();
        } else {
            return LogEntry.NULL;
        }
    }

    @TestUseOnly
    void ensureUnlocked(@NotNull final String callerDescription, @Nullable final List<String> errors) {
        if (exclusiveLock().isHeldByCurrentThread()) {
            if (errors != null) {
                errors.add(callerDescription + ": UpdateGraph exclusive lock is still held");
            }
            while (exclusiveLock().isHeldByCurrentThread()) {
                exclusiveLock().unlock();
            }
        }
        if (sharedLock().isHeldByCurrentThread()) {
            if (errors != null) {
                errors.add(callerDescription + ": UpdateGraph shared lock is still held");
            }
            while (sharedLock().isHeldByCurrentThread()) {
                sharedLock().unlock();
            }
        }
    }

    public static UpdateGraph getInstance(final String name) {
        return INSTANCES.get(name);
    }

    /**
     * Clear a named instance of an update graph.
     *
     * <p>In addition to removing the update graph from the instances, an attempt is made to shut it down.</p>
     *
     * @param name the name of the update graph to clear
     * @return true if the update graph was found
     */
    public static boolean clearInstance(final String name) {
        final UpdateGraph graph;
        synchronized (INSTANCES) {
            graph = INSTANCES.removeKey(name);
            if (graph == null) {
                return false;
            }
        }
        graph.stop();
        return true;
    }

    /**
     * Inserts the given UpdateGraph into the INSTANCES array. It is an error to do so if instance already exists
     * with the name provided to this builder.
     *
     * @param updateGraph the update graph to insert into INSTANCES
     *
     * @throws IllegalStateException if a PeriodicUpdateGraph with the provided name already exists
     */
    public static void insertInstance(UpdateGraph updateGraph) {
        final String name = updateGraph.getName();
        synchronized (INSTANCES) {
            if (INSTANCES.containsKey(name)) {
                throw new IllegalStateException(
                        String.format("UpdateGraph with name %s already exists", name));
            }
            INSTANCES.put(name, updateGraph);
        }
    }

    /**
     * Returns an existing PeriodicUpdateGraph with the name provided to this Builder, if one exists, else returns a
     * new PeriodicUpdateGraph.
     *
     * @return the PeriodicUpdateGraph
     */
    public static UpdateGraph existingOrBuild(final String name, Function<String, UpdateGraph> construct) {
        return INSTANCES.putIfAbsent(name, construct::apply);
    }
}