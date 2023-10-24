package io.deephaven.engine.updategraph.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.base.SleepUtil;
import io.deephaven.configuration.DataDir;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.sources.LongSingleValueSource;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import junit.framework.TestCase;
import org.junit.*;

import java.nio.file.Path;
import java.util.Collections;

import static io.deephaven.engine.util.TableTools.*;

public class TestEventDrivenUpdateGraph {
    EventDrivenUpdateGraph defaultUpdateGraph;

    @Before
    public void before() {
        // the default update is necessary for the update performance tracker
        BaseUpdateGraph.clearInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME);
        BaseUpdateGraph.clearInstance("TestEDUG");
        BaseUpdateGraph.clearInstance("TestEDUG1");
        BaseUpdateGraph.clearInstance("TestEDUG2");

        defaultUpdateGraph = new EventDrivenUpdateGraph.Builder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME).build();
    }

    @After
    public void after() {
        BaseUpdateGraph.clearInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME);
    }


    final static class SourceThatRefreshes extends QueryTable implements Runnable {
        public SourceThatRefreshes(UpdateGraph updateGraph) {
            super(RowSetFactory.empty().toTracking(), Collections.emptyMap());
            setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
            updateGraph.addSource(this);
        }

        @Override
        public void run() {
            final RowSet added;
            if (getRowSet().isEmpty()) {
                added = RowSetFactory.fromKeys(0);
            } else {
                added = RowSetFactory.fromKeys(getRowSet().lastRowKey() + 1);
            }
            getRowSet().writableCast().insert(added);
            notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }
    }

    final static class SourceThatModifiesItself extends QueryTable implements Runnable {
        final LongSingleValueSource svcs;

        public SourceThatModifiesItself(UpdateGraph updateGraph) {
            super(RowSetFactory.fromKeys(42).toTracking(), Collections.singletonMap("V", new LongSingleValueSource()));
            svcs = (LongSingleValueSource)getColumnSource("V", long.class);
            svcs.startTrackingPrevValues();
            updateGraph.addSource(this);
            svcs.set(0L);
        }

        @Override
        public void run() {
            svcs.set(svcs.getLong(0) + 1);
            notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(), getRowSet().copy());
        }
    }

    private QueryCompiler compilerForUnitTests() {
        final Path queryCompilerDir = DataDir.get()
                .resolve("io.deephaven.engine.updategraph.impl.TestEventDrivenUpdateGraph.compilerForUnitTests");

        return QueryCompiler.create(queryCompilerDir.toFile(), getClass().getClassLoader());
    }

    @Test
    public void testSimpleAdd() {
        final EventDrivenUpdateGraph eventDrivenUpdateGraph = new EventDrivenUpdateGraph.Builder("TestEDUG").build();

        final ExecutionContext context = ExecutionContext.newBuilder().setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope().newQueryLibrary().setQueryCompiler(compilerForUnitTests()).build();
        try (final SafeCloseable ignored = context.open()) {
            final SourceThatRefreshes sourceThatRefreshes = new SourceThatRefreshes(eventDrivenUpdateGraph);
            final Table updated =
                    eventDrivenUpdateGraph.sharedLock().computeLocked(() -> sourceThatRefreshes.update("X=i"));

            int steps = 0;
            do {
                TestCase.assertEquals(steps, updated.size());
                eventDrivenUpdateGraph.requestRefresh();
            } while (steps++ < 100);
            TestCase.assertEquals(steps, updated.size());
        }
    }

    @Test
    public void testSimpleModify() {
        final EventDrivenUpdateGraph eventDrivenUpdateGraph = new EventDrivenUpdateGraph.Builder("TestEDUG").build();

        final ExecutionContext context = ExecutionContext.newBuilder().setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope().newQueryLibrary().setQueryCompiler(compilerForUnitTests()).build();
        try (final SafeCloseable ignored = context.open()) {
            final SourceThatModifiesItself modifySource = new SourceThatModifiesItself(eventDrivenUpdateGraph);
            final Table updated =
                    eventDrivenUpdateGraph.sharedLock().computeLocked(() -> modifySource.update("X=2 * V"));

            final ColumnSource<Long> xcs = updated.getColumnSource("X");

            int steps = 0;
            do {
                TestCase.assertEquals(1, updated.size());
                eventDrivenUpdateGraph.requestRefresh();

                TableTools.showWithRowSet(modifySource);

                final TrackingRowSet rowSet = updated.getRowSet();
                System.out.println("Step = " + steps);
                final long xv = xcs.getLong (rowSet.firstRowKey());
                TestCase.assertEquals(2L * (steps + 1), xv);
            } while (steps++ < 100);
            TestCase.assertEquals(1, updated.size());
        }
    }

    @Test
    public void testUpdatePerformanceTracker() {
        final Table upt = UpdatePerformanceTracker.getQueryTable();

        final long start = System.currentTimeMillis();

        final EventDrivenUpdateGraph eventDrivenUpdateGraph1 = new EventDrivenUpdateGraph.Builder("TestEDUG1").build();
        final EventDrivenUpdateGraph eventDrivenUpdateGraph2 = new EventDrivenUpdateGraph.Builder("TestEDUG2").build();

        final int count1 = 100;
        final int count2 = 200;
        final int time1 = 10;
        final int time2 = 5;

        doWork(eventDrivenUpdateGraph1, count1, time1);
        doWork(eventDrivenUpdateGraph2, count2, time2);

        do {
            final long now = System.currentTimeMillis();
            final long end = start + UpdatePerformanceTracker.REPORT_INTERVAL_MILLIS;
            if (end < now) {
                break;
            }
            System.out.println("Did work, waiting for performance cycle to complete: " + (end - now) + " ms");
            SleepUtil.sleep(end - now);
        } while (true);

        doWork(eventDrivenUpdateGraph1, 1, 0);
        doWork(eventDrivenUpdateGraph2, 1, 0);

        defaultUpdateGraph.requestRefresh();

        final Table uptAgged = upt.aggBy(Aggregation.AggSum("EntryIntervalUsage", "EntryIntervalInvocationCount", "EntryIntervalModified"), "UpdateGraph", "EntryId");
        final ExecutionContext context = ExecutionContext.newBuilder().setUpdateGraph(defaultUpdateGraph)
                .emptyQueryScope().newQueryLibrary().setQueryCompiler(compilerForUnitTests()).build();
        final Table inRange;
        try (final SafeCloseable ignored = context.open()) {
            inRange = defaultUpdateGraph.sharedLock().computeLocked(() -> uptAgged.update("EIUExpectedMillis = UpdateGraph==`TestEDUG1` ? (" + time1 + " * EntryIntervalInvocationCount) : (" + time2 + " * EntryIntervalInvocationCount)", "InRange=EntryIntervalUsage > 0.9 * EIUExpectedMillis && EntryIntervalUsage < 1.1 * EIUExpectedMillis"));
        }
        TableTools.show(inRange);

        final Table compare = inRange.dropColumns("EntryId", "EntryIntervalUsage", "EIUExpectedMillis");
        TableTools.show(compare);

        final Table expect = TableTools.newTable(stringCol("UpdateGraph", "TestEDUG1", "TestEDUG2"), longCol("EntryIntervalInvocationCount", count1, count2), longCol("EntryIntervalModified", count1, count2), booleanCol("InRange", true, true));
        TstUtils.assertTableEquals(expect, compare);
    }

    @ReflexiveUse(referrers = "TestEventDrivenUpdateGraph")
    static public <T> T sleepValue(long duration, T retVal) {
        final Object blech = new Object();
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (blech) {
            try {
                final long milliSeconds = duration / 1_000_000L;
                final int nanos = (int) (duration % 1_000_000L);
                blech.wait(milliSeconds, nanos);
            } catch (InterruptedException ignored) {
            }
        }
        return retVal;
    }

    private void doWork(final EventDrivenUpdateGraph eventDrivenUpdateGraph, final int durationMillis, final int steps) {
        final ExecutionContext context = ExecutionContext.newBuilder().setUpdateGraph(eventDrivenUpdateGraph)
                .emptyQueryScope().newQueryLibrary().setQueryCompiler(compilerForUnitTests()).build();
        try (final SafeCloseable ignored = context.open()) {
            final SourceThatModifiesItself modifySource = new SourceThatModifiesItself(eventDrivenUpdateGraph);
            final Table updated =
                    eventDrivenUpdateGraph.sharedLock().computeLocked(() -> modifySource.update("X=" + getClass().getName() + ".sleepValue(" + (1000L * 1000L * durationMillis) + ", 2 * V)"));

            int step = 0;
            do {
                TestCase.assertEquals(1, updated.size());
                eventDrivenUpdateGraph.requestRefresh();
            } while (step++ < steps);
            TestCase.assertEquals(1, updated.size());
        }
    }
}
