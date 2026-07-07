//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.col;

public class TestInstrumentedTableUpdateListenerAdapter extends RefreshingTableTestCase {

    /**
     * The creating thread's context is captured whole, so the creator's auth context is active during onUpdate. The
     * unit test harness context is systemic; capture must not depend on the systemic flag, because script session
     * contexts are also systemic.
     */
    public void testAuthContextCaptured() {
        final AuthContext creatorAuthContext = new AuthContext.Anonymous();
        final ExecutionContext creatorContext = ExecutionContext.getContext().withAuthContext(creatorAuthContext);
        assertSame(creatorContext, tickWithListenerCreatedUnder(creatorContext, true, false));
    }

    public void testAuthContextNotCaptured() {
        final AuthContext creatorAuthContext = new AuthContext.Anonymous();
        final ExecutionContext creatorContext = ExecutionContext.getContext().withAuthContext(creatorAuthContext);
        assertNotSame(creatorAuthContext, tickWithListenerCreatedUnder(creatorContext, false, false).getAuthContext());
    }

    public void testAuthContextCapturedShiftOblivious() {
        final AuthContext creatorAuthContext = new AuthContext.Anonymous();
        final ExecutionContext creatorContext = ExecutionContext.getContext().withAuthContext(creatorAuthContext);
        assertSame(creatorContext, tickWithListenerCreatedUnder(creatorContext, true, true));
    }

    public void testAuthContextNotCapturedShiftOblivious() {
        final AuthContext creatorAuthContext = new AuthContext.Anonymous();
        final ExecutionContext creatorContext = ExecutionContext.getContext().withAuthContext(creatorAuthContext);
        assertNotSame(creatorAuthContext, tickWithListenerCreatedUnder(creatorContext, false, true).getAuthContext());
    }

    /**
     * The creator's QueryScope must be observable during onUpdate, so listener callbacks can compile formulas.
     */
    public void testQueryScopeCaptured() {
        final ExecutionContext creatorContext = makeContextWithQueryScope();
        final ExecutionContext observed = tickWithListenerCreatedUnder(creatorContext, true, false);
        assertSame(creatorContext, observed);
        assertTrue(observed.getQueryScope().hasParamName("ListenerTestVar"));
    }

    public void testQueryScopeCapturedShiftOblivious() {
        final ExecutionContext creatorContext = makeContextWithQueryScope();
        final ExecutionContext observed = tickWithListenerCreatedUnder(creatorContext, true, true);
        assertSame(creatorContext, observed);
        assertTrue(observed.getQueryScope().hasParamName("ListenerTestVar"));
    }

    /**
     * Build a context with a fresh QueryScope holding a test variable.
     */
    private static ExecutionContext makeContextWithQueryScope() {
        final ExecutionContext creatorContext = ExecutionContext.newBuilder()
                .newQueryScope()
                .captureQueryLibrary()
                .captureQueryCompiler()
                .build();
        creatorContext.getQueryScope().putParam("ListenerTestVar", 42);
        return creatorContext;
    }

    /**
     * Create a listener under {@code creatorContext}, tick the source table outside that context, and return the
     * {@link ExecutionContext} that was active during {@code onUpdate}.
     */
    private static ExecutionContext tickWithListenerCreatedUnder(final ExecutionContext creatorContext,
            final boolean captureExecutionContext, final boolean shiftOblivious) {
        final QueryTable source = TstUtils.testRefreshingTable(i(10).toTracking(), col("Sentinel", 1));

        final MutableBoolean fired = new MutableBoolean();
        final MutableObject<ExecutionContext> observed = new MutableObject<>();
        // hold a strong reference to the listener for the duration of the test, as the source subscribes weakly
        final Object listener;
        try (final SafeCloseable ignored = creatorContext.open()) {
            if (shiftOblivious) {
                final ShiftObliviousInstrumentedListenerAdapter shiftObliviousListener =
                        new ShiftObliviousInstrumentedListenerAdapter(null, source, false, captureExecutionContext) {
                            @Override
                            public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
                                fired.setTrue();
                                observed.setValue(ExecutionContext.getContext());
                            }
                        };
                source.addUpdateListener(shiftObliviousListener, false);
                listener = shiftObliviousListener;
            } else {
                final TableUpdateListener updateListener =
                        new InstrumentedTableUpdateListenerAdapter(null, source, false, captureExecutionContext) {
                            @Override
                            public void onUpdate(final TableUpdate upstream) {
                                fired.setTrue();
                                observed.setValue(ExecutionContext.getContext());
                            }
                        };
                source.addUpdateListener(updateListener);
                listener = updateListener;
            }
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(20), col("Sentinel", 2));
            source.notifyListeners(i(20), i(), i());
        });

        assertNotNull(listener);
        assertTrue(fired.booleanValue());
        return observed.getValue();
    }
}
