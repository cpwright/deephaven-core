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

    public void testAuthContextCaptured() {
        final AuthContext creatorContext = new AuthContext.Anonymous();
        assertSame(creatorContext, tickWithListenerCreatedUnder(creatorContext, true, false));
    }

    public void testAuthContextNotCaptured() {
        final AuthContext creatorContext = new AuthContext.Anonymous();
        assertNotSame(creatorContext, tickWithListenerCreatedUnder(creatorContext, false, false));
    }

    public void testAuthContextCapturedShiftOblivious() {
        final AuthContext creatorContext = new AuthContext.Anonymous();
        assertSame(creatorContext, tickWithListenerCreatedUnder(creatorContext, true, true));
    }

    public void testAuthContextNotCapturedShiftOblivious() {
        final AuthContext creatorContext = new AuthContext.Anonymous();
        assertNotSame(creatorContext, tickWithListenerCreatedUnder(creatorContext, false, true));
    }

    /**
     * Create a listener under {@code creatorContext}, tick the source table outside that context, and return the
     * {@link AuthContext} that was active during {@code onUpdate}.
     */
    private static AuthContext tickWithListenerCreatedUnder(final AuthContext creatorContext,
            final boolean captureAuthContext, final boolean shiftOblivious) {
        final QueryTable source = TstUtils.testRefreshingTable(i(10).toTracking(), col("Sentinel", 1));

        final MutableBoolean fired = new MutableBoolean();
        final MutableObject<AuthContext> observed = new MutableObject<>();
        // hold a strong reference to the listener for the duration of the test, as the source subscribes weakly
        final Object listener;
        try (final SafeCloseable ignored = ExecutionContext.getContext().withAuthContext(creatorContext).open()) {
            if (shiftOblivious) {
                final ShiftObliviousInstrumentedListenerAdapter shiftObliviousListener =
                        new ShiftObliviousInstrumentedListenerAdapter(null, source, false, captureAuthContext) {
                            @Override
                            public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
                                fired.setTrue();
                                observed.setValue(ExecutionContext.getContext().getAuthContext());
                            }
                        };
                source.addUpdateListener(shiftObliviousListener, false);
                listener = shiftObliviousListener;
            } else {
                final TableUpdateListener updateListener =
                        new InstrumentedTableUpdateListenerAdapter(null, source, false, captureAuthContext) {
                            @Override
                            public void onUpdate(final TableUpdate upstream) {
                                fired.setTrue();
                                observed.setValue(ExecutionContext.getContext().getAuthContext());
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
