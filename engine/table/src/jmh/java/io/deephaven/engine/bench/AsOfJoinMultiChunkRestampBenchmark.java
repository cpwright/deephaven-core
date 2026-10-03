//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.bench;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.AsOfJoinBenchmarkAccess;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * An as-of join of a left table against a refreshing right table, where one cycle adds or removes a block of right rows
 * that spans many chunks of the right chunk size.
 *
 * <p>
 * The block holds {@code blockRows} rows at row keys {@code [0, blockRows)}; one more right row, at row key
 * {@code blockRows}, is always present and sorts before every block stamp for aj and after every block stamp for raj,
 * so every left row matches it when the block is absent. Block row {@code k} has stamp {@code 2 * k} for
 * {@code ASCENDING}, {@code 2 * (blockRows - 1 - k)} for {@code DESCENDING} and {@code 2 * p(k)} for a random
 * permutation {@code p} for {@code RANDOM}. The left table has {@code leftRows} rows with odd stamps spread across the
 * block's stamps, so every left row is restamped by the block. Every row of both tables has the key 0.
 * </p>
 *
 * <p>
 * Stamps that descend with the row key are the case that restamps each left row once per chunk when each chunk is
 * restamped as it is applied, for both aj and raj. The restampBudgetFactor parameter is the join's
 * {@link io.deephaven.engine.table.impl.JoinControl#restampBudgetFactor()}; {@code 2147483647} restamps every chunk as
 * it is applied. The path parameter selects a zero-key or bucketed join of a static or refreshing left table.
 * </p>
 *
 * <pre>
 * ./gradlew engine-table:jmhJar
 * java -jar engine/table/build/libs/deephaven-engine-table-&lt;version&gt;-jmh.jar AsOfJoinMultiChunkRestampBenchmark
 * </pre>
 */
@Fork(value = 2, jvmArgs = {"-Xms12G", "-Xmx12G"})
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 6)
@State(Scope.Benchmark)
public class AsOfJoinMultiChunkRestampBenchmark {
    static {
        System.setProperty("Configuration.rootFile", "dh-tests.prop");
        System.setProperty("workspace", "build/workspace");
    }

    public enum Join {
        AJ, RAJ
    }

    public enum StampOrder {
        ASCENDING, DESCENDING, RANDOM
    }

    public enum Path {
        ZERO_KEY_STATIC_LEFT, ZERO_KEY_REFRESHING_LEFT, BUCKETED_STATIC_LEFT, BUCKETED_REFRESHING_LEFT
    }

    @Param({"ZERO_KEY_STATIC_LEFT"})
    public Path path;

    @Param({"AJ", "RAJ"})
    public Join join;

    @Param({"ASCENDING", "DESCENDING", "RANDOM"})
    public StampOrder stampOrder;

    @Param({"3", "2147483647"})
    public int restampBudgetFactor;

    @Param({"4000000"})
    public int leftRows;

    @Param({"2621440"})
    public int blockRows;

    @Param({"65536"})
    public int rightChunkSize;

    private EngineCleanup engine;
    private ControlledUpdateGraph updateGraph;
    private LivenessScope scope;

    private Table left;
    private TrackingWritableRowSet rightRowSet;
    private QueryTable right;
    private Table result;
    private BlackholeListener listener;
    private boolean blockPresent;

    /**
     * A right column whose value is a function of the row key.
     */
    private static class RowKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long[] blockValues;
        private final long otherValue;

        private RowKeySource(final long[] blockValues, final long otherValue) {
            super(long.class);
            this.blockValues = blockValues;
            this.otherValue = otherValue;
        }

        @Override
        public long getLong(final long rowKey) {
            return rowKey < blockValues.length ? blockValues[(int) rowKey] : otherValue;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return getLong(rowKey);
        }

        @Override
        public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> dest,
                @NotNull final RowSequence rowSequence) {
            final WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
            longDest.setSize(0);
            rowSequence.forAllRowKeys(rowKey -> longDest.add(getLong(rowKey)));
        }

        @Override
        public void fillPrevChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest, @NotNull final RowSequence rowSequence) {
            fillChunk(context, dest, rowSequence);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial(final Blackhole blackhole) throws Exception {
        engine = new EngineCleanup();
        engine.setUp();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final long[] blockStamps = new long[blockRows];
        final long[] blockSentinels = new long[blockRows];
        for (int ii = 0; ii < blockRows; ++ii) {
            blockSentinels[ii] = ii;
            blockStamps[ii] = 2L * (stampOrder == StampOrder.DESCENDING ? blockRows - 1 - ii : ii);
        }
        if (stampOrder == StampOrder.RANDOM) {
            final Random random = new Random(0);
            for (int ii = blockRows - 1; ii > 0; --ii) {
                final int swap = random.nextInt(ii + 1);
                final long tmp = blockStamps[ii];
                blockStamps[ii] = blockStamps[swap];
                blockStamps[swap] = tmp;
            }
        }

        final long[] leftStamps = new long[leftRows];
        for (int ii = 0; ii < leftRows; ++ii) {
            leftStamps[ii] = 2L * (long) blockRows * ii / leftRows | 1;
        }

        final boolean keyed = path == Path.BUCKETED_STATIC_LEFT || path == Path.BUCKETED_REFRESHING_LEFT;
        scope = new LivenessScope();
        LivenessScopeStack.push(scope);
        updateGraph.startCycleForUnitTests();
        try {
            final QueryTable leftTable = (QueryTable) TableTools.newTable(TableTools.longCol("K", new long[leftRows]),
                    TableTools.longCol("T", leftStamps));
            leftTable.setRefreshing(path == Path.ZERO_KEY_REFRESHING_LEFT || path == Path.BUCKETED_REFRESHING_LEFT);
            left = leftTable;

            rightRowSet = RowSetFactory.fromKeys(blockRows).toTracking();
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            columns.put("K", new RowKeySource(new long[blockRows], 0));
            columns.put("T", new RowKeySource(blockStamps, join == Join.AJ ? -1 : 2L * blockRows + 1));
            columns.put("V", new RowKeySource(blockSentinels, -1));
            right = new QueryTable(rightRowSet, columns);
            right.setRefreshing(true);
            blockPresent = false;

            result = AsOfJoinBenchmarkAccess.asOfJoin(rightChunkSize, restampBudgetFactor, leftTable, right,
                    join == Join.RAJ, keyed);
            listener = new BlackholeListener(blackhole);
            result.addUpdateListener(listener);
        } finally {
            updateGraph.completeCycleForUnitTests();
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() throws Exception {
        // the incremental result must match a static join of the final right table
        final Table rightSnapshot = right.snapshot();
        TstUtils.assertTableEquals(join == Join.AJ ? left.snapshot().aj(rightSnapshot, "K,T>=T", "V")
                : left.snapshot().raj(rightSnapshot, "K,T<=T", "V"), result);
        result.removeUpdateListener(listener);
        listener = null;
        result = null;
        left = null;
        right = null;
        rightRowSet = null;
        LivenessScopeStack.pop(scope);
        scope.release();
        scope = null;
        updateGraph = null;
        engine.tearDown();
        engine = null;
    }

    private void changeBlock(final boolean add) {
        updateGraph.runWithinUnitTestCycle(() -> {
            if (add) {
                rightRowSet.insertRange(0, blockRows - 1);
                right.notifyListeners(new TableUpdateImpl(RowSetFactory.flat(blockRows), RowSetFactory.empty(),
                        RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            } else {
                rightRowSet.removeRange(0, blockRows - 1);
                right.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.flat(blockRows),
                        RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            }
        });
        blockPresent = add;
        if (listener.e != null) {
            throw new IllegalStateException(listener.e);
        }
    }

    @State(Scope.Benchmark)
    public static class BlockAbsent {
        @Setup(Level.Invocation)
        public void setupInvocation(final AsOfJoinMultiChunkRestampBenchmark benchmark) {
            if (benchmark.blockPresent) {
                benchmark.changeBlock(false);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class BlockPresent {
        @Setup(Level.Invocation)
        public void setupInvocation(final AsOfJoinMultiChunkRestampBenchmark benchmark) {
            if (!benchmark.blockPresent) {
                benchmark.changeBlock(true);
            }
        }
    }

    @Benchmark
    public void addBlock(final BlockAbsent absent) {
        changeBlock(true);
    }

    @Benchmark
    public void removeBlock(final BlockPresent present) {
        changeBlock(false);
    }
}
