//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;

/**
 * Incremental as-of joins whose right side adds, removes and modifies many rows in one cycle, so that each change is
 * applied in several chunks. The stamps of a changed block ascend with the row key, descend with it, or are random, or
 * start ordered and then become disordered, and repeat so that runs of equal stamps break ties by row key and continue
 * across chunk boundaries. Every cycle is compared against a fresh join of the current tables, with restamp budgets
 * that restamp every chunk against the final SSA after the first, switch to the final SSA part way through an update,
 * and never switch.
 */
public class QueryTableAjMultiChunkTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final int SMALL_CHUNK_SIZE = 8;
    private static final int STEPS = 40;

    private enum StampOrder {
        FORWARD, REVERSE, RANDOM, FORWARD_THEN_DISORDERED, REVERSE_THEN_DISORDERED
    }

    @Test
    public void testMultiChunkRightChanges() {
        for (final int restampBudgetFactor : new int[] {0, 1, new JoinControl().restampBudgetFactor(),
                Integer.MAX_VALUE}) {
            for (final boolean leftRefreshing : new boolean[] {false, true}) {
                for (int seed = 0; seed < 4; ++seed) {
                    try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                        testMultiChunkRightChanges(seed, leftRefreshing, restampBudgetFactor);
                    }
                }
            }
        }
    }

    private void testMultiChunkRightChanges(final int seed, final boolean leftRefreshing,
            final int restampBudgetFactor) {
        final Random random = new Random(seed);
        final JoinControl control = new JoinControl() {
            @Override
            int rightSsaNodeSize() {
                return 4;
            }

            @Override
            int leftSsaNodeSize() {
                return 4;
            }

            @Override
            public int rightChunkSize() {
                return SMALL_CHUNK_SIZE;
            }

            @Override
            public int leftChunkSize() {
                return SMALL_CHUNK_SIZE;
            }

            @Override
            public int restampBudgetFactor() {
                return restampBudgetFactor;
            }
        };

        final int leftSize = 150;
        final int[] leftBuckets = new int[leftSize];
        final int[] leftStamps = new int[leftSize];
        final int[] leftSentinels = new int[leftSize];
        for (int ii = 0; ii < leftSize; ++ii) {
            leftBuckets[ii] = random.nextInt(2);
            leftStamps[ii] = random.nextInt(1000);
            leftSentinels[ii] = ii;
        }
        final RowSet leftRowSet = RowSetFactory.flat(leftSize);
        final QueryTable left = leftRefreshing
                ? TstUtils.testRefreshingTable(leftRowSet.copy().toTracking(), intCol("Bucket", leftBuckets),
                        intCol("LeftStamp", leftStamps), intCol("LeftSentinel", leftSentinels))
                : TstUtils.testTable(leftRowSet.copy().toTracking(), intCol("Bucket", leftBuckets),
                        intCol("LeftStamp", leftStamps), intCol("LeftSentinel", leftSentinels));
        leftRowSet.close();

        final QueryTable right = TstUtils.testRefreshingTable(i().toTracking(), intCol("Bucket"), intCol("RightStamp"),
                intCol("RightSentinel"));
        final QueryTable rightReversed = (QueryTable) right.reverse();

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                ajNugget(control, left, right, "LeftStamp=RightStamp", SortingOrder.Ascending, false),
                ajNugget(control, left, right, "LeftStamp=RightStamp", SortingOrder.Ascending, true),
                ajNugget(control, left, rightReversed, "LeftStamp=RightStamp", SortingOrder.Descending, false),
                ajNugget(control, left, rightReversed, "LeftStamp=RightStamp", SortingOrder.Descending, true),
                ajNugget(control, left, right, "Bucket,LeftStamp=RightStamp", SortingOrder.Ascending, false),
                ajNugget(control, left, right, "Bucket,LeftStamp=RightStamp", SortingOrder.Ascending, true),
                ajNugget(control, left, rightReversed, "Bucket,LeftStamp=RightStamp", SortingOrder.Descending, false),
                ajNugget(control, left, rightReversed, "Bucket,LeftStamp=RightStamp", SortingOrder.Descending, true),
        };

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        long nextRightKey = 0;
        long nextLeftKey = leftSize;
        int sentinel = 0;

        for (int step = 0; step < STEPS; ++step) {
            final RowSet rightRowSet = right.getRowSet();

            // remove a run of rows by position, or rows scattered across the table
            final WritableRowSet removed;
            if (rightRowSet.isNonempty() && random.nextInt(3) != 0) {
                if (random.nextBoolean()) {
                    final long start = random.nextInt(rightRowSet.intSize());
                    removed = rightRowSet.subSetByPositionRange(start, start + 2 * SMALL_CHUNK_SIZE
                            + random.nextInt(6 * SMALL_CHUNK_SIZE));
                } else {
                    final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                    final int removeOneIn = 1 + random.nextInt(3);
                    rightRowSet.forAllRowKeys(key -> {
                        if (random.nextInt(removeOneIn) == 0) {
                            builder.appendKey(key);
                        }
                    });
                    removed = builder.build();
                }
            } else {
                removed = RowSetFactory.empty();
            }

            // modify the stamps and buckets of some surviving rows, which restamps them as a removal and an addition
            final WritableRowSet modified;
            if (random.nextInt(3) == 0) {
                try (final WritableRowSet survivors = rightRowSet.minus(removed)) {
                    final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                    survivors.forAllRowKeys(key -> {
                        if (random.nextInt(3) == 0) {
                            builder.appendKey(key);
                        }
                    });
                    modified = builder.build();
                }
            } else {
                modified = RowSetFactory.empty();
            }

            // add a block of rows, either past the existing row keys or interleaved with them
            final int addedSize = random.nextInt(4) == 0 ? 0 : 1 + random.nextInt(8 * SMALL_CHUNK_SIZE);
            final WritableRowSet added;
            if (addedSize == 0) {
                added = RowSetFactory.empty();
            } else if (random.nextBoolean() || rightRowSet.isEmpty()) {
                final int gap = 1 + random.nextInt(3);
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                for (int ii = 0; ii < addedSize; ++ii) {
                    builder.appendKey(nextRightKey + (long) ii * gap);
                }
                nextRightKey += (long) addedSize * gap + 1000;
                added = builder.build();
            } else {
                added = RowSetFactory.empty();
                final long span = nextRightKey + 1000;
                while (added.size() < addedSize) {
                    final long candidate = (long) (random.nextDouble() * span);
                    if (rightRowSet.find(candidate) < 0) {
                        added.insert(candidate);
                    }
                }
                nextRightKey = Math.max(nextRightKey, added.lastRowKey() + 1);
            }

            final int[] addedStamps = makeStamps(random, added.intSize());
            final int[] addedBuckets = new int[added.intSize()];
            final int[] addedSentinels = new int[added.intSize()];
            for (int ii = 0; ii < addedBuckets.length; ++ii) {
                addedBuckets[ii] = random.nextInt(2);
                addedSentinels[ii] = ++sentinel;
            }
            final int[] modifiedStamps = makeStamps(random, modified.intSize());
            // a modified row may also move to the other bucket
            final int[] modifiedBuckets = new int[modified.intSize()];
            final int[] modifiedSentinels = new int[modified.intSize()];
            for (int ii = 0; ii < modifiedSentinels.length; ++ii) {
                modifiedBuckets[ii] = random.nextInt(2);
                modifiedSentinels[ii] = ++sentinel;
            }

            // sometimes the left side changes in the same cycle
            final boolean leftChanges = leftRefreshing && random.nextBoolean();
            final WritableRowSet leftRemoved;
            final WritableRowSet leftAdded;
            if (leftChanges) {
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                left.getRowSet().forAllRowKeys(key -> {
                    if (random.nextInt(10) == 0) {
                        builder.appendKey(key);
                    }
                });
                leftRemoved = builder.build();
                final int leftAddedSize = random.nextInt(2 * SMALL_CHUNK_SIZE);
                leftAdded = leftAddedSize == 0 ? RowSetFactory.empty()
                        : RowSetFactory.fromRange(nextLeftKey, nextLeftKey + leftAddedSize - 1);
                nextLeftKey += leftAddedSize;
            } else {
                leftRemoved = RowSetFactory.empty();
                leftAdded = RowSetFactory.empty();
            }
            final int[] leftAddedBuckets = new int[leftAdded.intSize()];
            final int[] leftAddedStamps = new int[leftAdded.intSize()];
            final int[] leftAddedSentinels = new int[leftAdded.intSize()];
            for (int ii = 0; ii < leftAddedStamps.length; ++ii) {
                leftAddedBuckets[ii] = random.nextInt(2);
                leftAddedStamps[ii] = random.nextInt(1000);
                leftAddedSentinels[ii] = ++sentinel;
            }

            updateGraph.runWithinUnitTestCycle(() -> {
                if (leftChanges) {
                    TstUtils.removeRows(left, leftRemoved);
                    TstUtils.addToTable(left, leftAdded, intCol("Bucket", leftAddedBuckets),
                            intCol("LeftStamp", leftAddedStamps), intCol("LeftSentinel", leftAddedSentinels));
                    left.notifyListeners(leftAdded.copy(), leftRemoved.copy(), i());
                }
                TstUtils.removeRows(right, removed);
                TstUtils.addToTable(right, added, intCol("Bucket", addedBuckets), intCol("RightStamp", addedStamps),
                        intCol("RightSentinel", addedSentinels));
                TstUtils.addToTable(right, modified, intCol("Bucket", modifiedBuckets),
                        intCol("RightStamp", modifiedStamps),
                        intCol("RightSentinel", modifiedSentinels));
                right.notifyListeners(added.copy(), removed.copy(), modified.copy());
            });
            TstUtils.validate("step " + step + ", seed " + seed + ", leftRefreshing " + leftRefreshing
                    + ", restampBudgetFactor " + restampBudgetFactor, en);

            removed.close();
            modified.close();
            added.close();
            leftRemoved.close();
            leftAdded.close();
        }
    }

    /**
     * Stamps for a block of rows in row key order: ascending, descending or random, each repeated so that equal stamps
     * form runs. The block is added from its highest positions down, so an ordered block suffix followed by disordered
     * positions starts in order and then overlaps it, sometimes with a run of equal stamps that continues across the
     * point where the order breaks.
     */
    private static int[] makeStamps(final Random random, final int size) {
        final StampOrder stampOrder = StampOrder.values()[random.nextInt(StampOrder.values().length)];
        final int repeat = 1 + random.nextInt(4);
        final int base = random.nextInt(1000);
        final int cut = size == 0 ? 0 : random.nextInt(size);
        final int[] stamps = new int[size];
        for (int ii = size - 1; ii >= 0; --ii) {
            switch (stampOrder) {
                case FORWARD:
                    stamps[ii] = base + ii / repeat;
                    break;
                case REVERSE:
                    stamps[ii] = base - ii / repeat;
                    break;
                case RANDOM:
                    stamps[ii] = random.nextInt(1000 / repeat) * repeat;
                    break;
                case FORWARD_THEN_DISORDERED:
                case REVERSE_THEN_DISORDERED:
                    if (ii >= cut) {
                        final int offset = ii / repeat;
                        stamps[ii] = stampOrder == StampOrder.FORWARD_THEN_DISORDERED ? base + offset : base - offset;
                    } else if (random.nextInt(3) == 0) {
                        // continue the run of equal stamps at the start of the ordered suffix
                        stamps[ii] = stamps[cut];
                    } else {
                        stamps[ii] = base - size / repeat + random.nextInt(2 * size / repeat + 1);
                    }
                    break;
            }
        }
        return stamps;
    }

    private static EvalNuggetInterface ajNugget(final JoinControl control, final QueryTable left,
            final QueryTable right, final String columnsToMatch, final SortingOrder order,
            final boolean disallowExactMatch) {
        return EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, left, right,
                MatchPairFactory.getExpressions(columnsToMatch.split(",")),
                MatchPairFactory.getExpressions("RightStamp", "RightSentinel"), order, disallowExactMatch));
    }
}
