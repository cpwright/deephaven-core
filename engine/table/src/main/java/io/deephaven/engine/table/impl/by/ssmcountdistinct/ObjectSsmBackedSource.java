//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSsmBackedSource and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.engine.table.impl.ssms.EqualsConsistentObjectSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper;

import java.util.Objects;

import io.deephaven.util.compare.ObjectComparisons;

import io.deephaven.vector.ObjectVector;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssms.AbstractObjectSegmentedSortedMultiset;
import io.deephaven.engine.rowset.RowSet;

/**
 * A {@link SsmBackedColumnSource} for Objects.
 */
public class ObjectSsmBackedSource extends AbstractColumnSource<ObjectVector>
        implements ColumnSourceGetDefaults.ForObject<ObjectVector>,
        MutableColumnSourceGetDefaults.ForObject<ObjectVector>,
        SsmBackedColumnSource<AbstractObjectSegmentedSortedMultiset, ObjectVector> {
    private final ObjectArraySource<AbstractObjectSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    // region Constructor
    private final boolean equalsConsistent;

    public ObjectSsmBackedSource(Class type) {
        super(ObjectVector.class, type);
        underlying = new ObjectArraySource<>(AbstractObjectSegmentedSortedMultiset.class, type);
        equalsConsistent = BinarySearchKernelHelper.compareConsistentWithEquality(type);
    }
    // endregion Constructor

    // region SsmBackedColumnSource
    @Override
    public AbstractObjectSegmentedSortedMultiset getOrCreate(long key) {
        AbstractObjectSegmentedSortedMultiset ssm = underlying.getUnsafe(key);
        if (ssm == null) {
            // region CreateNew
            ssm = equalsConsistent
                    ? new EqualsConsistentObjectSegmentedSortedMultiset(SsmDistinctContext.NODE_SIZE, componentType)
                    : new ObjectSegmentedSortedMultiset(SsmDistinctContext.NODE_SIZE, componentType);
            underlying.set(key, ssm);
            // endregion CreateNew
        }
        ssm.setTrackDeltas(trackingPrevious);
        return ssm;
    }

    @Override
    public AbstractObjectSegmentedSortedMultiset getCurrentSsm(long key) {
        return underlying.getUnsafe(key);
    }

    @Override
    public void clear(long key) {
        underlying.set(key, null);
    }

    @Override
    public void ensureCapacity(long capacity) {
        underlying.ensureCapacity(capacity);
    }

    @Override
    public ObjectArraySource<AbstractObjectSegmentedSortedMultiset> getUnderlyingSource() {
        return underlying;
    }
    // endregion

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public ObjectVector get(long rowKey) {
        return underlying.get(rowKey);
    }

    @Override
    public ObjectVector getPrev(long rowKey) {
        final AbstractObjectSegmentedSortedMultiset maybePrev = underlying.getPrev(rowKey);
        return maybePrev == null ? null : maybePrev.getPrevValues();
    }

    @Override
    public void startTrackingPrevValues() {
        trackingPrevious = true;
        underlying.startTrackingPrevValues();
    }

    @Override
    public void clearDeltas(RowSet indices) {
        indices.iterator().forEachLong(key -> {
            final AbstractObjectSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if (ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
