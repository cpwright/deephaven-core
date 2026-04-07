//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.profiling.ThreadProfiler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.minus;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.plus;

/**
 * A smaller entry that simply records usage data, meant for aggregating into the larger entry.
 */
public class BasePerformanceEntry implements LogOutputAppendable, ReadMetricsRecorder {
    private long usageNanos;

    private long cpuNanos;
    private long userCpuNanos;

    private long allocatedBytes;
    private long poolAllocatedBytes;

    private long dataReadNanos;
    private long dataReadCount;
    private long metadataReadNanos;

    private long startTimeNanos;

    private long startCpuNanos;
    private long startUserCpuNanos;

    private long startAllocatedBytes;
    private long startPoolAllocatedBytes;

    public synchronized void onBaseEntryStart() {
        startAllocatedBytes = ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes();
        startPoolAllocatedBytes = QueryPerformanceRecorderState.getPoolAllocatedBytesForCurrentThread();

        startUserCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadUserTime();
        startCpuNanos = ThreadProfiler.DEFAULT.getCurrentThreadCpuTime();
        startTimeNanos = System.nanoTime();
    }

    public synchronized void onBaseEntryEnd() {
        userCpuNanos = plus(userCpuNanos,
                minus(ThreadProfiler.DEFAULT.getCurrentThreadUserTime(), startUserCpuNanos));
        cpuNanos =
                plus(cpuNanos, minus(ThreadProfiler.DEFAULT.getCurrentThreadCpuTime(), startCpuNanos));

        usageNanos += System.nanoTime() - startTimeNanos;

        poolAllocatedBytes = plus(poolAllocatedBytes,
                minus(QueryPerformanceRecorderState.getPoolAllocatedBytesForCurrentThread(), startPoolAllocatedBytes));
        allocatedBytes = plus(allocatedBytes,
                minus(ThreadProfiler.DEFAULT.getCurrentThreadAllocatedBytes(), startAllocatedBytes));

        startAllocatedBytes = 0;
        startPoolAllocatedBytes = 0;

        startUserCpuNanos = 0;
        startCpuNanos = 0;
        startTimeNanos = 0;
    }

    synchronized void baseEntryReset() {
        Assert.eqZero(startTimeNanos, "startTimeNanos");

        usageNanos = 0;

        cpuNanos = 0;
        userCpuNanos = 0;

        allocatedBytes = 0;
        poolAllocatedBytes = 0;

        dataReadNanos = 0;
        dataReadCount = 0;
        metadataReadNanos = 0;
    }

    /**
     * Get the aggregate usage in nanoseconds. This getter should be called by exclusive owners of the entry, and never
     * concurrently with mutators.
     *
     * @return total wall clock time in nanos
     */
    public long getUsageNanos() {
        return usageNanos;
    }

    /**
     * Get the aggregate cpu time in nanoseconds. This getter should be called by exclusive owners of the entry, and
     * never concurrently with mutators.
     *
     * @return total cpu time in nanos
     */
    public long getCpuNanos() {
        return cpuNanos;
    }

    /**
     * Get the aggregate cpu user time in nanoseconds. This getter should be called by exclusive owners of the entry,
     * and never concurrently with mutators.
     *
     * @return total cpu user time in nanos
     */
    public long getUserCpuNanos() {
        return userCpuNanos;
    }

    /**
     * Get the aggregate allocated memory in bytes. This getter should be called by exclusive owners of the entry, and
     * never concurrently with mutators.
     *
     * @return The bytes of allocated memory attributed to the instrumented operation.
     */
    public long getAllocatedBytes() {
        return allocatedBytes;
    }

    /**
     * Get allocated pooled/reusable memory attributed to the instrumented operation in bytes. This getter should be
     * called by exclusive owners of the entry, and never concurrently with mutators.
     *
     * @return total pool allocated memory in bytes
     */
    public long getPoolAllocatedBytes() {
        return poolAllocatedBytes;
    }

    /**
     * Get the aggregate time spent reading data in nanoseconds. This getter should be called by exclusive owners of the
     * entry, and never concurrently with mutators.
     *
     * @return total data read time in nanos
     */
    public long getDataReadNanos() {
        return dataReadNanos;
    }

    /**
     * Get the aggregate number of data read operations. This getter should be called by exclusive owners of the entry,
     * and never concurrently with mutators.
     *
     * @return total number of data read operations
     */
    public long getDataReadCount() {
        return dataReadCount;
    }

    /**
     * Get the aggregate time spent on metadata operations (e.g. listing files, checking existence, determining file
     * sizes) in nanoseconds. This getter should be called by exclusive owners of the entry, and never concurrently with
     * mutators.
     *
     * @return total metadata operation time in nanos
     */
    public long getMetadataReadNanos() {
        return metadataReadNanos;
    }

    /**
     * Record a data read operation. May be called concurrently from I/O threads.
     *
     * @param nanos time spent on the read in nanoseconds
     * @param count number of read operations (typically 1)
     * @param source optional human-readable description of the data source (e.g. a URI or filename), may be null
     */
    public synchronized void recordRead(final long nanos, final int count, @Nullable final String source) {
        dataReadNanos += nanos;
        dataReadCount += count;
    }

    /**
     * Record a metadata operation (e.g. listing files, checking existence, determining file sizes). May be called
     * concurrently from I/O threads.
     *
     * @param nanos time spent on the metadata operation in nanoseconds
     * @param source optional human-readable description of the source (e.g. a URI or directory path), may be null
     */
    public synchronized void recordMetadataOperation(final long nanos, @Nullable final String source) {
        metadataReadNanos += nanos;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        final LogOutput currentValues = logOutput.append("BasePerformanceEntry{")
                .append(", intervalUsageNanos=").append(usageNanos)
                .append(", intervalCpuNanos=").append(cpuNanos)
                .append(", intervalUserCpuNanos=").append(userCpuNanos)
                .append(", intervalAllocatedBytes=").append(allocatedBytes)
                .append(", intervalPoolAllocatedBytes=").append(poolAllocatedBytes)
                .append(", dataReadNanos=").append(dataReadNanos)
                .append(", dataReadCount=").append(dataReadCount)
                .append(", metadataReadNanos=").append(metadataReadNanos);
        return appendStart(currentValues)
                .append('}');
    }

    LogOutput appendStart(LogOutput logOutput) {
        return logOutput
                .append(", startCpuNanos=").append(startCpuNanos)
                .append(", startUserCpuNanos=").append(startUserCpuNanos)
                .append(", startTimeNanos=").append(startTimeNanos)
                .append(", startAllocatedBytes=").append(startAllocatedBytes)
                .append(", startPoolAllocatedBytes=").append(startPoolAllocatedBytes);
    }

    /**
     * Accumulate the values from another entry into this one. The provided entry will not be mutated.
     *
     * @param entry the entry to accumulate
     */
    public synchronized void accumulate(@NotNull final BasePerformanceEntry entry) {
        this.usageNanos += entry.usageNanos;
        this.cpuNanos = plus(this.cpuNanos, entry.cpuNanos);
        this.userCpuNanos = plus(this.userCpuNanos, entry.userCpuNanos);

        this.allocatedBytes = plus(this.allocatedBytes, entry.allocatedBytes);
        this.poolAllocatedBytes = plus(this.poolAllocatedBytes, entry.poolAllocatedBytes);

        this.dataReadNanos += entry.dataReadNanos;
        this.dataReadCount += entry.dataReadCount;
        this.metadataReadNanos += entry.metadataReadNanos;
    }
}
