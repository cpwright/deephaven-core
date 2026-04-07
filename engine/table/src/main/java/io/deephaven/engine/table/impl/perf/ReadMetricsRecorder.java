//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import org.jetbrains.annotations.Nullable;

/**
 * Interface for recording I/O read metrics. Implementations aggregate timing data for data reads (reading bytes from
 * files/channels) and metadata operations (listing files, checking existence, determining file sizes).
 *
 * <p>
 * The {@code source} parameter is an optional human-readable description of the data source (e.g. a URI or filename)
 * that may be used for diagnostic tracing in future implementations.
 */
public interface ReadMetricsRecorder {

    /**
     * A no-op recorder that discards all metrics.
     */
    ReadMetricsRecorder NULL_RECORDER = new ReadMetricsRecorder() {
        @Override
        public void recordRead(final long elapsedNanos, final int bytesRead, @Nullable final String source) {
            // no-op
        }

        @Override
        public void recordMetadataOperation(final long elapsedNanos, @Nullable final String source) {
            // no-op
        }
    };

    /**
     * Obtain a {@link ReadMetricsRecorder} for the current thread's performance nugget. If no recorder is installed
     * (i.e. the dummy recorder is active), returns {@link #NULL_RECORDER}.
     *
     * @return a {@link ReadMetricsRecorder} that records to the current thread's enclosing performance nugget
     */
    static ReadMetricsRecorder current() {
        final QueryPerformanceRecorder recorder = QueryPerformanceRecorder.getInstance();
        final QueryPerformanceNugget nugget = recorder.getEnclosingNugget();
        if (nugget == QueryPerformanceNugget.DUMMY_NUGGET) {
            return NULL_RECORDER;
        }
        return nugget;
    }

    /**
     * Record a data read operation (reading bytes from a file or channel).
     *
     * @param elapsedNanos time spent on the read in nanoseconds
     * @param bytesRead number of bytes read
     * @param source optional human-readable description of the data source (e.g. a URI or filename), may be null
     */
    void recordRead(long elapsedNanos, int bytesRead, @Nullable String source);

    /**
     * Record a metadata operation (e.g. listing files, checking existence, determining file sizes).
     *
     * @param elapsedNanos time spent on the metadata operation in nanoseconds
     * @param source optional human-readable description of the source (e.g. a URI or directory path), may be null
     */
    void recordMetadataOperation(long elapsedNanos, @Nullable String source);
}

