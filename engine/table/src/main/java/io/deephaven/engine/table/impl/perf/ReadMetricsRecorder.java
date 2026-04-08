//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
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
     * Thread-local override for the current thread's {@link ReadMetricsRecorder}. When set, {@link #current()} returns
     * this value instead of resolving through the performance nugget. This supports nested installations &mdash; each
     * {@link #install(ReadMetricsRecorder)} call saves the previous value and restores it on close.
     */
    ThreadLocal<ReadMetricsRecorder> THREAD_LOCAL = new ThreadLocal<>();

    /**
     * A no-op recorder that discards all metrics.
     */
    ReadMetricsRecorder NULL_RECORDER = new ReadMetricsRecorder() {
        @Override
        public void recordRead(final long elapsedNanos, final int bytesRead, @Nullable final String source) {
            // no-op
        }

        @Override
        public void recordMetadataOperation(
                @NotNull final String type, final long elapsedNanos, @Nullable final String source) {
            // no-op
        }
    };

    /**
     * Install the given {@link ReadMetricsRecorder} as the current thread's recorder. Returns a {@link SafeCloseable}
     * that, when closed, restores the previous recorder (supporting nested/stacked installations). Intended for use
     * with try-with-resources.
     *
     * @param recorder the recorder to install on the current thread
     * @return a {@link SafeCloseable} that restores the previous recorder when closed
     */
    static SafeCloseable install(@NotNull final ReadMetricsRecorder recorder) {
        final ReadMetricsRecorder previous = THREAD_LOCAL.get();
        THREAD_LOCAL.set(recorder);
        return () -> {
            if (previous == null) {
                THREAD_LOCAL.remove();
            } else {
                THREAD_LOCAL.set(previous);
            }
        };
    }

    /**
     * Obtain a {@link ReadMetricsRecorder} for the current thread. If a thread-local override has been installed via
     * {@link #install(ReadMetricsRecorder)}, that recorder is returned. Otherwise, the current thread's performance
     * nugget is used. If no recorder is available, returns {@link #NULL_RECORDER}.
     *
     * @return a {@link ReadMetricsRecorder} for the current thread
     */
    static ReadMetricsRecorder current() {
        final ReadMetricsRecorder override = THREAD_LOCAL.get();
        if (override != null) {
            return override;
        }
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
     * @param type the type of metadata operation (e.g. "exists", "list", "walk", "size")
     * @param elapsedNanos time spent on the metadata operation in nanoseconds
     * @param source optional human-readable description of the source (e.g. a URI or directory path), may be null
     */
    void recordMetadataOperation(@NotNull String type, long elapsedNanos, @Nullable String source);
}
