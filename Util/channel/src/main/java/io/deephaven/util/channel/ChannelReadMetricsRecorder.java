//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.Nullable;

/**
 * Callback interface for recording I/O metrics at the channel/provider level. This is a lightweight interface intended
 * to be implemented by performance instrumentation in higher-level modules.
 *
 * <p>
 * The {@code source} parameter provides a human-readable description of the data source (e.g. a URI or filename) for
 * diagnostic tracing.
 */
public interface ChannelReadMetricsRecorder {

    /**
     * A no-op recorder that discards all metrics.
     */
    ChannelReadMetricsRecorder NULL_RECORDER = new ChannelReadMetricsRecorder() {
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
