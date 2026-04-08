//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.table.impl.perf.ReadMetricsRecorder;
import io.deephaven.util.channel.ChannelReadMetricsRecorder;
import io.deephaven.util.channel.InstrumentedSeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Factory that creates instrumented {@link SeekableChannelsProvider} instances for Parquet reading. The returned
 * providers are wrapped with {@link InstrumentedSeekableChannelsProvider} so that I/O metrics (data read time, metadata
 * operation time) are reported to the current thread's performance nugget.
 */
public final class ParquetChannelProviderFactory {

    private ParquetChannelProviderFactory() {}

    /**
     * Create a new instrumented {@link SeekableChannelsProvider} for the given URI scheme and special instructions.
     *
     * @param scheme the URI scheme (e.g. "file", "s3")
     * @param specialInstructions special instructions for the provider, may be null
     * @return a new instrumented {@link SeekableChannelsProvider}
     */
    public static SeekableChannelsProvider create(
            @NotNull final String scheme,
            @Nullable final Object specialInstructions) {
        return instrument(SeekableChannelsProviderLoader.getInstance().load(scheme, specialInstructions));
    }

    /**
     * Wrap the given provider with instrumentation that reports I/O metrics to the current thread's performance nugget.
     *
     * @param provider the underlying provider to instrument
     * @return an instrumented wrapper around the given provider
     */
    public static SeekableChannelsProvider instrument(@NotNull final SeekableChannelsProvider provider) {
        return new InstrumentedSeekableChannelsProvider(provider,
                ParquetChannelProviderFactory::currentRecorder);
    }

    /**
     * Resolve the current thread's {@link ChannelReadMetricsRecorder} by bridging from
     * {@link ReadMetricsRecorder#current()}.
     */
    private static ChannelReadMetricsRecorder currentRecorder() {
        final ReadMetricsRecorder r = ReadMetricsRecorder.current();
        if (r == ReadMetricsRecorder.NULL_RECORDER) {
            return ChannelReadMetricsRecorder.NULL_RECORDER;
        }
        // Adapt ReadMetricsRecorder to ChannelReadMetricsRecorder via an anonymous wrapper.
        return new ChannelReadMetricsRecorder() {
            @Override
            public void recordRead(final long elapsedNanos, final int bytesRead, final String source) {
                r.recordRead(elapsedNanos, bytesRead, source);
            }

            @Override
            public void recordMetadataOperation(@NotNull final String type, final long elapsedNanos, final String source) {
                r.recordMetadataOperation(type, elapsedNanos, source);
            }
        };
    }
}


