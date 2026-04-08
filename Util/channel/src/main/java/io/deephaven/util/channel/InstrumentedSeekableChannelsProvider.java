//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A {@link SeekableChannelsProvider} decorator that instruments read operations and metadata operations with timing
 * metrics. The timing data is reported to a {@link ChannelReadMetricsRecorder} obtained from a {@link Supplier} on each
 * operation, allowing it to resolve to the current thread's performance context.
 */
public class InstrumentedSeekableChannelsProvider extends SeekableChannelsProviderDelegate {

    private final Supplier<ChannelReadMetricsRecorder> recorderSupplier;

    /**
     * Create an instrumented wrapper around the given provider.
     *
     * @param delegate the underlying provider to delegate to
     * @param recorderSupplier a supplier that returns the {@link ChannelReadMetricsRecorder} for the current context
     *        (typically resolved per-call via a thread-local)
     */
    public InstrumentedSeekableChannelsProvider(
            @NotNull final SeekableChannelsProvider delegate,
            @NotNull final Supplier<ChannelReadMetricsRecorder> recorderSupplier) {
        super(delegate);
        this.recorderSupplier = recorderSupplier;
    }

    @Override
    public boolean exists(@NotNull final URI uri) {
        final long startNanos = System.nanoTime();
        try {
            return super.exists(uri);
        } finally {
            final long elapsed = System.nanoTime() - startNanos;
            recorderSupplier.get().recordMetadataOperation("exists", elapsed, uri.toString());
        }
    }

    @Override
    public SeekableByteChannel getReadChannel(
            @NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) throws IOException {
        return new InstrumentedSeekableByteChannel(
                super.getReadChannel(channelContext, uri), uri.toString(), recorderSupplier);
    }

    @Override
    public SeekableByteChannel getReadChannel(
            @NotNull final SeekableChannelContext channelContext,
            @NotNull final String uriStr) throws IOException {
        return new InstrumentedSeekableByteChannel(
                super.getReadChannel(channelContext, uriStr), uriStr, recorderSupplier);
    }

    @Override
    public Stream<URI> list(@NotNull final URI directory) throws IOException {
        final long startNanos = System.nanoTime();
        try {
            return super.list(directory);
        } finally {
            final long elapsed = System.nanoTime() - startNanos;
            recorderSupplier.get().recordMetadataOperation("list", elapsed, directory.toString());
        }
    }

    @Override
    public Stream<URI> walk(@NotNull final URI directory) throws IOException {
        final long startNanos = System.nanoTime();
        try {
            return super.walk(directory);
        } finally {
            final long elapsed = System.nanoTime() - startNanos;
            recorderSupplier.get().recordMetadataOperation("walk", elapsed, directory.toString());
        }
    }

    /**
     * A {@link SeekableByteChannel} wrapper that instruments {@code read()} and {@code size()} calls with timing
     * metrics.
     */
    private static final class InstrumentedSeekableByteChannel implements SeekableByteChannel,
            CachedChannelProvider.ContextHolder {

        private final SeekableByteChannel delegate;
        private final String source;
        private final Supplier<ChannelReadMetricsRecorder> recorderSupplier;

        InstrumentedSeekableByteChannel(
                @NotNull final SeekableByteChannel delegate,
                @NotNull final String source,
                @NotNull final Supplier<ChannelReadMetricsRecorder> recorderSupplier) {
            this.delegate = delegate;
            this.source = source;
            this.recorderSupplier = recorderSupplier;
        }

        @Override
        public int read(final ByteBuffer dst) throws IOException {
            final long startNanos = System.nanoTime();
            final int bytesRead = delegate.read(dst);
            final long elapsed = System.nanoTime() - startNanos;
            recorderSupplier.get().recordRead(elapsed, Math.max(bytesRead, 0), source);
            return bytesRead;
        }

        @Override
        public int write(final ByteBuffer src) throws IOException {
            return delegate.write(src);
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public SeekableByteChannel position(final long newPosition) throws IOException {
            delegate.position(newPosition);
            return this;
        }

        @Override
        public long size() throws IOException {
            final long startNanos = System.nanoTime();
            try {
                return delegate.size();
            } finally {
                final long elapsed = System.nanoTime() - startNanos;
                recorderSupplier.get().recordMetadataOperation("size", elapsed, source);
            }
        }

        @Override
        public SeekableByteChannel truncate(final long size) throws IOException {
            delegate.truncate(size);
            return this;
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void setContext(final SeekableChannelContext channelContext) {
            if (delegate instanceof CachedChannelProvider.ContextHolder) {
                ((CachedChannelProvider.ContextHolder) delegate).setContext(channelContext);
            }
        }
    }
}
