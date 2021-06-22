package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

class SessionHandler {

    private final ScheduledExecutorService executor;
    private final ClientInterceptorDelegate channelInterceptor;
    private final SessionServiceGrpc.SessionServiceStub sessionService;

    @Inject
    public SessionHandler(ScheduledExecutorService executor,
        ClientInterceptorDelegate channelInterceptor, SessionServiceStub sessionService) {
        this.executor = Objects.requireNonNull(executor);
        this.channelInterceptor = Objects.requireNonNull(channelInterceptor);
        this.sessionService = Objects.requireNonNull(sessionService);
    }

    void newSession() {
        sessionService.newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build(),
            new HandshakeHandler());
    }

    private void sendHandshake() {
        sessionService.refreshSessionToken(HandshakeRequest.newBuilder().setAuthProtocol(0).build(),
            new HandshakeHandler());
    }

    private class HandshakeHandler implements StreamObserver<HandshakeResponse> {

        @Override
        public void onNext(HandshakeResponse value) {
            channelInterceptor.set(AuthInterceptor.of(value));
            final long refreshDelayMs = Math.min(
                System.currentTimeMillis() + value.getTokenExpirationDelayMillis() / 3,
                value.getTokenDeadlineTimeMillis() - value.getTokenExpirationDelayMillis() / 10);
            executor.schedule(SessionHandler.this::sendHandshake, refreshDelayMs,
                TimeUnit.MILLISECONDS);
        }

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
        }

        @Override
        public void onCompleted() {

        }
    }
}
