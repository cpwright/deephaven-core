package io.deephaven.client.impl;

import dagger.Module;
import dagger.Provides;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javax.inject.Singleton;

@Module
public interface JavaClientModule {

    @Provides
    @Singleton
    static ClientInterceptorDelegate providesChannelInterceptor() {
        return new ClientInterceptorDelegate();
    }

    @Provides
    @Singleton
    static Channel providesChannel(ClientInterceptorDelegate channelInterceptor) {
        final ManagedChannel basic =
            ManagedChannelBuilder.forTarget("localhost:10000").usePlaintext().build();
        return ClientInterceptors.intercept(basic, channelInterceptor);
    }

    @Provides
    static SessionServiceStub providesSessionService(Channel channel) {
        return SessionServiceGrpc.newStub(channel);
    }
}
