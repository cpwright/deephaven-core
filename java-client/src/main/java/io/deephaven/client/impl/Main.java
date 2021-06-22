package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.qst.examples.Examples;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class Main {

    public static void main(String[] args) throws InterruptedException {
        final String target = "localhost:10000";

        final ClientInterceptorDelegate channelInterceptor = new ClientInterceptorDelegate();
        final Channel channel = ClientInterceptors.intercept(
            ManagedChannelBuilder.forTarget(target).usePlaintext().build(), channelInterceptor);


        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);

        final SessionServiceStub sessionService = SessionServiceGrpc.newStub(channel);

        final SessionHandler sessionHandler =
            new SessionHandler(executor, channelInterceptor, sessionService);

        sessionHandler.newSession();

        Thread.sleep(5000);

        final ExportManagerClientImpl manager =
            new ExportManagerClientImpl(sessionService, TableServiceGrpc.newStub(channel));

        manager.startExportedTableUpdateMessages();

        Examples.showSizes(manager, TableSizeImpl.INSTANCE);

        executor.shutdownNow();
    }
}
