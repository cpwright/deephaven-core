package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ExportManagerClientImpl extends ExportManagerClientBase {

    private static final Logger log = LoggerFactory.getLogger(ExportManagerClientImpl.class);

    private final SessionServiceGrpc.SessionServiceStub sessionStub;
    private final TableServiceGrpc.TableServiceStub tableStub;

    @Inject
    public ExportManagerClientImpl(SessionServiceStub sessionStub, TableServiceStub tableStub) {
        this.sessionStub = Objects.requireNonNull(sessionStub);
        this.tableStub = Objects.requireNonNull(tableStub);
    }

    @Override
    protected void execute(BatchTableRequest batchTableRequest) {
        System.out.println(batchTableRequest);
        tableStub.batch(batchTableRequest, new StreamObserver<ExportedTableCreationResponse>() {
            @Override
            public void onNext(ExportedTableCreationResponse value) {

            }

            @Override
            public void onError(Throwable t) {
                log.error("BatchTableRequest onError", t);
            }

            @Override
            public void onCompleted() {

            }
        });
    }

    @Override
    protected void executeRelease(Ticket ticket) {
        sessionStub.release(ticket, new StreamObserver<ReleaseResponse>() {
            @Override
            public void onNext(ReleaseResponse value) {

            }

            @Override
            public void onError(Throwable t) {
                log.error("Release onError", t);
            }

            @Override
            public void onCompleted() {

            }
        });
    }

    @Override
    protected void execute(ExportedTableUpdatesRequest request,
        StreamObserver<ExportedTableUpdateMessage> observer) {
        tableStub.exportedTableUpdates(request, observer);
    }
}
