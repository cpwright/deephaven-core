package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class AuthInterceptor implements ClientInterceptor {

    public static AuthInterceptor of(HandshakeResponse response) {
        return ImmutableAuthInterceptor.of(
            Key.of(response.getMetadataHeader().toStringUtf8(), Metadata.ASCII_STRING_MARSHALLER),
            response.getSessionToken().toStringUtf8());
    }

    @Parameter
    public abstract Key<String> sessionHeaderKey();

    @Parameter
    public abstract String session();

    @Override
    public final <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new AddHeader<>(next.newCall(method, callOptions));
    }

    private final class AddHeader<ReqT, RespT>
        extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

        private AddHeader(ClientCall<ReqT, RespT> delegate) {
            super(delegate);
        }

        @Override
        public final void start(Listener<RespT> responseListener, Metadata headers) {
            headers.put(sessionHeaderKey(), session());
            super.start(responseListener, headers);
        }
    }
}
