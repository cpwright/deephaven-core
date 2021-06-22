package io.deephaven.client.impl;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;

public class ClientInterceptorDelegate implements ClientInterceptor {
    private volatile ClientInterceptor delegate;

    public void set(ClientInterceptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public final <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        final ClientInterceptor local = delegate;
        if (local != null) {
            return local.interceptCall(method, callOptions, next);
        }
        return next.newCall(method, callOptions);
    }
}
