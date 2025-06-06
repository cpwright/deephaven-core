//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.session_pb.HandshakeRequest",
        namespace = JsPackage.GLOBAL)
public class HandshakeRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetPayloadUnionType {
        @JsOverlay
        static HandshakeRequest.GetPayloadUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetPayloadValueUnionType {
        @JsOverlay
        static HandshakeRequest.SetPayloadValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static HandshakeRequest.ToObjectReturnType.GetPayloadUnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default Uint8Array asUint8Array() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static HandshakeRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getAuthProtocol();

        @JsProperty
        HandshakeRequest.ToObjectReturnType.GetPayloadUnionType getPayload();

        @JsProperty
        void setAuthProtocol(double authProtocol);

        @JsProperty
        void setPayload(HandshakeRequest.ToObjectReturnType.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(
                    Js.<HandshakeRequest.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(
                    Js.<HandshakeRequest.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static HandshakeRequest.ToObjectReturnType0.GetPayloadUnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default Uint8Array asUint8Array() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static HandshakeRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getAuthProtocol();

        @JsProperty
        HandshakeRequest.ToObjectReturnType0.GetPayloadUnionType getPayload();

        @JsProperty
        void setAuthProtocol(double authProtocol);

        @JsProperty
        void setPayload(HandshakeRequest.ToObjectReturnType0.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(
                    Js.<HandshakeRequest.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(
                    Js.<HandshakeRequest.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }
    }

    public static native HandshakeRequest deserializeBinary(Uint8Array bytes);

    public static native HandshakeRequest deserializeBinaryFromReader(
            HandshakeRequest message, Object reader);

    public static native void serializeBinaryToWriter(HandshakeRequest message, Object writer);

    public static native HandshakeRequest.ToObjectReturnType toObject(
            boolean includeInstance, HandshakeRequest msg);

    public native double getAuthProtocol();

    public native HandshakeRequest.GetPayloadUnionType getPayload();

    public native String getPayload_asB64();

    public native Uint8Array getPayload_asU8();

    public native Uint8Array serializeBinary();

    public native void setAuthProtocol(double value);

    public native void setPayload(HandshakeRequest.SetPayloadValueUnionType value);

    @JsOverlay
    public final void setPayload(String value) {
        setPayload(Js.<HandshakeRequest.SetPayloadValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setPayload(Uint8Array value) {
        setPayload(Js.<HandshakeRequest.SetPayloadValueUnionType>uncheckedCast(value));
    }

    public native HandshakeRequest.ToObjectReturnType0 toObject();

    public native HandshakeRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
