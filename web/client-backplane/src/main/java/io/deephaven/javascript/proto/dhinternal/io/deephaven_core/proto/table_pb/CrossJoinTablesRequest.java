//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.CrossJoinTablesRequest",
        namespace = JsPackage.GLOBAL)
public class CrossJoinTablesRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LeftIdFieldType {
            @JsOverlay
            static CrossJoinTablesRequest.ToObjectReturnType.LeftIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static CrossJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
                        Object o) {
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
            static CrossJoinTablesRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CrossJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    CrossJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<CrossJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<CrossJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static CrossJoinTablesRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnsToAddList();

        @JsProperty
        JsArray<String> getColumnsToMatchList();

        @JsProperty
        CrossJoinTablesRequest.ToObjectReturnType.LeftIdFieldType getLeftId();

        @JsProperty
        double getReserveBits();

        @JsProperty
        CrossJoinTablesRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        Object getRightId();

        @JsProperty
        void setColumnsToAddList(JsArray<String> columnsToAddList);

        @JsOverlay
        default void setColumnsToAddList(String[] columnsToAddList) {
            setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
        }

        @JsProperty
        void setColumnsToMatchList(JsArray<String> columnsToMatchList);

        @JsOverlay
        default void setColumnsToMatchList(String[] columnsToMatchList) {
            setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
        }

        @JsProperty
        void setLeftId(CrossJoinTablesRequest.ToObjectReturnType.LeftIdFieldType leftId);

        @JsProperty
        void setReserveBits(double reserveBits);

        @JsProperty
        void setResultId(CrossJoinTablesRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setRightId(Object rightId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LeftIdFieldType {
            @JsOverlay
            static CrossJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static CrossJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
                        Object o) {
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
            static CrossJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CrossJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    CrossJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<CrossJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<CrossJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static CrossJoinTablesRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnsToAddList();

        @JsProperty
        JsArray<String> getColumnsToMatchList();

        @JsProperty
        CrossJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType getLeftId();

        @JsProperty
        double getReserveBits();

        @JsProperty
        CrossJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        Object getRightId();

        @JsProperty
        void setColumnsToAddList(JsArray<String> columnsToAddList);

        @JsOverlay
        default void setColumnsToAddList(String[] columnsToAddList) {
            setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
        }

        @JsProperty
        void setColumnsToMatchList(JsArray<String> columnsToMatchList);

        @JsOverlay
        default void setColumnsToMatchList(String[] columnsToMatchList) {
            setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
        }

        @JsProperty
        void setLeftId(CrossJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType leftId);

        @JsProperty
        void setReserveBits(double reserveBits);

        @JsProperty
        void setResultId(CrossJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setRightId(Object rightId);
    }

    public static native CrossJoinTablesRequest deserializeBinary(Uint8Array bytes);

    public static native CrossJoinTablesRequest deserializeBinaryFromReader(
            CrossJoinTablesRequest message, Object reader);

    public static native void serializeBinaryToWriter(CrossJoinTablesRequest message, Object writer);

    public static native CrossJoinTablesRequest.ToObjectReturnType toObject(
            boolean includeInstance, CrossJoinTablesRequest msg);

    public native String addColumnsToAdd(String value, double index);

    public native String addColumnsToAdd(String value);

    public native String addColumnsToMatch(String value, double index);

    public native String addColumnsToMatch(String value);

    public native void clearColumnsToAddList();

    public native void clearColumnsToMatchList();

    public native void clearLeftId();

    public native void clearResultId();

    public native void clearRightId();

    public native JsArray<String> getColumnsToAddList();

    public native JsArray<String> getColumnsToMatchList();

    public native TableReference getLeftId();

    public native double getReserveBits();

    public native Ticket getResultId();

    public native TableReference getRightId();

    public native boolean hasLeftId();

    public native boolean hasResultId();

    public native boolean hasRightId();

    public native Uint8Array serializeBinary();

    public native void setColumnsToAddList(JsArray<String> value);

    @JsOverlay
    public final void setColumnsToAddList(String[] value) {
        setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setColumnsToMatchList(JsArray<String> value);

    @JsOverlay
    public final void setColumnsToMatchList(String[] value) {
        setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setLeftId();

    public native void setLeftId(TableReference value);

    public native void setReserveBits(double value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setRightId();

    public native void setRightId(TableReference value);

    public native CrossJoinTablesRequest.ToObjectReturnType0 toObject();

    public native CrossJoinTablesRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
