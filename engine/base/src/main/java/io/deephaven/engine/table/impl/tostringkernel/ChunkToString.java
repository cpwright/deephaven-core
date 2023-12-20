/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.tostringkernel;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

/**
 * Convert an arbitrary chunk to a chunk of toString-ed objects.
 */
public class ChunkToString {

    /**
     * Return a chunk that contains toStringed Objects representing the values in values.
     */
    public interface ToString extends SafeCloseable {
        /**
         * Convert all values to a String.
         *
         * @param values the values to convert
         *
         * @return a chunk containing values as Strings
         */
        ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values);
    }

    public static ToString getToString(ChunkType type, int capacity) {
        switch (type) {
            case Boolean:
                return new BooleanToString(capacity);
            case Char:
                return new CharToString(capacity);
            case Byte:
                return new ByteToString(capacity);
            case Short:
                return new ShortToString(capacity);
            case Int:
                return new IntToString(capacity);
            case Long:
                return new LongToString(capacity);
            case Float:
                return new FloatToString(capacity);
            case Double:
                return new DoubleToString(capacity);
            case Object:
                return new ObjectToString(capacity);
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    private static abstract class ToStringCommon implements ChunkToString.ToString {
        final WritableObjectChunk<String, Values> stringChunk;

        private ToStringCommon(int capacity) {
            stringChunk = WritableObjectChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            stringChunk.close();
        }
    }

    private static class ObjectToString extends ToStringCommon {
        private ObjectToString(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final ObjectChunk<?, ? extends Values> objectChunk = values.asObjectChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                // noinspection UnnecessarytoStringing
                stringChunk.set(ii, Objects.toString(objectChunk.get(ii)));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }

    private static class BooleanToString extends ToStringCommon {
        BooleanToString(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final BooleanChunk<? extends Values> booleanChunk = values.asBooleanChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                // noinspection UnnecessarytoStringing
                stringChunk.set(ii, Boolean.toString(booleanChunk.get(ii)));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }

    private static class CharToString extends ToStringCommon {
        CharToString(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                final char cc = charChunk.get(ii);
                stringChunk.set(ii, cc == QueryConstants.NULL_CHAR ? "(null)" : Character.toString(cc));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }

    private static class ByteToString extends ToStringCommon {
        ByteToString(int capacity) {
            
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                final byte bb = byteChunk.get(ii);
                stringChunk.set(ii, bb == QueryConstants.NULL_BYTE ? "(null)" : Byte.toString(bb));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }

    private static class ShortToString extends ToStringCommon {
        ShortToString(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                final short ss = shortChunk.get(ii);
                stringChunk.set(ii, ss == QueryConstants.NULL_SHORT ? "(null)" : Short.toString(ss));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }

    private static class IntToString extends ToStringCommon {
        IntToString(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                final int vv = intChunk.get(ii);
                stringChunk.set(ii, vv == QueryConstants.NULL_INT ? "(null)" : Integer.toString(vv));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }

    private static class LongToString extends ToStringCommon {
        LongToString(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                final long vv = longChunk.get(ii);
                stringChunk.set(ii, vv == QueryConstants.NULL_LONG ? "(null)" : Long.toString(vv));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }

    private static class FloatToString extends ToStringCommon {
        FloatToString(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                final float vv = floatChunk.get(ii);
                stringChunk.set(ii, vv == QueryConstants.NULL_FLOAT ? "(null)" : Float.toString(vv));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }

    private static class DoubleToString extends ToStringCommon {
        DoubleToString(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<String, ? extends Values> toString(Chunk<? extends Values> values) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                final double vv = doubleChunk.get(ii);
                stringChunk.set(ii, vv == QueryConstants.NULL_DOUBLE ? "(null)" : Double.toString(vv));
            }
            stringChunk.setSize(values.size());
            return stringChunk;
        }
    }
}
