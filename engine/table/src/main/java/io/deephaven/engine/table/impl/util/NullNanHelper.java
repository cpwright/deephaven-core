//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.util.QueryConstants;

/**
 * Helper functions to determine if a value is Null or NaN for purposes of skipping values or poisoning in aggregations.
 */
public final class NullNanHelper {

    public static boolean isNull(final byte x) {
        return x == QueryConstants.NULL_BYTE;
    }

    public static boolean isNull(final char x) {
        return x == QueryConstants.NULL_CHAR;
    }

    public static boolean isNull(final short x) {
        return x == QueryConstants.NULL_SHORT;
    }

    public static boolean isNull(final int x) {
        return x == QueryConstants.NULL_INT;
    }

    public static boolean isNull(final long x) {
        return x == QueryConstants.NULL_LONG;
    }

    public static boolean isNull(final float x) {
        return x == QueryConstants.NULL_FLOAT;
    }

    public static boolean isNull(final double x) {
        return x == QueryConstants.NULL_DOUBLE;
    }

    public static boolean isNull(final Object o) {
        return o == null;
    }
    
    public static boolean isNaN(final byte x) {
        return false;
    }

    public static boolean isNaN(final char x) {
        return false;
    }

    public static boolean isNaN(final short x) {
        return false;
    }

    public static boolean isNaN(final int x) {
        return false;
    }

    public static boolean isNaN(final long x) {
        return false;
    }

    public static boolean isNaN(final float x) {
        return Float.isNaN(x);
    }

    public static boolean isNaN(final double x) {
        return Double.isNaN(x);
    }

    public static boolean isNaN(final Object o) {
        return false;
    }

    public static boolean charHasNans() {
        return false;
    }

    public static boolean byteHasNans() {
        return false;
    }

    public static boolean shortHasNans() {
        return false;
    }

    public static boolean intHasNans() {
        return false;
    }

    public static boolean longHasNans() {
        return false;
    }

    public static boolean doubleHasNans() {
        return true;
    }

    public static boolean floatHasNans() {
        return true;
    }

    public static boolean ObjectHasNans() {
        return false;
    }
}