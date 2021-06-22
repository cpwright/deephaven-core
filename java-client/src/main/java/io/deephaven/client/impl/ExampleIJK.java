package io.deephaven.client.impl;

import io.deephaven.qst.table.Table;

public class ExampleIJK {

    public static Table empty42() {
        return Table.empty(42);
    }

    public static Table i() {
        return empty42().view("I=i");
    }

    public static Table ij() {
        return i().join(i(), "", "J=I");
    }

    public static Table ijk() {
        return ij().join(i(), "", "K=I");
    }
}
