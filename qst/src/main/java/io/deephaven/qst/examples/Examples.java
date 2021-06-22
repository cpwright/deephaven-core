package io.deephaven.qst.examples;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.qst.manager.ExportManager;
import io.deephaven.qst.manager.ExportedTable;
import io.deephaven.qst.table.Table;
import io.deephaven.qst.todo.TableSize;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Examples {
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

    public static Table iSum() {
        return i()
            .by(Collections.emptyList(), Collections.singletonList(Aggregation.AggSum("I")));
    }

    public static Table ijSum() {
        return ij()
            .view("IJ=I*J")
            .by(Collections.emptyList(), Collections.singletonList(Aggregation.AggSum("IJ")));
    }

    public static Table ijkSum() {
        return ijk()
            .view("IJK=I*J*K")
            .by(Collections.emptyList(), Collections.singletonList(Aggregation.AggSum("IJK")));
    }

    public static void showSizes(ExportManager manager, TableSize tableSize)
        throws InterruptedException {
        final List<? extends ExportedTable> exports = manager
            .export(Arrays.asList(i(), ij(), ijk()));

        ExportedTable ijSum;
        try (
            ExportedTable i = exports.get(0);
            ExportedTable ij = exports.get(1);
            ExportedTable ijk = exports.get(2)) {

            ijSum = manager.export(ijSum());

            System.out.printf(
                "i=%d, ij=%d, ijk=%d%n",
                tableSize.awaitSize(i),
                tableSize.awaitSize(ij),
                tableSize.awaitSize(ijk));
        }

        try {
            System.out.printf("ijSum=%d%n", tableSize.awaitSize(ijSum));
        } finally {
            ijSum.close();
        }
    }
}
