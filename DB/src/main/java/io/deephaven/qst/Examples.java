package io.deephaven.qst;

import static io.deephaven.qst.examples.Examples.i;
import static io.deephaven.qst.examples.Examples.ij;
import static io.deephaven.qst.examples.Examples.ijSum;
import static io.deephaven.qst.examples.Examples.ijk;

import io.deephaven.qst.ExportManagerDb.State.Export;
import io.deephaven.qst.manager.ExportManager;
import java.util.Arrays;
import java.util.List;

public class Examples {

    public static void showSizesGeneric() throws InterruptedException {
        final ExportManager manager = new ExportManagerDb();
        io.deephaven.qst.examples.Examples.showSizes(manager, TableSizeDb.INSTANCE);
    }

    public static void showSizesSpecific() {
        final ExportManagerDb manager = new ExportManagerDb();
        final List<Export> exports = manager
            .export(Arrays.asList(i(), ij(), ijk()));
        final Export ijSum;
        try (
            Export i = exports.get(0);
            Export ij = exports.get(1);
            Export ijk = exports.get(2)) {

            ijSum = manager.export(ijSum());

            System.out.printf(
                "i=%d, ij=%d, ijk=%d%n",
                i.size(),
                ij.size(),
                ijk.size());
        }

        try {
            System.out.printf("ijSum=%d%n", ijSum.size());
        } finally {
            ijSum.close();
        }
    }
}
