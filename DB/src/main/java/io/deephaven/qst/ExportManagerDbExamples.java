package io.deephaven.qst;

import io.deephaven.qst.manager.ExportManager;
import io.deephaven.qst.manager.ExportedTable;
import io.deephaven.qst.table.Table;
import java.util.function.Consumer;

public class ExportManagerDbExamples {

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

    public static void test1() {
        final ExportManager manager = new ExportManagerDb();
        final TableResolver resolver = TableResolverDb.INSTANCE;

        consume(i(), manager, resolver, ExportManagerDbExamples::printSize);
        consume(ij(), manager, resolver, ExportManagerDbExamples::printSize);
        consume(ijk(), manager, resolver, ExportManagerDbExamples::printSize);
    }

    public static void test2() {
        final ExportManager manager = new ExportManagerDb();
        final TableResolver resolver = TableResolverDb.INSTANCE;

        try (ExportedTable i = manager.export(i());
            ExportedTable ij = manager.export(ij());
            ExportedTable ijk = manager.export(ijk())) {
            printSize(resolver.resolve(i));
            printSize(resolver.resolve(ij));
            printSize(resolver.resolve(ijk));
        }
    }

    public static void test3() {
        final ExportManager manager = new ExportManagerDb();
        final TableResolver resolver = TableResolverDb.INSTANCE;

        ExportedTable i = manager.export(i());
        ExportedTable ij = manager.export(ij());
        ExportedTable ijk = manager.export(ijk());

        printSize(resolver.resolve(i));
        i.release();

        printSize(resolver.resolve(ij));
        ij.release();

        printSize(resolver.resolve(ijk));
        ijk.release();
    }

    public static void test4() {
        final ExportManager manager = new ExportManagerDb();
        final TableResolver resolver = TableResolverDb.INSTANCE;

        ExportedTable ijk = manager.exportChain(ijk());
        ExportedTable ij = manager.export(ij());
        ExportedTable i = manager.export(i());

        printSize(resolver.resolve(i));
        i.release();

        printSize(resolver.resolve(ij));
        ij.release();

        printSize(resolver.resolve(ijk));
        ijk.release();
    }

    private static void printSize(io.deephaven.db.tables.Table t) {
        System.out.println(t.size());
    }

    public static void consume(
        Table table,
        ExportManager manager,
        TableResolver resolver,
        Consumer<io.deephaven.db.tables.Table> consumer) {
        try (ExportedTable export = manager.export(table)) {
            consumer.accept(resolver.resolve(export));
        }
    }
}
