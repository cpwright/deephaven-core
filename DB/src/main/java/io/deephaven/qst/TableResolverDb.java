package io.deephaven.qst;

import io.deephaven.db.tables.Table;
import io.deephaven.qst.manager.ExportedTable;

public enum TableResolverDb implements TableResolver {
    INSTANCE;

    @Override
    public final Table resolve(ExportedTable export) {
        return ExportManagerDb.cast(export).queryTable();
    }
}
