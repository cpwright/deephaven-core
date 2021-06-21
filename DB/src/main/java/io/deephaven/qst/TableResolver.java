package io.deephaven.qst;

import io.deephaven.db.tables.Table;
import io.deephaven.qst.manager.ExportedTable;

/**
 * Resolves an {@link ExportedTable exported table} into a {@link Table}.
 *
 * @see io.deephaven.qst.manager.ExportManager
 */
public interface TableResolver {

    Table resolve(ExportedTable export);
}
