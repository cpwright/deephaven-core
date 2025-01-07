package io.deephaven.engine.table;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

@Value.Immutable
@BuildableStyle
public
interface DataIndexOptions {
    DataIndexOptions DEFAULT = DataIndexOptions.builder().build();

    /**
     * Does this operation use only a subset of the DataIndex?
     *
     * <p>The DataIndex implementation may use this hint to defer work for some row sets.</p>
     *
     * @return if this operation is only going to use a subset of this data index
     */
    @Value.Default
    default boolean operationUsesPartialTable() {
        return false;
    }

    /**
     * Create a new builder for a {@link DataIndexOptions}.
     * @return
     */
    static Builder builder() {
        return ImmutableDataIndexOptions.builder();
    }

    /**
     * The builder interface to construct a {@link DataIndexOptions}.
     */
    interface Builder {
        /**
         * Set whether this operation only uses a subset of the data index.
         *
         * @param usesPartialTable true if this operation only uses a partial table
         * @return this builder
         */
        Builder operationUsesPartialTable(boolean usesPartialTable);

        /**
         * Build the {@link DataIndexOptions}.
         * @return an immutable DataIndexOptions structure.
         */
        DataIndexOptions build();
    }
}
