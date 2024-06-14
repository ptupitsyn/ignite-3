/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table.distributed.index;

import static java.util.Collections.unmodifiableMap;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.Map;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/** Immutable index meta, based on the {@link CatalogIndexDescriptor}. */
public class IndexMeta implements Serializable {
    private static final long serialVersionUID = 1044129530453957897L;

    private final int catalogVersion;

    private final int indexId;

    private final int tableId;

    private final String indexName;

    private final MetaIndexStatus currentStatus;

    @IgniteToStringInclude
    private final Map<MetaIndexStatus, MetaIndexStatusChange> statusChanges;

    /**
     * Constructor.
     *
     * @param catalogVersion Catalog version in which the current meta was created.
     * @param indexId Index ID.
     * @param tableId Table ID to which the index belongs.
     * @param indexName Index name.
     * @param currentStatus Current status of the index
     * @param statusChanges <b>Immutable</b> map of index statuses with change info (for example catalog version) in which they appeared.
     */
    private IndexMeta(
            int catalogVersion,
            int indexId,
            int tableId,
            String indexName,
            MetaIndexStatus currentStatus,
            Map<MetaIndexStatus, MetaIndexStatusChange> statusChanges
    ) {
        this.catalogVersion = catalogVersion;
        this.indexId = indexId;
        this.tableId = tableId;
        this.indexName = indexName;
        this.currentStatus = currentStatus;
        this.statusChanges = unmodifiableMap(statusChanges);
    }

    /**
     * Creates a index meta instance.
     *
     * @param catalogIndexDescriptor Catalog index descriptor to create meta from.
     * @param catalog Catalog version from which the {@code catalogIndexDescriptor} was taken.
     */
    static IndexMeta of(CatalogIndexDescriptor catalogIndexDescriptor, Catalog catalog) {
        assert catalog.index(catalogIndexDescriptor.id()) != null :
                "indexId=" + catalogIndexDescriptor.id() + ", catalogVersion=" + catalog.version();

        return new IndexMeta(
                catalog.version(),
                catalogIndexDescriptor.id(),
                catalogIndexDescriptor.tableId(),
                catalogIndexDescriptor.name(),
                MetaIndexStatus.convert(catalogIndexDescriptor.status()),
                Map.of(
                        MetaIndexStatus.convert(catalogIndexDescriptor.status()),
                        new MetaIndexStatusChange(catalog.version(), catalog.time())
                )
        );
    }

    /** Returns catalog version in which the current meta was created. */
    int catalogVersion() {
        return catalogVersion;
    }

    /** Returns index ID. */
    public int indexId() {
        return indexId;
    }

    /** Returns table ID to which the index belongs. */
    public int tableId() {
        return tableId;
    }

    /** Returns index name. */
    public String indexName() {
        return indexName;
    }

    /**
     * Changes the index name.
     *
     * @param catalogVersion Catalog version in which the index name has changed.
     * @param newIndexName New index name.
     * @return New instance of the index meta with only a new index name.
     */
    IndexMeta indexName(int catalogVersion, String newIndexName) {
        return new IndexMeta(catalogVersion, indexId, tableId, newIndexName, currentStatus, new EnumMap<>(statusChanges));
    }

    /** Returns the current status of the index. */
    public MetaIndexStatus status() {
        return currentStatus;
    }

    /**
     * Sets the new current index status and adds to {@link #statusChanges()}.
     *
     * @param newStatus New current status of the index.
     * @param catalogVersion Catalog version in which the new index status appeared.
     * @param activationTs Activation timestamp of the catalog version in which the new status appeared.
     * @return New instance of the index meta with a change in the current status and status history updates.
     * @see Catalog#time()
     */
    IndexMeta status(MetaIndexStatus newStatus, int catalogVersion, long activationTs) {
        assert !statusChanges.containsKey(newStatus) : "newStatus=" + newStatus + ", catalogVersion=" + catalogVersion;

        var newStatuses = new EnumMap<>(statusChanges);
        newStatuses.put(newStatus, new MetaIndexStatusChange(catalogVersion, activationTs));

        return new IndexMeta(catalogVersion, indexId, tableId, indexName, newStatus, newStatuses);
    }

    /** Returns a map of index statuses with change info (for example catalog version) in which they appeared. */
    public Map<MetaIndexStatus, MetaIndexStatusChange> statusChanges() {
        return statusChanges;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
