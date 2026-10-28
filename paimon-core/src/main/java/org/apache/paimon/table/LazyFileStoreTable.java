/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A planning table whose metadata is available without initializing the runtime table.
 *
 * <p>Runtime operations use the captured schema and catalog environment, never a fresh catalog
 * lookup. Only the planning catalog entry point returns this wrapper; SDK table loading remains
 * eager.
 *
 * <p>Schema, options, identity, location, bucket mode and cache configuration do not initialize the
 * runtime table. Both dynamic-option copy methods defer initialization: {@link #copy(Map)} resolves
 * a requested historical schema during planning, while {@link #copyWithoutTimeTravel(Map)} retains
 * the current schema. Other operations, including schema replacement, branch switching and
 * refreshing the schema, delegate to the validated runtime table.
 */
public final class LazyFileStoreTable extends DelegatedFileStoreTable {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> dynamicOptions;
    private final TableSchema planningSchema;

    private transient FileStoreTable runtimeTable;
    private transient Cache<Path, Snapshot> snapshotCache;
    private transient Cache<String, Statistics> statsCache;
    private transient SegmentsCache<Path> manifestCache;
    private transient SegmentsCache<Path> manifestSidecarCache;
    private transient DVMetaCache dvMetaCache;

    public LazyFileStoreTable(
            FileIO fileIO, Path path, TableSchema schema, CatalogEnvironment environment) {
        this(metadataTable(fileIO, path, schema, environment), Collections.emptyMap());
    }

    private LazyFileStoreTable(
            AbstractFileStoreTable metadataTable, Map<String, String> dynamicOptions) {
        this(metadataTable, dynamicOptions, true);
    }

    private LazyFileStoreTable(
            AbstractFileStoreTable metadataTable,
            Map<String, String> dynamicOptions,
            boolean tryTimeTravel) {
        super(metadataTable);
        this.dynamicOptions = new HashMap<>(dynamicOptions);
        this.planningSchema = metadataTable.copySchema(dynamicOptions, tryTimeTravel);
    }

    private static AbstractFileStoreTable metadataTable(
            FileIO fileIO, Path path, TableSchema schema, CatalogEnvironment environment) {
        return schema.primaryKeys().isEmpty()
                ? new AppendOnlyFileStoreTable(fileIO, path, schema, environment)
                : new PrimaryKeyFileStoreTable(fileIO, path, schema, environment);
    }

    @Override
    public synchronized FileStoreTable wrapped() {
        if (runtimeTable == null) {
            FileStoreTable table =
                    FileStoreTableFactory.createWithBranches(
                            ((AbstractFileStoreTable) wrapped)
                                    .copyWithSchema(planningSchema, dynamicOptions),
                            fileIO(),
                            location(),
                            Options.fromMap(dynamicOptions),
                            catalogEnvironment());
            configureCaches(table);
            runtimeTable = table;
        }
        return runtimeTable;
    }

    private void configureCaches(FileStoreTable table) {
        if (snapshotCache != null) {
            table.setSnapshotCache(snapshotCache);
        }
        if (statsCache != null) {
            table.setStatsCache(statsCache);
        }
        if (manifestCache != null) {
            table.setManifestCache(manifestCache);
        }
        if (manifestSidecarCache != null) {
            table.setManifestSidecarCache(manifestSidecarCache);
        }
        if (dvMetaCache != null) {
            table.setDVMetaCache(dvMetaCache);
        }
    }

    @Override
    public String name() {
        return wrapped.name();
    }

    @Override
    public String fullName() {
        return wrapped.fullName();
    }

    @Override
    public String uuid() {
        return wrapped.uuid();
    }

    @Override
    public TableSchema schema() {
        return planningSchema;
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    @Override
    public Path location() {
        return wrapped.location();
    }

    @Override
    public CatalogEnvironment catalogEnvironment() {
        return wrapped.catalogEnvironment();
    }

    @Override
    public CoreOptions coreOptions() {
        return new CoreOptions(options());
    }

    @Override
    public BucketMode bucketMode() {
        return BucketMode.fromOptions(
                schema().numBuckets(), !primaryKeys().isEmpty(), schema().crossPartitionUpdate());
    }

    @Override
    public FileStoreTable copy(Map<String, String> options) {
        metadataTable(fileIO(), location(), schema(), catalogEnvironment())
                .checkImmutability(options);
        Map<String, String> merged = new HashMap<>(dynamicOptions);
        merged.putAll(options);
        LazyFileStoreTable copied =
                new LazyFileStoreTable((AbstractFileStoreTable) wrapped, merged);
        configureCaches(copied);
        return copied;
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> options) {
        AbstractFileStoreTable metadata =
                metadataTable(fileIO(), location(), schema(), catalogEnvironment());
        metadata.checkImmutability(options);
        Map<String, String> merged = new HashMap<>(dynamicOptions);
        merged.putAll(options);
        LazyFileStoreTable copied = new LazyFileStoreTable(metadata, merged, false);
        configureCaches(copied);
        return copied;
    }

    @Override
    public FileStoreTable copy(TableSchema schema) {
        return wrapped().copy(schema);
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return wrapped().copyWithLatestSchema();
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return wrapped().switchToBranch(branchName);
    }

    @Override
    public synchronized void setSnapshotCache(Cache<Path, Snapshot> cache) {
        snapshotCache = cache;
        if (runtimeTable != null) {
            runtimeTable.setSnapshotCache(cache);
        }
    }

    @Override
    public synchronized void setStatsCache(Cache<String, Statistics> cache) {
        statsCache = cache;
        if (runtimeTable != null) {
            runtimeTable.setStatsCache(cache);
        }
    }

    @Override
    public synchronized void setManifestCache(SegmentsCache<Path> cache) {
        manifestCache = cache;
        if (runtimeTable != null) {
            runtimeTable.setManifestCache(cache);
        }
    }

    @Override
    public SegmentsCache<Path> getManifestCache() {
        return manifestCache;
    }

    @Override
    public synchronized void setManifestSidecarCache(SegmentsCache<Path> cache) {
        manifestSidecarCache = cache;
        if (runtimeTable != null) {
            runtimeTable.setManifestSidecarCache(cache);
        }
    }

    @Override
    public SegmentsCache<Path> getManifestSidecarCache() {
        return manifestSidecarCache;
    }

    @Override
    public synchronized void setDVMetaCache(DVMetaCache cache) {
        dvMetaCache = cache;
        if (runtimeTable != null) {
            runtimeTable.setDVMetaCache(cache);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LazyFileStoreTable)) {
            return false;
        }
        LazyFileStoreTable that = (LazyFileStoreTable) other;
        return wrapped.equals(that.wrapped)
                && planningSchema.equals(that.planningSchema)
                && dynamicOptions.equals(that.dynamicOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrapped, planningSchema, dynamicOptions);
    }
}
