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

package org.apache.paimon.flink;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.DefaultCatalogMaterializedTable;
import org.apache.flink.table.catalog.IntervalFreshness;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link DefaultCatalogMaterializedTable} to wrap {@link FileStoreTable}. */
public class DataCatalogMaterializedTable extends DefaultCatalogMaterializedTable
        implements PaimonTableProvider {

    private final Table table;
    private final TableSchema tableSchema;

    public DataCatalogMaterializedTable(
            Table table,
            TableSchema tableSchema,
            String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            Long snapshot,
            String definitionQuery,
            IntervalFreshness freshness,
            LogicalRefreshMode logicalRefreshMode,
            RefreshMode refreshMode,
            RefreshStatus refreshStatus,
            @Nullable String refreshHandlerDescription,
            @Nullable byte[] serializedRefreshHandler) {
        super(
                tableSchema.toSchema(),
                comment,
                partitionKeys,
                options,
                snapshot,
                definitionQuery,
                freshness,
                logicalRefreshMode,
                refreshMode,
                refreshStatus,
                refreshHandlerDescription,
                serializedRefreshHandler);
        this.table = table;
        this.tableSchema = tableSchema;
    }

    @Override
    public Table table() {
        return table;
    }

    @Override
    public Schema getUnresolvedSchema() {
        return tableSchema.toSchema();
    }

    @Override
    public TableSchema getSchema() {
        return this.tableSchema;
    }

    @Override
    public CatalogBaseTable copy() {
        return new DataCatalogMaterializedTable(
                table,
                tableSchema.copy(),
                getComment(),
                new ArrayList<>(getPartitionKeys()),
                new HashMap<>(getOptions()),
                getSnapshot().get(),
                getDefinitionQuery(),
                IntervalFreshness.ofSecond(getFreshness().toMillis() / 1000 + ""),
                getLogicalRefreshMode(),
                getRefreshMode(),
                getRefreshStatus(),
                getRefreshHandlerDescription().get(),
                getSerializedRefreshHandler());
    }

    @Override
    public CatalogMaterializedTable copy(Map<String, String> options) {
        return new DataCatalogMaterializedTable(
                table,
                tableSchema.copy(),
                getComment(),
                new ArrayList<>(getPartitionKeys()),
                new HashMap<>(options),
                getSnapshot().get(),
                getDefinitionQuery(),
                IntervalFreshness.ofSecond(getFreshness().toMillis() / 1000 + ""),
                getLogicalRefreshMode(),
                getRefreshMode(),
                getRefreshStatus(),
                getRefreshHandlerDescription().get(),
                getSerializedRefreshHandler());
    }
}
