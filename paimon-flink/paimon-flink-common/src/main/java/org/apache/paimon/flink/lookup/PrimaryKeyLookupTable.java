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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBValueState;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 用于根据主键 lookup 主键表.
 *
 * <p>join key 和 主键表的主键完全相同（顺序不要求）.
 *
 * <p>A {@link LookupTable} for primary key table.
 */
public class PrimaryKeyLookupTable extends FullCacheLookupTable {

    protected final long lruCacheSize;

    // 用于获取 key
    protected final KeyProjectedRow primaryKeyRow;

    @Nullable private final ProjectedRow keyRearrange;

    protected RocksDBValueState<InternalRow, InternalRow> tableState;

    public PrimaryKeyLookupTable(Context context, long lruCacheSize, List<String> joinKey) {
        super(context);
        this.lruCacheSize = lruCacheSize;
        List<String> fieldNames = projectedType.getFieldNames();
        FileStoreTable table = context.table;
        int[] primaryKeyMapping =
                table.primaryKeys().stream().mapToInt(fieldNames::indexOf).toArray();
        this.primaryKeyRow = new KeyProjectedRow(primaryKeyMapping);

        // 用于获取 input 中的 KEY 值
        ProjectedRow keyRearrange = null;
        if (!table.primaryKeys().equals(joinKey)) {
            keyRearrange =
                    ProjectedRow.from(
                            table.primaryKeys().stream()
                                    .map(joinKey::indexOf)
                                    .mapToInt(value -> value)
                                    .toArray());
        }
        this.keyRearrange = keyRearrange;
    }

    @Override
    public void open() throws Exception {
        openStateFactory();
        createTableState();
        bootstrap();
    }

    protected void createTableState() throws IOException {
        // K-V 存储
        this.tableState =
                stateFactory.valueState(
                        "table", // column family
                        InternalSerializers.create(
                                TypeUtils.project(
                                        projectedType,
                                        primaryKeyRow.indexMapping())), // key serializer
                        InternalSerializers.create(projectedType), // value serializer
                        lruCacheSize);
    }

    @Override
    public List<InternalRow> innerGet(InternalRow key) throws IOException {
        // 将 input 的 key 转换为维表 的 key
        if (keyRearrange != null) {
            key = keyRearrange.replaceRow(key);
        }
        InternalRow value = tableState.get(key);
        // 返回空或单值集合
        return value == null ? Collections.emptyList() : Collections.singletonList(value);
    }

    @Override
    public void refresh(Iterator<InternalRow> incremental) throws IOException {
        Predicate predicate = projectedPredicate();
        while (incremental.hasNext()) {
            InternalRow row = incremental.next();
            primaryKeyRow.replaceRow(row);
            if (userDefinedSeqComparator != null) {
                InternalRow previous = tableState.get(primaryKeyRow);
                // 变更后 sequence field 大才需要更新本地数据
                if (previous != null && userDefinedSeqComparator.compare(previous, row) > 0) {
                    continue;
                }
            }

            if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
                if (predicate == null || predicate.test(row)) {
                    tableState.put(primaryKeyRow, row);
                } else {
                    // 更新后的数据不符合过滤条件，则需要从本地删除
                    // The new record under primary key is filtered
                    // We need to delete this primary key as it no longer exists.
                    tableState.delete(primaryKeyRow);
                }
            } else {
                tableState.delete(primaryKeyRow);
            }
        }
    }

    @Override
    public byte[] toKeyBytes(InternalRow row) throws IOException {
        // 序列化 KEY
        primaryKeyRow.replaceRow(row);
        return tableState.serializeKey(primaryKeyRow);
    }

    @Override
    public byte[] toValueBytes(InternalRow row) throws IOException {
        // 序列化 VALUE
        return tableState.serializeValue(row);
    }

    // 用于将数据批量导入 rocksdb.
    @Override
    public TableBulkLoader createBulkLoader() {
        BulkLoader bulkLoader = tableState.createBulkLoader();
        return new TableBulkLoader() {

            @Override
            public void write(byte[] key, byte[] value)
                    throws BulkLoader.WriteException, IOException {
                bulkLoader.write(key, value);
                bulkLoadWritePlus(key, value);
            }

            @Override
            public void finish() {
                bulkLoader.finish();
            }
        };
    }

    public void bulkLoadWritePlus(byte[] key, byte[] value) throws IOException {}
}
