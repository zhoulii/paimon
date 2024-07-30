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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.query.RemoteTableQuery;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.SCAN_BOUNDED_WATERMARK;
import static org.apache.paimon.CoreOptions.STREAM_SCAN_MODE;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;

/**
 * 直接读取 LSM 数据（一个 bucket 数据），底层原理是把 bucket 数据拉到本地，然后转换成 KV Store 文件存储. 之后就可以根据 KEY 查询 VALUE.
 *
 * <p>Lookup table for primary key which supports to read the LSM tree directly.
 */
public class PrimaryKeyPartialLookupTable implements LookupTable {

    // 用于创建 QueryExecutor.
    private final Function<Predicate, QueryExecutor> executorFactory;

    // 用于从 primary key 中提取一些有用信息
    private final FixedBucketFromPkExtractor extractor;

    // 对 KEY 进行重新排序
    @Nullable private final ProjectedRow keyRearrange;

    // 对 TRIMMED KEY 进行重新排序
    @Nullable private final ProjectedRow trimmedKeyRearrange;

    // 根据分区过滤
    private Predicate specificPartition;

    // 用于执行 QUERY
    private QueryExecutor queryExecutor;

    private PrimaryKeyPartialLookupTable(
            Function<Predicate, QueryExecutor> executorFactory,
            FileStoreTable table,
            List<String> joinKey) {
        this.executorFactory = executorFactory;

        // 只支持固定 bucket 的模式
        if (table.bucketMode() != BucketMode.FIXED) {
            throw new UnsupportedOperationException(
                    "Unsupported mode for partial lookup: " + table.bucketMode());
        }

        // 创建 extractor
        this.extractor = new FixedBucketFromPkExtractor(table.schema());

        ProjectedRow keyRearrange = null;

        // 表的主键和关联的主键不相等，获取主键在 join key 中的索引
        // join key 表示左表关联的 KEY
        // join key 表示的 row 是从左表传递来的数据
        // 这里做 rearrange 是为了从左表数据中提取关联表的 KEY
        if (!table.primaryKeys().equals(joinKey)) {
            keyRearrange =
                    ProjectedRow.from(
                            table.primaryKeys().stream()
                                    .map(joinKey::indexOf)
                                    .mapToInt(value -> value)
                                    .toArray());
        }
        this.keyRearrange = keyRearrange;

        List<String> trimmedPrimaryKeys = table.schema().trimmedPrimaryKeys();
        ProjectedRow trimmedKeyRearrange = null;
        // 表的 trimmed primary key 和关联的主键不相等，获取主键在 join key 中的索引
        if (!trimmedPrimaryKeys.equals(joinKey)) {
            trimmedKeyRearrange =
                    ProjectedRow.from(
                            trimmedPrimaryKeys.stream()
                                    .map(joinKey::indexOf)
                                    .mapToInt(value -> value)
                                    .toArray());
        }
        this.trimmedKeyRearrange = trimmedKeyRearrange;
    }

    @VisibleForTesting
    QueryExecutor queryExecutor() {
        return queryExecutor;
    }

    @Override
    public void specificPartitionFilter(Predicate filter) {
        // 指定分区过滤
        this.specificPartition = filter;
    }

    @Override
    public void open() throws Exception {
        // 创建 QueryExecutor
        this.queryExecutor = executorFactory.apply(specificPartition);
        refresh();
    }

    @Override
    public List<InternalRow> get(InternalRow key) throws IOException {
        // key 是左表用来关联的 KEY，转换后的 adjustedKey 则可以视为右表 primary key
        InternalRow adjustedKey = key;
        if (keyRearrange != null) {
            adjustedKey = keyRearrange.replaceRow(adjustedKey);
        }
        extractor.setRecord(adjustedKey);
        // 提取 bucket
        int bucket = extractor.bucket();
        // 提取 partition
        BinaryRow partition = extractor.partition();

        InternalRow trimmedKey = key;
        // 从左表的 KEY 中提取 TRIMMED KEY
        if (trimmedKeyRearrange != null) {
            trimmedKey = trimmedKeyRearrange.replaceRow(trimmedKey);
        }

        // 查询结果
        InternalRow kv = queryExecutor.lookup(partition, bucket, trimmedKey);
        if (kv == null) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(kv);
        }
    }

    @Override
    public void refresh() {
        // 委托给 QueryExecutor 来执行
        queryExecutor.refresh();
    }

    @Override
    public void close() throws IOException {
        if (queryExecutor != null) {
            queryExecutor.close();
        }
    }

    public static PrimaryKeyPartialLookupTable createLocalTable(
            FileStoreTable table, int[] projection, File tempPath, List<String> joinKey) {
        // 查询本地文件
        return new PrimaryKeyPartialLookupTable(
                filter -> new LocalQueryExecutor(table, projection, tempPath, filter),
                table,
                joinKey);
    }

    public static PrimaryKeyPartialLookupTable createRemoteTable(
            FileStoreTable table, int[] projection, List<String> joinKey) {
        // 查询远程服务
        return new PrimaryKeyPartialLookupTable(
                filter -> new RemoteQueryExecutor(table, projection), table, joinKey);
    }

    // 查询的执行器接口，有两个实现类，分别表示从本地查询、从远程查询.
    interface QueryExecutor extends Closeable {
        // 执行查询
        InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException;

        // 刷新数据，本地查询会更新数据，远程查询什么都不用做
        void refresh();
    }

    static class LocalQueryExecutor implements QueryExecutor {

        private final LocalTableQuery tableQuery;
        private final StreamTableScan scan;

        private LocalQueryExecutor(
                FileStoreTable table, int[] projection, File tempPath, @Nullable Predicate filter) {
            // 创建某个表的 LocalTableQuery
            this.tableQuery =
                    table.newLocalTableQuery()
                            .withValueProjection(Projection.of(projection).toNestedIndexes())
                            .withIOManager(new IOManagerImpl(tempPath.toString()));

            Map<String, String> dynamicOptions = new HashMap<>();
            // 监控文件变化，有变化则更新表数据
            dynamicOptions.put(STREAM_SCAN_MODE.key(), FILE_MONITOR.getValue());
            // 当 watermark 大于某个值时，则停止 scan，设置为 null 则表示不限制
            dynamicOptions.put(SCAN_BOUNDED_WATERMARK.key(), null);

            // 创建 stream scan
            this.scan =
                    table.copy(dynamicOptions).newReadBuilder().withFilter(filter).newStreamScan();
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            // 委托给 TableQuery 来执行
            return tableQuery.lookup(partition, bucket, key);
        }

        @Override
        public void refresh() {
            while (true) {
                // 第一次获取所有 Split，之后获取增量 Split
                List<Split> splits = scan.plan().splits();
                if (splits.isEmpty()) {
                    return;
                }

                for (Split split : splits) {
                    if (!(split instanceof DataSplit)) {
                        throw new IllegalArgumentException(
                                "Unsupported split: " + split.getClass());
                    }
                    BinaryRow partition = ((DataSplit) split).partition();
                    int bucket = ((DataSplit) split).bucket();
                    // bucket 中删除的文件
                    List<DataFileMeta> before = ((DataSplit) split).beforeFiles();
                    // bucket 中新增的文件
                    List<DataFileMeta> after = ((DataSplit) split).dataFiles();
                    // 更新 LookupLevels
                    tableQuery.refreshFiles(partition, bucket, before, after);
                }
            }
        }

        @Override
        public void close() throws IOException {
            tableQuery.close();
        }
    }

    static class RemoteQueryExecutor implements QueryExecutor {

        private final RemoteTableQuery tableQuery;

        private RemoteQueryExecutor(FileStoreTable table, int[] projection) {
            this.tableQuery = new RemoteTableQuery(table).withValueProjection(projection);
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            return tableQuery.lookup(partition, bucket, key);
        }

        @Override
        public void refresh() {
            // 什么都不需要做
        }

        @Override
        public void close() throws IOException {
            tableQuery.close();
        }
    }
}
