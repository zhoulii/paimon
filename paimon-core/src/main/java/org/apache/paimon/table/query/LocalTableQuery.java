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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.hash.HashLookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;

/** Implementation for {@link TableQuery} for caching data and file in local. */
public class LocalTableQuery implements TableQuery {

    // partition -> <bucket -> LookupLevels>
    private final Map<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> tableView;

    private final CoreOptions options;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    // 读取 KeyValue files.
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    // KV 存储读写优化
    private final HashLookupStoreFactory hashLookupStoreFactory;

    // lookup 的起始层
    private final int startLevel;

    private IOManager ioManager;

    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        this.tableView = new HashMap<>();
        FileStore<?> tableStore = table.store();
        // 只支持 PK 表，PK 表使用的是 KeyValueFileStore
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());
        this.hashLookupStoreFactory =
                new HashLookupStoreFactory(
                        new CacheManager(options.lookupCacheMaxMemory()),
                        options.cachePageSize(),
                        options.toConfiguration().get(CoreOptions.LOOKUP_HASH_LOAD_FACTOR),
                        options.toConfiguration().get(CoreOptions.LOOKUP_CACHE_SPILL_COMPRESSION));

        // 满足条件时，compaction 阶段强制执行所有 level-0 的文件合并
        // 可以理解成就没有 level-0
        if (options.needLookup()) {
            startLevel = 1;
        } else {
            // 不能指定 sequence field
            // 考虑这种情况：level-0 存储的某个 key sequence field 比 level-1 中相同 key 的 sequence field 更小，这种情况下
            // level-0 中的这个 KEY 是不应该被 lookup 出来的
            // 所以要求不能指定 sequence field，只能是去重 merge function，这样 lookup 出来的数据就是需要的数据
            if (options.sequenceField().size() > 0) {
                throw new UnsupportedOperationException(
                        "Not support sequence field definition, but is: "
                                + options.sequenceField());
            }

            // 只能使用去重的 merge-engine
            if (options.mergeEngine() != DEDUPLICATE) {
                throw new UnsupportedOperationException(
                        "Only support deduplicate merge engine, but is: " + options.mergeEngine());
            }

            startLevel = 0;
        }
    }

    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        // bucket 中删除哪些文件、新增哪些文件
        LookupLevels<KeyValue> lookupLevels =
                tableView.computeIfAbsent(partition, k -> new HashMap<>()).get(bucket);
        if (lookupLevels == null) {
            // bucket 第一次添加进来，删除的文件为空
            Preconditions.checkArgument(
                    beforeFiles.isEmpty(),
                    "The before file should be empty for the initial phase.");
            // 创建 LookupLevels，每个 bucket 对应一个 LookupLevels
            newLookupLevels(partition, bucket, dataFiles);
        } else {
            // 更新文件
            lookupLevels.getLevels().update(beforeFiles, dataFiles);
        }
    }

    private void newLookupLevels(BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        // 创建 Levels，本质上是划分 SortedRun
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        // TODO pass DeletionVector factory
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        Options options = this.options.toConfiguration();
        LookupLevels<KeyValue> lookupLevels =
                new LookupLevels<>(
                        levels,
                        keyComparatorSupplier.get(),
                        readerFactoryBuilder.keyType(),
                        // lookup 读写 KeyValue 文件，使用 KeyValueProcessor
                        new LookupLevels.KeyValueProcessor(
                                readerFactoryBuilder.projectedValueType()),
                        // IOFunction，用于创建 RecordReader
                        file ->
                                factory.createRecordReader(
                                        file.schemaId(),
                                        file.fileName(),
                                        file.fileSize(),
                                        file.level()),
                        // 创建文件
                        () ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel()
                                        .getPathFile(),
                        hashLookupStoreFactory,
                        // 文件缓存时间
                        options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                        // 最大磁盘缓存大小
                        options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE),
                        // 用于创建 BloomFilter
                        bfGenerator(options));

        // 存储到表视图
        tableView.computeIfAbsent(partition, k -> new HashMap<>()).put(bucket, lookupLevels);
    }

    /** TODO remove synchronized and supports multiple thread to lookup. */
    @Nullable
    @Override
    public synchronized InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        Map<Integer, LookupLevels<KeyValue>> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null;
        }

        KeyValue kv = lookupLevels.lookup(key, startLevel);
        if (kv == null || kv.valueKind().isRetract()) {
            return null;
        } else {
            return kv.value();
        }
    }

    @Override
    public LocalTableQuery withValueProjection(int[][] projection) {
        // 设置 value 的 projection
        this.readerFactoryBuilder.withValueProjection(projection);
        return this;
    }

    public LocalTableQuery withIOManager(IOManager ioManager) {
        // 指定 IOManager，执行 DISK IO 操作
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        // 创建 value 字段序列化器
        return InternalSerializers.create(readerFactoryBuilder.projectedValueType());
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> buckets :
                tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels<KeyValue>> bucket :
                    buckets.getValue().entrySet()) {
                // 关闭所有 LookupLevels，主要就是清空文件缓存，删除文件
                bucket.getValue().close();
            }
        }
        tableView.clear();
    }
}
