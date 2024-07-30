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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBState;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.PartialRow;
import org.apache.paimon.utils.TypeUtils;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 完全缓存维表数据到本地，使用 Rocksdb 存储.
 *
 * <p>Lookup table of full cache.
 */
public abstract class FullCacheLookupTable implements LookupTable {

    protected final Context context;
    protected final RowType projectedType;

    @Nullable protected final FieldsComparator userDefinedSeqComparator;

    // 添加到 projection 中 sequence field 的数量
    protected final int appendUdsFieldNumber;

    protected RocksDBStateFactory stateFactory;
    private LookupStreamingReader reader;
    private Predicate specificPartition;

    public FullCacheLookupTable(Context context) {
        FileStoreTable table = context.table;
        List<String> sequenceFields = new ArrayList<>();
        if (table.primaryKeys().size() > 0) {
            // sequence field 在主键表下才有效
            sequenceFields = new CoreOptions(table.options()).sequenceField();
        }
        RowType projectedType = TypeUtils.project(table.rowType(), context.projection);
        if (sequenceFields.size() > 0) {
            RowType.Builder builder = RowType.builder();
            projectedType.getFields().forEach(f -> builder.field(f.name(), f.type()));
            RowType rowType = table.rowType();
            AtomicInteger appendUdsFieldNumber = new AtomicInteger(0);
            // 如果 sequence field 没被投影，则添加
            sequenceFields.stream()
                    .filter(projectedType::notContainsField)
                    .map(rowType::getField)
                    .forEach(
                            f -> {
                                appendUdsFieldNumber.incrementAndGet();
                                builder.field(f.name(), f.type());
                            });
            projectedType = builder.build();
            // 修改 context 中关于维表的 projection 信息
            context = context.copy(table.rowType().getFieldIndices(projectedType.getFieldNames()));
            // 创建 sequence field 的比较器
            this.userDefinedSeqComparator =
                    UserDefinedSeqComparator.create(projectedType, sequenceFields);
            this.appendUdsFieldNumber = appendUdsFieldNumber.get();
        } else {
            this.userDefinedSeqComparator = null;
            this.appendUdsFieldNumber = 0;
        }

        this.context = context;
        this.projectedType = projectedType;
    }

    @Override
    public void specificPartitionFilter(Predicate filter) {
        this.specificPartition = filter;
    }

    protected void openStateFactory() throws Exception {
        // 维表数据本地存储方案，full cache 只支持 rocksdb
        this.stateFactory =
                new RocksDBStateFactory(
                        context.tempPath.toString(),
                        context.table.coreOptions().toConfiguration(),
                        null);
    }

    protected void bootstrap() throws Exception {
        // scan 过滤条件
        Predicate scanPredicate =
                PredicateBuilder.andNullable(context.tablePredicate, specificPartition);
        // 能用于获取 RecordReader 来读取数据
        this.reader = new LookupStreamingReader(context.table, context.projection, scanPredicate);
        // 用于将 table 数据全局排序
        BinaryExternalSortBuffer bulkLoadSorter =
                RocksDBState.createBulkLoadSorter(
                        IOManager.create(context.tempPath.toString()), context.table.coreOptions());
        // 读取过滤条件
        Predicate predicate = projectedPredicate();
        try (RecordReaderIterator<InternalRow> batch =
                new RecordReaderIterator<>(reader.nextBatch(true))) {
            while (batch.hasNext()) {
                InternalRow row = batch.next();
                // 没有过滤条件或过滤通过
                if (predicate == null || predicate.test(row)) {
                    // 写入 key 和 value 到 sorter
                    bulkLoadSorter.write(GenericRow.of(toKeyBytes(row), toValueBytes(row)));
                }
            }
        }

        MutableObjectIterator<BinaryRow> keyIterator = bulkLoadSorter.sortedIterator();
        // row 有两个字段：KEY + VALUE
        BinaryRow row = new BinaryRow(2);
        TableBulkLoader bulkLoader = createBulkLoader();
        try {
            while ((row = keyIterator.next(row)) != null) {
                // bulk load 将数据导入到 rocksdb
                bulkLoader.write(row.getBinary(0), row.getBinary(1));
            }
        } catch (BulkLoader.WriteException e) {
            throw new RuntimeException(
                    "Exception in bulkLoad, the most suspicious reason is that "
                            + "your data contains duplicates, please check your lookup table. ",
                    e.getCause());
        }

        bulkLoader.finish();
        bulkLoadSorter.clear();
    }

    @Override
    public void refresh() throws Exception {
        // 更新缓存数据
        while (true) {
            try (RecordReaderIterator<InternalRow> batch =
                    new RecordReaderIterator<>(reader.nextBatch(false))) {
                if (!batch.hasNext()) {
                    return;
                }
                refresh(batch);
            }
        }
    }

    @Override
    public final List<InternalRow> get(InternalRow key) throws IOException {
        List<InternalRow> values = innerGet(key);
        if (appendUdsFieldNumber == 0) {
            return values;
        }

        // 删除之前添加到 projection 中的缺失的 sequence field
        // 返回真正需要的 projection 数据
        List<InternalRow> dropSequence = new ArrayList<>(values.size());
        for (InternalRow matchedRow : values) {
            dropSequence.add(
                    new PartialRow(matchedRow.getFieldCount() - appendUdsFieldNumber, matchedRow));
        }
        return dropSequence;
    }

    public abstract List<InternalRow> innerGet(InternalRow key) throws IOException;

    public abstract void refresh(Iterator<InternalRow> input) throws IOException;

    @Nullable
    public Predicate projectedPredicate() {
        return context.projectedPredicate;
    }

    public abstract byte[] toKeyBytes(InternalRow row) throws IOException;

    public abstract byte[] toValueBytes(InternalRow row) throws IOException;

    public abstract TableBulkLoader createBulkLoader();

    @Override
    public void close() throws IOException {
        // 关闭 rocksdb 实例
        stateFactory.close();
        // 删除临时目录
        FileIOUtils.deleteDirectory(context.tempPath);
    }

    /**
     * 批量加载 Paimon 数据为 Rocksdb 数据.
     *
     * <p>Bulk loader for the table.
     */
    public interface TableBulkLoader {

        // 写出 paimon 数据
        void write(byte[] key, byte[] value) throws BulkLoader.WriteException, IOException;

        // 完成批量加载
        void finish() throws IOException;
    }

    static FullCacheLookupTable create(Context context, long lruCacheSize) {
        List<String> primaryKeys = context.table.primaryKeys();
        if (primaryKeys.isEmpty()) {
            // lookup join 非主键表
            return new NoPrimaryKeyLookupTable(context, lruCacheSize);
        } else {
            if (new HashSet<>(primaryKeys).equals(new HashSet<>(context.joinKey))) {
                // join 主键表，primary key 和 join key 相同
                return new PrimaryKeyLookupTable(context, lruCacheSize, context.joinKey);
            } else {
                // lookup join 主键表，primary key 和 join key 不同
                return new SecondaryIndexLookupTable(context, lruCacheSize);
            }
        }
    }

    /**
     * lookup 的上下文信息.
     *
     * <p>Context 类用于传递信息，比如写阶段一些元数据信息保存为 Context，然后在读阶段使用.
     *
     * <p>Context for {@link LookupTable}.
     */
    public static class Context {

        public final FileStoreTable table;
        public final int[] projection;
        @Nullable public final Predicate tablePredicate;
        // 裁剪后的数据过滤条件
        @Nullable public final Predicate projectedPredicate;
        public final File tempPath;
        public final List<String> joinKey;

        public Context(
                FileStoreTable table,
                int[] projection,
                @Nullable Predicate tablePredicate,
                @Nullable Predicate projectedPredicate,
                File tempPath,
                List<String> joinKey) {
            this.table = table;
            this.projection = projection;
            this.tablePredicate = tablePredicate;
            this.projectedPredicate = projectedPredicate;
            this.tempPath = tempPath;
            this.joinKey = joinKey;
        }

        public Context copy(int[] newProjection) {
            return new Context(
                    table, newProjection, tablePredicate, projectedPredicate, tempPath, joinKey);
        }
    }
}
