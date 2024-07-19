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

package org.apache.paimon.format.orc;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.OrcFormatReaderContext;
import org.apache.paimon.format.fs.HadoopReadOnlyFileSystem;
import org.apache.paimon.format.orc.filter.OrcFilters;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Pool;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.format.orc.reader.AbstractOrcColumnVector.createPaimonVector;
import static org.apache.paimon.format.orc.reader.OrcSplitReaderUtil.toOrcType;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 创建读取 orc 格式的 RecordReader. 返回 ColumnarRow.
 *
 * <p>An ORC reader that produces a stream of {@link ColumnarRow} records.
 */
public class OrcReaderFactory implements FormatReaderFactory {

    private static final long serialVersionUID = 1L;

    protected final SerializableHadoopConfigWrapper hadoopConfigWrapper;

    // TypeDescription 描述 orc 文件中的类型
    protected final TypeDescription schema;

    // 表的字段类型
    private final RowType tableType;

    // 筛选条件
    protected final List<OrcFilters.Predicate> conjunctPredicates;

    // orc reader 读取的批大小
    protected final int batchSize;

    /**
     * @param hadoopConfig the hadoop config for orc reader.
     * @param conjunctPredicates the filter predicates that can be evaluated.
     * @param batchSize the batch size of orc reader.
     */
    public OrcReaderFactory(
            final org.apache.hadoop.conf.Configuration hadoopConfig,
            final RowType readType,
            final List<OrcFilters.Predicate> conjunctPredicates,
            final int batchSize) {
        this.hadoopConfigWrapper = new SerializableHadoopConfigWrapper(checkNotNull(hadoopConfig));
        this.schema = toOrcType(readType);
        this.tableType = readType;
        this.conjunctPredicates = checkNotNull(conjunctPredicates);
        this.batchSize = batchSize;
    }

    // ------------------------------------------------------------------------

    @Override
    public OrcVectorizedReader createReader(FormatReaderFactory.Context context)
            throws IOException {
        int poolSize =
                context instanceof OrcFormatReaderContext
                        ? ((OrcFormatReaderContext) context).poolSize()
                        : 1;
        // Pool 里可以存储多少 OrcReaderBatch
        Pool<OrcReaderBatch> poolOfBatches = createPoolOfBatches(context.filePath(), poolSize);

        RecordReader orcReader =
                createRecordReader(
                        hadoopConfigWrapper.getHadoopConfig(),
                        schema,
                        conjunctPredicates,
                        context.fileIO(),
                        context.filePath(),
                        0,
                        context.fileSize());
        return new OrcVectorizedReader(orcReader, poolOfBatches);
    }

    /**
     * 将 VectorizedRowBatch 转换为 VectorizedColumnBatch，然后创建 OrcReaderBatch.
     *
     * <p>Creates the {@link OrcReaderBatch} structure, which is responsible for holding the data
     * structures that hold the batch data (column vectors, row arrays, ...) and the batch
     * conversion from the ORC representation to the result format.
     */
    public OrcReaderBatch createReaderBatch(
            Path filePath, VectorizedRowBatch orcBatch, Pool.Recycler<OrcReaderBatch> recycler) {
        List<String> tableFieldNames = tableType.getFieldNames();
        List<DataType> tableFieldTypes = tableType.getFieldTypes();

        // create and initialize the row batch
        // 创建 ColumnVector，最终为了创建 VectorizedColumnBatch.
        ColumnVector[] vectors = new ColumnVector[tableType.getFieldCount()];
        for (int i = 0; i < vectors.length; i++) {
            String name = tableFieldNames.get(i);
            DataType type = tableFieldTypes.get(i);
            vectors[i] = createPaimonVector(orcBatch.cols[tableFieldNames.indexOf(name)], type);
        }
        return new OrcReaderBatch(filePath, orcBatch, new VectorizedColumnBatch(vectors), recycler);
    }

    // ------------------------------------------------------------------------

    private Pool<OrcReaderBatch> createPoolOfBatches(Path filePath, int numBatches) {
        final Pool<OrcReaderBatch> pool = new Pool<>(numBatches);

        for (int i = 0; i < numBatches; i++) {
            // VectorizedRowBatch 是个 Row 集合，但是按列存储.
            // batchSize / numBatches 作为 VectorizedRowBatch 批次大小，相当于将一个大批次转成多个小批次
            final VectorizedRowBatch orcBatch = createBatchWrapper(schema, batchSize / numBatches);
            final OrcReaderBatch batch = createReaderBatch(filePath, orcBatch, pool.recycler());
            pool.add(batch);
        }

        return pool;
    }

    // ------------------------------------------------------------------------

    private static class OrcReaderBatch {

        private final VectorizedRowBatch orcVectorizedRowBatch;
        private final Pool.Recycler<OrcReaderBatch> recycler;

        private final VectorizedColumnBatch paimonColumnBatch;
        private final ColumnarRowIterator result;

        protected OrcReaderBatch(
                final Path filePath,
                final VectorizedRowBatch orcVectorizedRowBatch,
                final VectorizedColumnBatch paimonColumnBatch,
                final Pool.Recycler<OrcReaderBatch> recycler) {
            this.orcVectorizedRowBatch = checkNotNull(orcVectorizedRowBatch);
            this.recycler = checkNotNull(recycler);
            this.paimonColumnBatch = paimonColumnBatch;
            this.result =
                    new ColumnarRowIterator(
                            filePath, new ColumnarRow(paimonColumnBatch), this::recycle);
        }

        /**
         * 释放 OrcReaderBatch 时，将其放回 pool.
         *
         * <p>Puts this batch back into the pool. This should be called after all records from the
         * batch have been returned, typically in the {@link RecordIterator#releaseBatch()} method.
         */
        public void recycle() {
            recycler.recycle(this);
        }

        /** Gets the ORC VectorizedRowBatch structure from this batch. */
        public VectorizedRowBatch orcVectorizedRowBatch() {
            return orcVectorizedRowBatch;
        }

        private RecordIterator<InternalRow> convertAndGetIterator(
                VectorizedRowBatch orcBatch, long rowNumber) {
            // 不需要复制 VectorizedRowBatch 到 VectorizedColumnBatch，其底层数组一致.
            // VectorizedColumnBatch 中的 ColumnVector 基于 VectorizedRowBatch 中的 ColumnVector.
            // no copying from the ORC column vectors to the Paimon columns vectors necessary,
            // because they point to the same data arrays internally design
            paimonColumnBatch.setNumRows(orcBatch.size); // 批次大小
            result.reset(rowNumber); // 下一次要读去的行数
            return result;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * 基于 orc RecordReader 向量化读取一个批次的数据，并转换为行返回.
     *
     * <p>A vectorized ORC reader. This reader reads an ORC Batch at a time and converts it to one
     * or more records to be returned. An ORC Row-wise reader would convert the batch into a set of
     * rows, while a reader for a vectorized query processor might return the whole batch as one
     * record.
     *
     * <p>The conversion of the {@code VectorizedRowBatch} happens in the specific {@link
     * OrcReaderBatch} implementation.
     *
     * <p>The reader tracks its current position using ORC's <i>row numbers</i>. Each record in a
     * batch is addressed by the starting row number of the batch, plus the number of records to be
     * skipped before.
     */
    private static final class OrcVectorizedReader
            implements org.apache.paimon.reader.RecordReader<InternalRow> {

        private final RecordReader orcReader;
        private final Pool<OrcReaderBatch> pool;

        private OrcVectorizedReader(final RecordReader orcReader, final Pool<OrcReaderBatch> pool) {
            this.orcReader = checkNotNull(orcReader, "orcReader");
            this.pool = checkNotNull(pool, "pool");
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            // 获取一个 OrcReaderBatch 对象
            final OrcReaderBatch batch = getCachedEntry();
            // 获取 OrcReaderBatch 中的 VectorizedRowBatch 对象
            final VectorizedRowBatch orcVectorBatch = batch.orcVectorizedRowBatch();

            // orcReader 下次要读第几行.
            long rowNumber = orcReader.getRowNumber();
            if (!nextBatch(orcReader, orcVectorBatch)) { // 读取一个 batch 的数据到 rowBatch 中.
                batch.recycle(); // 没有读到数据，回收 OrcReaderBatch
                return null; // 没有数据
            }

            // 读取结果转换为 RecordIterator<InternalRow>
            return batch.convertAndGetIterator(orcVectorBatch, rowNumber);
        }

        @Override
        public void close() throws IOException {
            orcReader.close();
        }

        private OrcReaderBatch getCachedEntry() throws IOException {
            try {
                return pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted");
            }
        }
    }

    private static RecordReader createRecordReader(
            org.apache.hadoop.conf.Configuration conf,
            TypeDescription schema,
            List<OrcFilters.Predicate> conjunctPredicates,
            FileIO fileIO,
            org.apache.paimon.fs.Path path,
            long splitStart,
            long splitLength)
            throws IOException {
        // 创建 orc reader.
        org.apache.orc.Reader orcReader = createReader(conf, fileIO, path);
        try {
            // createRecordReader 只被调用过一次，splitStart 为 0，splitLength 为文件长度
            // 所以是读取整个文件中所有 stripe.
            // get offset and length for the stripes that start in the split
            Pair<Long, Long> offsetAndLength =
                    getOffsetAndLengthForSplit(splitStart, splitLength, orcReader.getStripes());

            // create ORC row reader configuration
            org.apache.orc.Reader.Options options =
                    new org.apache.orc.Reader.Options()
                            .schema(schema)
                            .range(
                                    offsetAndLength.getLeft(),
                                    offsetAndLength.getRight()) // 读取的数据范围.
                            .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf)) // 是否使用 zero copy.
                            .skipCorruptRecords(
                                    OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf)) // 是否跳过损坏数据.
                            .tolerateMissingSchema(
                                    OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(
                                            conf)); // 容忍不完整的 schema.

            // configure filters
            // 设置读取时的过滤条件.
            if (!conjunctPredicates.isEmpty()) {
                SearchArgument.Builder b = SearchArgumentFactory.newBuilder();
                b = b.startAnd();
                for (OrcFilters.Predicate predicate : conjunctPredicates) {
                    predicate.add(b);
                }
                b = b.end();
                options.searchArgument(b.build(), new String[] {});
            }

            // create ORC row reader
            // 创建 orc Recorder，能够一行一行读取数据.
            RecordReader orcRowsReader = orcReader.rows(options);

            // assign ids
            // 给 TypeDescription 分配一个唯一 ID
            schema.getId();

            return orcRowsReader;
        } catch (IOException e) {
            // exception happened, we need to close the reader
            IOUtils.closeQuietly(orcReader);
            throw e;
        }
    }

    private static VectorizedRowBatch createBatchWrapper(TypeDescription schema, int batchSize) {
        // VectorizedRowBatch 是一个类，用于存储列向量数据.
        //
        // 在传统的查询执行中，数据通常是以逐行的方式处理的，即一次处理一行数据。而在向量化查询执行中，数据被处理为列向量，允许以更高效的方式进行批量处理，从而提高了查询执行的性能。
        // VectorizedRowBatch 类主要用于存储查询结果或中间数据。它以列式存储（columnar
        // storage）的方式组织数据，允许查询运算符一次性处理一批数据，而不是一次处理一行。
        // 此类还提供了许多方法来访问和操作存储的列向量数据，包括获取值、设置值、批处理过滤等。
        return schema.createRowBatch(batchSize);
    }

    private static boolean nextBatch(RecordReader reader, VectorizedRowBatch rowBatch)
            throws IOException {
        // 读取一个 batch 的数据到 rowBatch 中.
        return reader.nextBatch(rowBatch);
    }

    private static Pair<Long, Long> getOffsetAndLengthForSplit(
            long splitStart, long splitLength, List<StripeInformation> stripes) {
        // 要读取的 stripe 范围区间
        // 比如，有如下 stripe: [0, 10], [11, 20], [21, 30]
        // 返回值为：[0,30]

        long splitEnd = splitStart + splitLength;
        long readStart = Long.MAX_VALUE;
        long readEnd = Long.MIN_VALUE;

        for (StripeInformation s : stripes) {
            if (splitStart <= s.getOffset() && s.getOffset() < splitEnd) {
                // stripe starts in split, so it is included
                readStart = Math.min(readStart, s.getOffset());
                readEnd = Math.max(readEnd, s.getOffset() + s.getLength());
            }
        }

        if (readStart < Long.MAX_VALUE) {
            // at least one split is included
            return Pair.of(readStart, readEnd - readStart);
        } else {
            return Pair.of(0L, 0L);
        }
    }

    /**
     * 创建 orc reader.
     *
     * @param conf orc reader conf
     * @param fileIO file io
     * @param path file path
     * @return orc reader
     * @throws IOException IOException
     */
    public static org.apache.orc.Reader createReader(
            org.apache.hadoop.conf.Configuration conf,
            FileIO fileIO,
            org.apache.paimon.fs.Path path)
            throws IOException {
        // open ORC file and create reader
        org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(path.toUri());

        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);

        // configure filesystem from Paimon FileIO
        readerOptions.filesystem(new HadoopReadOnlyFileSystem(fileIO));

        return OrcFile.createReader(hPath, readerOptions);
    }
}
