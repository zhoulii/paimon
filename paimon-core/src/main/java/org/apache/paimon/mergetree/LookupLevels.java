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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IOFunction;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.VarLengthIntUtils.MAX_VAR_LONG_SIZE;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeLong;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeLong;

/**
 * 根据 Key 快速查找 LSM，对应一个完成的 LSM.
 *
 * <p>Provide lookup by key.
 */
public class LookupLevels<T> implements Levels.DropFileCallback, Closeable {

    private final Levels levels; // LSM 完整数据文件
    private final Comparator<InternalRow> keyComparator; // key 比较器
    private final RowCompactedSerializer keySerializer; // key 压缩序列化器
    private final ValueProcessor<T> valueProcessor; // 如何读取、写入 value
    private final IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory; // 读取文件
    private final Supplier<File> localFileFactory; // 获取本地文件
    private final LookupStoreFactory lookupStoreFactory; // 如何转换文件方便根据 key 查找、如何读取转换后的文件.
    private final Cache<String, LookupFile> lookupFiles; // 缓存文件，Key 是文件名，value 是 LookupFile
    private final Function<Long, BloomFilter.Builder> bfGenerator; // 用于创建 BloomFilter，输入是文件行数

    public LookupLevels(
            Levels levels,
            Comparator<InternalRow> keyComparator,
            RowType keyType,
            ValueProcessor<T> valueProcessor,
            IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory,
            Supplier<File> localFileFactory,
            LookupStoreFactory lookupStoreFactory,
            Duration fileRetention, // 文件缓存时间
            MemorySize maxDiskSize,
            Function<Long, BloomFilter.Builder> bfGenerator) {
        this.levels = levels;
        this.keyComparator = keyComparator;
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.valueProcessor = valueProcessor;
        this.fileReaderFactory = fileReaderFactory;
        this.localFileFactory = localFileFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.lookupFiles =
                Caffeine.newBuilder()
                        .expireAfterAccess(fileRetention)
                        .maximumWeight(maxDiskSize.getKibiBytes()) // 最大磁盘占用
                        .weigher(this::fileWeigh)
                        .removalListener(this::removalCallback)
                        .executor(MoreExecutors.directExecutor()) // 使用当前线程执行异步任务
                        .build();
        this.bfGenerator = bfGenerator;
        levels.addDropFileCallback(this); // 添加删除文件回调
    }

    public Levels getLevels() {
        return levels;
    }

    @VisibleForTesting
    Cache<String, LookupFile> lookupFiles() {
        return lookupFiles;
    }

    @Override
    public void notifyDropFile(String file) {
        // 文件被删除时，清除该文件的缓存
        lookupFiles.invalidate(file);
    }

    @Nullable
    public T lookup(InternalRow key, int startLevel) throws IOException {
        // 从某层开始查找 key
        return LookupUtils.lookup(levels, key, startLevel, this::lookup, this::lookupLevel0);
    }

    @Nullable
    private T lookupLevel0(InternalRow key, TreeSet<DataFileMeta> level0) throws IOException {
        // 查找 level0 中的 key
        return LookupUtils.lookupLevel0(keyComparator, key, level0, this::lookup);
    }

    // 查找某个 SortedRun 中的某个 KEY
    @Nullable
    private T lookup(InternalRow key, SortedRun level) throws IOException {
        return LookupUtils.lookup(keyComparator, key, level, this::lookup);
    }

    // 查找文件中的某个 KEY
    @Nullable
    private T lookup(InternalRow key, DataFileMeta file) throws IOException {
        // 获取缓存的 LookupFile
        LookupFile lookupFile = lookupFiles.getIfPresent(file.fileName());

        while (lookupFile == null || lookupFile.isClosed) {
            lookupFile = createLookupFile(file); // 基于源文件重新创建 KV 存储文件
            lookupFiles.put(file.fileName(), lookupFile); // 加入缓存
        }

        byte[] keyBytes = keySerializer.serializeToBytes(key);
        byte[] valueBytes = lookupFile.get(keyBytes); // 使用 LookupStoreReader 查找
        if (valueBytes == null) {
            return null;
        }

        // 对 value 做处理，加入一些元信息，如属于哪个文件、属于哪个 level
        return valueProcessor.readFromDisk(
                key, lookupFile.remoteFile().level(), valueBytes, file.fileName());
    }

    private int fileWeigh(String file, LookupFile lookupFile) {
        // 获取文件大小的 KB 表示
        return fileKibiBytes(lookupFile.localFile);
    }

    private void removalCallback(String key, LookupFile file, RemovalCause cause) {
        if (file != null) {
            try {
                file.close(); // 关闭 reader 并删除本地转写的文件
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    // 创建 LookupFile.
    private LookupFile createLookupFile(DataFileMeta file) throws IOException {
        File localFile = localFileFactory.get(); // 获取一个本地文件路径，也就是转写成 KV 存储的文件
        if (!localFile.createNewFile()) {
            throw new IOException("Can not create new file: " + localFile);
        }

        // LookupStoreWriter 用于转写原始文件为 KV 存储
        LookupStoreWriter kvWriter =
                lookupStoreFactory.createWriter(localFile, bfGenerator.apply(file.rowCount()));
        // 存储转写过程中的上下文信息
        LookupStoreFactory.Context context;

        // 读取原始数据
        try (RecordReader<KeyValue> reader = fileReaderFactory.apply(file)) {
            KeyValue kv;
            if (valueProcessor.withPosition()) { // 转写过程中需要加上行号
                FileRecordIterator<KeyValue> batch;
                while ((batch = (FileRecordIterator<KeyValue>) reader.readBatch()) != null) {
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                        byte[] valueBytes =
                                valueProcessor.persistToDisk(kv, batch.returnedPosition());
                        kvWriter.put(keyBytes, valueBytes);
                    }
                    batch.releaseBatch();
                }
            } else {
                RecordReader.RecordIterator<KeyValue> batch;
                while ((batch = reader.readBatch()) != null) {
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = keySerializer.serializeToBytes(kv.key()); // 获取 key
                        byte[] valueBytes = valueProcessor.persistToDisk(kv); // 获取 value
                        kvWriter.put(keyBytes, valueBytes); // 转写 KV store.
                    }
                    batch.releaseBatch();
                }
            }
        } catch (IOException e) {
            FileIOUtils.deleteFileOrDirectory(localFile);
            throw e;
        } finally {
            context = kvWriter.close(); // 获取写入的上下文信息
        }

        // 创建 LookupFile，包含怎样读取这个文件的 reader
        return new LookupFile(localFile, file, lookupStoreFactory.createReader(localFile, context));
    }

    @Override
    public void close() throws IOException {
        // 清空缓存、删除本地文件
        lookupFiles.invalidateAll();
    }

    // 表示一种易于根据 KEY 查找值的数据文件
    private static class LookupFile implements Closeable {
        // bucket 中的一个文件，已经使用 LookupStoreWriter 转换为了 kv 文件存储

        private final File localFile;
        private final DataFileMeta remoteFile; // 文件描述信息
        private final LookupStoreReader reader;

        private boolean isClosed = false;

        public LookupFile(File localFile, DataFileMeta remoteFile, LookupStoreReader reader) {
            this.localFile = localFile;
            this.remoteFile = remoteFile;
            this.reader = reader;
        }

        @Nullable
        public byte[] get(byte[] key) throws IOException {
            checkArgument(!isClosed);
            return reader.lookup(key);
        }

        public DataFileMeta remoteFile() {
            return remoteFile;
        }

        @Override
        public void close() throws IOException {
            // 关闭 reader 删除本地文件
            reader.close();
            isClosed = true;
            FileIOUtils.deleteFileOrDirectory(localFile);
        }
    }

    /**
     * 自定义 Value 处理器，怎么读取、写入 Value.
     *
     * <p>用于对值做一些特殊处理，如加上文件行号，表示第几条数据.
     *
     * <p>Processor to process value.
     */
    public interface ValueProcessor<T> {

        boolean withPosition();

        byte[] persistToDisk(KeyValue kv);

        default byte[] persistToDisk(KeyValue kv, long rowPosition) {
            throw new UnsupportedOperationException();
        }

        T readFromDisk(InternalRow key, int level, byte[] valueBytes, String fileName);
    }

    /**
     * 读取写入 KeyValue 类型.
     *
     * <p>A {@link ValueProcessor} to return {@link KeyValue}.
     */
    public static class KeyValueProcessor implements ValueProcessor<KeyValue> {

        private final RowCompactedSerializer valueSerializer;

        public KeyValueProcessor(RowType valueType) {
            this.valueSerializer = new RowCompactedSerializer(valueType);
        }

        @Override
        public boolean withPosition() {
            return false;
        }

        @Override
        public byte[] persistToDisk(KeyValue kv) {
            byte[] vBytes = valueSerializer.serializeToBytes(kv.value());
            byte[] bytes = new byte[vBytes.length + 8 + 1];
            MemorySegment segment = MemorySegment.wrap(bytes);
            segment.put(0, vBytes); // 序列化 value
            segment.putLong(bytes.length - 9, kv.sequenceNumber()); // 序列化 sequence
            segment.put(bytes.length - 1, kv.valueKind().toByteValue()); // 序列化 rowKind
            return bytes;
        }

        @Override
        public KeyValue readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
            InternalRow value = valueSerializer.deserialize(bytes);
            long sequenceNumber =
                    MemorySegment.wrap(bytes).getLong(bytes.length - 9); // 反序列化 sequence
            RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]); // 反序列化 rowKind
            return new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level);
        }
    }

    /**
     * ContainsValueProcessor 并不会真正的读取数据，readFromDisk 直接返回 true，表示包含的意思.
     *
     * <p>A {@link ValueProcessor} to return {@link Boolean} only.
     */
    public static class ContainsValueProcessor implements ValueProcessor<Boolean> {

        private static final byte[] EMPTY_BYTES = new byte[0];

        @Override
        public boolean withPosition() {
            return false;
        }

        @Override
        public byte[] persistToDisk(KeyValue kv) {
            return EMPTY_BYTES;
        }

        @Override
        public Boolean readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
            return Boolean.TRUE;
        }
    }

    /**
     * 对 value 做序列化、反序列化，包含 row 在文化中的行号信息.
     *
     * <p>A {@link ValueProcessor} to return {@link PositionedKeyValue}.
     */
    public static class PositionedKeyValueProcessor implements ValueProcessor<PositionedKeyValue> {
        private final boolean persistValue; // 是否持久化 value，还是只持久化行号
        private final RowCompactedSerializer valueSerializer; // value 序列化器

        public PositionedKeyValueProcessor(RowType valueType, boolean persistValue) {
            this.persistValue = persistValue;
            this.valueSerializer = persistValue ? new RowCompactedSerializer(valueType) : null;
        }

        @Override
        public boolean withPosition() {
            return true;
        }

        @Override
        public byte[] persistToDisk(KeyValue kv) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] persistToDisk(KeyValue kv, long rowPosition) {
            if (persistValue) {
                byte[] vBytes = valueSerializer.serializeToBytes(kv.value());
                byte[] bytes = new byte[vBytes.length + 8 + 8 + 1];
                MemorySegment segment = MemorySegment.wrap(bytes);
                segment.put(0, vBytes);
                segment.putLong(bytes.length - 17, rowPosition); // 序列化包含行位置信息
                segment.putLong(bytes.length - 9, kv.sequenceNumber());
                segment.put(bytes.length - 1, kv.valueKind().toByteValue());
                return bytes;
            } else {
                byte[] bytes = new byte[MAX_VAR_LONG_SIZE]; // 下一行方法将 long 类型最多编码成 9 个字节
                int len = encodeLong(bytes, rowPosition);
                return Arrays.copyOf(bytes, len); // 复制出来的字节数组实际上就表示 rowPosition
            }
        }

        @Override
        public PositionedKeyValue readFromDisk(
                InternalRow key, int level, byte[] bytes, String fileName) {
            if (persistValue) {
                InternalRow value = valueSerializer.deserialize(bytes);
                MemorySegment segment = MemorySegment.wrap(bytes);
                long rowPosition = segment.getLong(bytes.length - 17); // 反序列化行号
                long sequenceNumber = segment.getLong(bytes.length - 9);
                RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]);
                return new PositionedKeyValue(
                        new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level),
                        fileName,
                        rowPosition);
            } else {
                long rowPosition = decodeLong(bytes, 0); // 只反序列化行号
                return new PositionedKeyValue(null, fileName, rowPosition);
            }
        }
    }

    /**
     * KeyValue 的包装，包含文件名、文件中的行号，用于 Deletion Vector.
     *
     * <p>{@link KeyValue} with file name and row position for DeletionVector.
     */
    public static class PositionedKeyValue {
        private final @Nullable KeyValue keyValue;
        private final String fileName;
        private final long rowPosition;

        public PositionedKeyValue(@Nullable KeyValue keyValue, String fileName, long rowPosition) {
            this.keyValue = keyValue;
            this.fileName = fileName;
            this.rowPosition = rowPosition;
        }

        public String fileName() {
            return fileName;
        }

        public long rowPosition() {
            return rowPosition;
        }

        @Nullable
        public KeyValue keyValue() {
            return keyValue;
        }
    }
}
