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

package org.apache.paimon.lookup.hash;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.io.CompressedPageFileOutput;
import org.apache.paimon.io.PageFileOutput;
import org.apache.paimon.lookup.LookupStoreFactory.Context;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/* This file is based on source code of StorageWriter from the PalDB Project (https://github.com/linkedin/PalDB), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * 将 LSM 的一个有序文件重写成一个 KV 存储文件，使其更容易按 key 的 hash 值进行高效查找.
 *
 * <p>第一步：对于每个 key 长度，创建一个索引文件，存储 key1,value_offset1,key2,value_offset2,key3,value_offset3 第二步：对于每个
 * key 长度，创建一个数据文件，存储 size1,value1,size2,value2,size3,value3 第三步：重写每个 key 长度索引文件 - key 数量 /
 * loadFactor 得到 slot 数量 - 每个 slot 大小相同，为 KEY 长度 + 值偏移量最大长度 - 重写后的索引文件大小为 slot 数量 * 每个 slot 大小 -
 * 遍历当前固定 KEY 长度下的所有 KEY，取 HASH 值，然后与 slot 数量取余，定位到存储的 slot - 如果 slot 为空，则写入 key + value 偏移量 -
 * 如果不为空，则 hash 值自增，然后重新计算 slot，直到找到空闲的为止 - 由于 slot 数量大于 key 数量，一定能找到空闲的 第四步：合并 布隆过滤器文件 + 索引文件 +
 * 数据文件，合并过程就是直接读取写入字节，不涉及其他逻辑，根据指定的压缩算法来压缩字节
 *
 * <p>上述过程中，每个长度 KEY 的元信息都被记录在 HashContext 中，用于 HashLookupStoreReader 读取时使用
 *
 * <p>Internal write implementation for hash kv store.
 */
public class HashLookupStoreWriter implements LookupStoreWriter {

    private static final Logger LOG =
            LoggerFactory.getLogger(HashLookupStoreWriter.class.getName());

    // 容量超过 load factor，执行 rehash 扩容
    // load factor of hash map, default 0.75
    private final double loadFactor;
    // Output 临时文件目录
    private final File tempFolder;
    // 结果文件
    private final File outputFile;

    // Index stream: 每个 key 长度对应不同的索引文，若两个 KEY 长度则对应同个索引文件
    private File[] indexFiles;

    // 每个 key 长度对应不同的索引文件输出流，若两个 KEY 长度则对应同个索引文件输出流
    private DataOutputStream[] indexStreams;
    // Data stream 每个 key 长度对应不同的数据文件
    private File[] dataFiles;

    // 每个 key 长度对应不同的数据文件输出流
    private DataOutputStream[] dataStreams;

    // 每个 KEY 长度上一次存储的 value 值
    // Cache last value
    private byte[][] lastValues;
    // 每个 KEY 长度上一次存储的 value 长度
    private int[] lastValuesLength;
    // Data length 每个 key 长度对应的 value 长度，值为：1 + 可变长度编码后的 value length + value length
    private long[] dataLengths;
    // Max offset length
    private int[] maxOffsetLengths;
    // Number of keys 文件中 key 的数量
    private int keyCount;
    // key 长度对应的 key 数量
    private int[] keyCounts;
    // Number of values 真实写出的 value 数量
    private int valueCount;
    // Number of collisions 记录冲突次数
    private int collisions;

    // Bloom 过滤器
    @Nullable private final BloomFilter.Builder bloomFilter;

    // 压缩工厂类
    @Nullable private final BlockCompressionFactory compressionFactory;

    // 压缩 page 大小（字节块大小？）
    private final int compressPageSize;

    HashLookupStoreWriter(
            double loadFactor,
            File file,
            @Nullable BloomFilter.Builder bloomFilter,
            @Nullable BlockCompressionFactory compressionFactory,
            int compressPageSize)
            throws IOException {
        this.loadFactor = loadFactor;
        this.outputFile = file;
        this.compressionFactory = compressionFactory;
        this.compressPageSize = compressPageSize;

        // 校验负载因子的合法性
        if (loadFactor <= 0.0 || loadFactor >= 1.0) {
            throw new IllegalArgumentException(
                    "Illegal load factor = " + loadFactor + ", should be between 0.0 and 1.0.");
        }

        // 在文件的父目录下创建一个临时文件夹
        this.tempFolder = new File(file.getParentFile(), UUID.randomUUID().toString());
        if (!tempFolder.mkdir()) {
            throw new IOException("Can not create temp folder: " + tempFolder);
        }
        this.indexStreams = new DataOutputStream[0];
        this.dataStreams = new DataOutputStream[0];
        this.indexFiles = new File[0];
        this.dataFiles = new File[0];
        this.lastValues = new byte[0][];
        this.lastValuesLength = new int[0];
        this.dataLengths = new long[0];
        this.maxOffsetLengths = new int[0];
        this.keyCounts = new int[0];
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        int keyLength = key.length;

        // 获取 key 长度对应的索引文件输出流
        // Get the Output stream for that keyLength, each key length has its own file
        DataOutputStream indexStream = getIndexStream(keyLength);

        // 写出 key
        // Write key
        indexStream.write(key);

        // 获取 key 长度上次写入的 value 和当前写写入的 value 是否相同
        // Check if the value is identical to the last inserted
        byte[] lastValue = lastValues[keyLength];
        boolean sameValue = lastValue != null && Arrays.equals(value, lastValue);

        // 获取 key 长度对应的 value 长度
        // Get data stream and length
        long dataLength = dataLengths[keyLength];
        if (sameValue) {
            // 减去上一次 压缩编码后的 value_size + value 的长度
            dataLength -= lastValuesLength[keyLength];
        }

        // Write offset and record max offset length
        // 写出 key 对应的 value 偏移量，返回偏移量长度
        int offsetLength = VarLengthIntUtils.encodeLong(indexStream, dataLength);
        // 保存每个 key 长度对应的最大的偏移量长度
        maxOffsetLengths[keyLength] = Math.max(offsetLength, maxOffsetLengths[keyLength]);

        // Write if data is not the same
        if (!sameValue) {
            // Get stream 获取 key 长度对应的文件输出流
            DataOutputStream dataStream = getDataStream(keyLength);

            // Write size and value 写出 value 长度
            int valueSize = VarLengthIntUtils.encodeInt(dataStream, value.length);
            // 写出 value
            dataStream.write(value);

            // Update data length 更新 key 长度对应的 value 长度
            dataLengths[keyLength] += valueSize + value.length;

            // Update last value 更新上一次写出的值
            lastValues[keyLength] = value;
            // 更新上一次写出的长度
            lastValuesLength[keyLength] = valueSize + value.length;

            valueCount++;
        }

        // key 的数量加 1
        keyCount++;
        // key 长度对应的 key 数量加 1
        keyCounts[keyLength]++;
        if (bloomFilter != null) {
            // 布隆过滤器中添加一个 hash key
            bloomFilter.addHash(MurmurHashUtils.hashBytes(key));
        }
    }

    @Override
    public Context close() throws IOException {
        // Close the data and index streams
        for (DataOutputStream dos : dataStreams) {
            if (dos != null) {
                dos.close();
            }
        }
        for (DataOutputStream dos : indexStreams) {
            if (dos != null) {
                dos.close();
            }
        }

        // Stats
        LOG.info("Number of keys: {}", keyCount);
        LOG.info("Number of values: {}", valueCount);

        // Prepare files to merge 需要合并的文件列表
        List<File> filesToMerge = new ArrayList<>();

        int bloomFilterBytes = bloomFilter == null ? 0 : bloomFilter.getBuffer().size();

        // writer 传递给 reader 的上下午信息
        HashContext context =
                new HashContext(
                        bloomFilter != null,
                        bloomFilter == null ? 0 : bloomFilter.expectedEntries(),
                        bloomFilterBytes,
                        new int[keyCounts.length],
                        new int[keyCounts.length],
                        new int[keyCounts.length],
                        new int[keyCounts.length],
                        new long[keyCounts.length],
                        0,
                        null);

        // 索引预留 bloomFilterBytes 存储 bloomFilter
        long indexesLength = bloomFilterBytes;
        long datasLength = 0;
        for (int i = 0; i < this.keyCounts.length; i++) {
            if (this.keyCounts[i] > 0) { // key 长度对应的 key 存在
                // Write the key Count 记录每个 key 对应的 key 数量
                context.keyCounts[i] = keyCounts[i];

                // Write slot count 计算每个 key 长度对应的 slot 数量
                int slots = (int) Math.round(keyCounts[i] / loadFactor);
                context.slots[i] = slots;

                // Write slot size value 的最大偏移量的长度
                int offsetLength = maxOffsetLengths[i];
                context.slotSizes[i] = i + offsetLength; // 键长度 + 值偏移量长度作为 slot 大小

                // Write index offset 记录每个 key 长度对应索引 offset
                context.indexOffsets[i] = (int) indexesLength;

                // Increment index length 下一个 key 长度对应的索引 offset
                indexesLength += (long) (i + offsetLength) * slots;

                // Write data length 记录 key 长度对应的 value 的 offset.
                // 相当于只合并 value 文件之后的每个 key 长度对应的值偏移量，起始值是 0
                context.dataOffsets[i] = datasLength;

                // Increment data length
                // 下一个 key 长度对应的 value 的 offset（假设只合并 value 文件）.
                datasLength += dataLengths[i];
            }
        }

        // adjust data offsets 加上索引长度，相当于索引写在数据前
        for (int i = 0; i < context.dataOffsets.length; i++) {
            context.dataOffsets[i] = indexesLength + context.dataOffsets[i];
        }

        // 以 page 为单位写出数据
        PageFileOutput output =
                PageFileOutput.create(outputFile, compressPageSize, compressionFactory);
        try {
            // Write bloom filter file 生成布隆过滤器文件
            if (bloomFilter != null) {
                File bloomFilterFile = new File(tempFolder, "bloomfilter.dat");
                bloomFilterFile.deleteOnExit();
                try (FileOutputStream bfOutputStream = new FileOutputStream(bloomFilterFile)) {
                    bfOutputStream.write(bloomFilter.getBuffer().getArray());
                    LOG.info("Bloom filter size: {} bytes", bloomFilter.getBuffer().size());
                }
                filesToMerge.add(bloomFilterFile);
            }

            // Build index file
            for (int i = 0; i < indexFiles.length; i++) {
                if (indexFiles[i] != null) {
                    filesToMerge.add(buildIndex(i));
                }
            }

            // Stats collisions
            LOG.info("Number of collisions: {}", collisions);

            // Add data files
            for (File dataFile : dataFiles) {
                if (dataFile != null) {
                    filesToMerge.add(dataFile);
                }
            }

            // Merge and write to output
            checkFreeDiskSpace(filesToMerge);
            mergeFiles(filesToMerge, output);
        } finally {
            cleanup(filesToMerge);
            output.close();
        }

        // 结果文件大小
        LOG.info(
                "Compressed Total store size: {} Mb",
                new DecimalFormat("#,##0.0").format(outputFile.length() / (1024 * 1024)));

        if (output instanceof CompressedPageFileOutput) {
            CompressedPageFileOutput compressedOutput = (CompressedPageFileOutput) output;
            // 存储压缩前大小以及压缩后每个 page 对应的偏移量
            context = context.copy(compressedOutput.uncompressBytes(), compressedOutput.pages());
        }
        return context;
    }

    private File buildIndex(int keyLength) throws IOException {
        // 构建 key 长度的索引文件
        long count = keyCounts[keyLength]; // key 长度对应的 key 数量
        int slots = (int) Math.round(count / loadFactor); // count 个 key 需要的 slot 数量
        int offsetLength = maxOffsetLengths[keyLength]; // 值偏移量长度
        int slotSize = keyLength + offsetLength; // slot 大小等于 key 长度 + 值偏移量长度

        // Init index
        File indexFile = new File(tempFolder, "index" + keyLength + ".dat");
        try (RandomAccessFile indexAccessFile = new RandomAccessFile(indexFile, "rw")) {
            indexAccessFile.setLength((long) slots * slotSize); // 设置文件长度
            FileChannel indexChannel = indexAccessFile.getChannel();
            // 内存映射文件
            MappedByteBuffer byteBuffer =
                    indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexAccessFile.length());

            // Init reading stream 获取中间 index 文件读取流
            File tempIndexFile = indexFiles[keyLength];
            DataInputStream tempIndexStream =
                    new DataInputStream(
                            new BufferedInputStream(new FileInputStream(tempIndexFile)));
            try {
                byte[] keyBuffer = new byte[keyLength]; // 存储 key
                byte[] slotBuffer = new byte[slotSize]; // 存储 key + 值偏移量
                byte[] offsetBuffer = new byte[offsetLength]; // 存储值偏移量

                // Read all keys
                for (int i = 0; i < count; i++) {
                    // Read key 读取 key
                    tempIndexStream.readFully(keyBuffer);

                    // Read offset 读取值偏移量
                    long offset = VarLengthIntUtils.decodeLong(tempIndexStream);

                    // Hash 对 key 取 hash
                    long hash = MurmurHashUtils.hashBytesPositive(keyBuffer);

                    boolean collision = false;
                    for (int probe = 0; probe < count; probe++) { // 这个循环可以保证 key + 值偏移量一定能写成功
                        int slot = (int) ((hash + probe) % slots);
                        byteBuffer.position(slot * slotSize); // 获取 slot 的偏移量
                        byteBuffer.get(slotBuffer); // 获取 slot 数据

                        long found = VarLengthIntUtils.decodeLong(slotBuffer, keyLength);
                        if (found == 0) {
                            // The spot is empty use it 发现 slot 没有被使用，直接存储 key 和偏移量
                            byteBuffer.position(slot * slotSize);
                            byteBuffer.put(keyBuffer);
                            int pos = VarLengthIntUtils.encodeLong(offsetBuffer, offset);
                            byteBuffer.put(offsetBuffer, 0, pos);
                            break;
                        } else { // 发生冲突
                            collision = true;
                            // Check for duplicates 检查 key 是否重复，lsm 文件中这不应该发生
                            if (Arrays.equals(keyBuffer, Arrays.copyOf(slotBuffer, keyLength))) {
                                throw new RuntimeException(
                                        String.format(
                                                "A duplicate key has been found for for key bytes %s",
                                                Arrays.toString(keyBuffer)));
                            }
                        }
                    }

                    if (collision) {
                        collisions++;
                    }
                }

                String msg =
                        "  Max offset length: "
                                + offsetLength
                                + " bytes"
                                + "\n  Slot size: "
                                + slotSize
                                + " bytes";

                LOG.info("Built index file {}\n" + msg, indexFile.getName());
            } finally {
                // Close input
                tempIndexStream.close();

                // Close index and make sure resources are liberated
                indexChannel.close();

                // Delete temp index file
                if (tempIndexFile.delete()) {
                    LOG.info("Temporary index file {} has been deleted", tempIndexFile.getName());
                }
            }
        }

        return indexFile;
    }

    // 要合并的文件大小不能超过剩余磁盘的 2/3.
    // Fail if the size of the expected store file exceed 2/3rd of the free disk space
    private void checkFreeDiskSpace(List<File> inputFiles) {
        // Check for free space
        long usableSpace = 0;
        long totalSize = 0;
        for (File f : inputFiles) {
            if (f.exists()) {
                totalSize += f.length();
                usableSpace = f.getUsableSpace(); // 获取磁盘的剩余空间，每次调用实际获取的值相同，没太大必要这样用
            }
        }
        LOG.info(
                "Total expected store size is {} Mb",
                new DecimalFormat("#,##0.0").format(totalSize / (1024 * 1024)));
        LOG.info(
                "Usable free space on the system is {} Mb",
                new DecimalFormat("#,##0.0").format(usableSpace / (1024 * 1024)));
        if (totalSize / (double) usableSpace >= 0.66) {
            throw new RuntimeException("Aborting because there isn' enough free disk space");
        }
    }

    // 顺序写出多个文件
    // Merge files to the provided fileChannel
    private void mergeFiles(List<File> inputFiles, PageFileOutput output) throws IOException {
        long startTime = System.nanoTime();

        // Merge files
        for (File f : inputFiles) {
            if (f.exists()) {
                FileInputStream fileInputStream = new FileInputStream(f);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                try {
                    LOG.info("Merging {} size={}", f.getName(), f.length());

                    byte[] buffer = new byte[8192];
                    int length;
                    while ((length = bufferedInputStream.read(buffer)) > 0) {
                        output.write(buffer, 0, length);
                    }
                } finally {
                    bufferedInputStream.close();
                    fileInputStream.close();
                }
            } else {
                LOG.info("Skip merging file {} because it doesn't exist", f.getName());
            }
        }

        LOG.info("Time to merge {} s", ((System.nanoTime() - startTime) / 1000000000.0));
    }

    // 删除所有中间文件
    // Cleanup files
    private void cleanup(List<File> inputFiles) {
        for (File f : inputFiles) {
            if (f.exists()) {
                if (f.delete()) {
                    LOG.info("Deleted temporary file {}", f.getName());
                }
            }
        }
        if (tempFolder.delete()) {
            LOG.info("Deleted temporary folder at {}", tempFolder.getAbsolutePath());
        }
    }

    // 每个 key 长度对应一个数据文件，每个数据文件对应一个输出流
    // Get the data stream for the specified keyLength, create it if needed
    private DataOutputStream getDataStream(int keyLength) throws IOException {
        // Resize array if necessary  必要时扩容
        if (dataStreams.length <= keyLength) {
            dataStreams = Arrays.copyOf(dataStreams, keyLength + 1);
            dataFiles = Arrays.copyOf(dataFiles, keyLength + 1);
        }

        DataOutputStream dos = dataStreams[keyLength];
        if (dos == null) {
            File file = new File(tempFolder, "data" + keyLength + ".dat");
            file.deleteOnExit();
            dataFiles[keyLength] = file;

            dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            dataStreams[keyLength] = dos;

            // 预留零偏移：零偏移（Offset
            // 0）是指文件的起始位置。在很多文件存储和读取操作中，零偏移是一个特殊位置，通常用于标记文件头信息或者其他控制信息。直接从零偏移开始读写数据可能会导致文件读取错误或者数据错位
            //
            //
            // 通过写入一个字节来预留零偏移，可以确保在后续的数据写入和读取过程中，不会因零偏移的问题而导致错误。这样做有以下好处：
            //  - 防止误读：在读取文件时，可以避免误读文件开头的一些控制字节或者头信息，确保读取的数据是正确的
            //  - 规范数据存储格式：统一的预留零偏移有助于数据存储格式的一致性，方便数据解析和处理
            //
            // 例如，如果零偏移存储的是头信息，而数据存储从第一个字节开始，当文件被重新读取时，可以轻松跳过头信息并读取实际的数据内容。
            //
            // 确保一致性：写入一个字节是一个简单且有效的办法，用来标记文件的起始点。这一字节作为占位符，可以确保索引和数据读取操作的定位准确无误
            //
            // 总结：在创建新的数据流时，预留零偏移是为了确保数据存储和读取的准确性和一致性。通过先写入一个字节，可以防止零偏移对文件读写操作的干扰，保证读写过程中的数据完整性和正确性
            // Write one byte so the zero offset is reserved
            dos.writeByte(0);
        }
        return dos;
    }

    // 每个 key 长度都对应着一个索引文件，也对应着一个输出流，下面的方法为获取该输出流
    // Get the index stream for the specified keyLength, create it if needed
    private DataOutputStream getIndexStream(int keyLength) throws IOException {
        // 判断是否需要扩容
        // Resize array if necessary
        if (indexStreams.length <= keyLength) {
            indexStreams = Arrays.copyOf(indexStreams, keyLength + 1);
            indexFiles = Arrays.copyOf(indexFiles, keyLength + 1);
            keyCounts = Arrays.copyOf(keyCounts, keyLength + 1);
            maxOffsetLengths = Arrays.copyOf(maxOffsetLengths, keyLength + 1);
            lastValues = Arrays.copyOf(lastValues, keyLength + 1);
            lastValuesLength = Arrays.copyOf(lastValuesLength, keyLength + 1);
            dataLengths = Arrays.copyOf(dataLengths, keyLength + 1);
        }

        // Get or create stream 获取或创建输出流
        DataOutputStream dos = indexStreams[keyLength];
        if (dos == null) {
            File file = new File(tempFolder, "temp_index" + keyLength + ".dat");
            file.deleteOnExit();
            indexFiles[keyLength] = file;

            dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            indexStreams[keyLength] = dos;

            // 对应 getDataStream 方法中的预留零偏移
            dataLengths[keyLength]++;
        }
        return dos;
    }
}
