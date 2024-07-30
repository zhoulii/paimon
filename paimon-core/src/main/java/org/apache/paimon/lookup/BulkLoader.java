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

package org.apache.paimon.lookup;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;
import org.rocksdb.TtlDB;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 批量加载 RocksDB 数据.
 *
 * <p>Bulk loader for RocksDB.
 */
public class BulkLoader {

    private final String uuid = UUID.randomUUID().toString();

    // 列族信息
    private final ColumnFamilyHandle columnFamily;
    // sst 文件路径
    private final String path;
    private final RocksDB db;
    // 是否启用 TTL
    private final boolean isTtlEnabled;
    private final Options options;
    // 保存 sst 文件的路径
    private final List<String> files = new ArrayList<>();
    // 开始导入数据的时间
    private final int currentTimeSeconds;

    // 写 sst 文件的 writer
    private SstFileWriter writer = null;
    // sst 文件的索引号
    private int sstIndex = 0;
    // sst 文件记录数
    private long recordNum = 0;

    public BulkLoader(RocksDB db, Options options, ColumnFamilyHandle columnFamily, String path) {
        this.db = db;
        this.isTtlEnabled = db instanceof TtlDB;
        this.options = options;
        this.columnFamily = columnFamily;
        this.path = path;
        this.currentTimeSeconds = (int) (System.currentTimeMillis() / 1000);
    }

    public void write(byte[] key, byte[] value) throws WriteException {
        try {
            if (writer == null) {
                // 创建 writer 及 sst 文件
                writer = new SstFileWriter(new EnvOptions(), options);
                String path = new File(this.path, "sst-" + uuid + "-" + (sstIndex++)).getPath();
                writer.open(path);
                files.add(path);
            }

            if (isTtlEnabled) {
                // 给要写入的 value 添加时间戳
                value = appendTimestamp(value);
            }

            try {
                // 写入 value
                writer.put(key, value);
            } catch (RocksDBException e) {
                throw new WriteException(e);
            }

            // 统计记录数
            recordNum++;
            // 判断是否要结束当前 SST
            if (recordNum % 1000 == 0 && writer.fileSize() >= options.targetFileSizeBase()) {
                writer.finish();
                writer.close();
                writer = null;
                recordNum = 0;
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] appendTimestamp(byte[] value) {
        // 在 value 末尾加上时间戳
        byte[] newValue = new byte[value.length + 4];
        System.arraycopy(value, 0, newValue, 0, value.length);
        newValue[value.length] = (byte) (currentTimeSeconds & 0xff);
        newValue[value.length + 1] = (byte) ((currentTimeSeconds >> 8) & 0xff);
        newValue[value.length + 2] = (byte) ((currentTimeSeconds >> 16) & 0xff);
        newValue[value.length + 3] = (byte) ((currentTimeSeconds >> 24) & 0xff);
        return newValue;
    }

    public void finish() {
        // 结束 bulk load
        try {
            if (writer != null) {
                writer.finish();
                writer.close();
            }

            if (files.size() > 0) {
                // 调用了 RocksDB 中的 ingestExternalFile 方法来执行数据的批量加载（bulk
                // load）操作，ingestExternalFile方法接收三个参数：
                // - columnFamily 表示要加载数据的列族（或列簇）
                // - files 表示需要加载的文件列表
                // - ingestOptions 表示批量加载的选项配置，例如是否在加载完成后删除源文件、是否进行数据校验等
                //
                // bulk load 可以快速地批量导入大量数据，避免逐条写入而带来的性能开销.

                IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions();
                db.ingestExternalFile(columnFamily, files, ingestOptions);
                ingestOptions.close();
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /** Exception during writing. */
    public static class WriteException extends Exception {
        public WriteException(Throwable cause) {
            super(cause);
        }
    }
}
