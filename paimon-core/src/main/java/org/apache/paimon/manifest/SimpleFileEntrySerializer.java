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

package org.apache.paimon.manifest;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.VersionedObjectSerializer;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * SimpleFileEntry 的序列化器.
 *
 * <p>不支持直接写出 SimpleFileEntry 对象（不支持将 SimpleFileEntry 转换为 InternalRow），因为 SimpleFileEntry
 * 设计的目的是为了减少内存占用，持久化存储使用的还是 ManifestEntry.
 *
 * <p>支持直接从表示 ManifestEntry 的 InternalRow 转换为 SimpleFileEntry.
 *
 * <p>A {@link VersionedObjectSerializer} for {@link SimpleFileEntry}, only supports reading.
 */
public class SimpleFileEntrySerializer extends VersionedObjectSerializer<SimpleFileEntry> {

    private static final long serialVersionUID = 1L;

    private final int version;

    public SimpleFileEntrySerializer() {
        super(ManifestEntry.schema()); // 和 ManifestEntry.schema() 一致
        this.version = new ManifestEntrySerializer().getVersion(); // 和 ManifestEntry version 一致
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public InternalRow convertTo(SimpleFileEntry meta) {
        // 不支持直接写出 SimpleFileEntry
        throw new UnsupportedOperationException("Only supports convert from row.");
    }

    @Override
    public SimpleFileEntry convertFrom(int version, InternalRow row) {
        if (this.version != version) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }

        // 这里的 row 是表示 ManifestEntry 的 InternalRow
        InternalRow file = row.getRow(4, 3);
        return new SimpleFileEntry(
                FileKind.fromByteValue(row.getByte(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                file.getInt(10),
                file.getString(0).toString(),
                deserializeBinaryRow(file.getBinary(3)),
                deserializeBinaryRow(file.getBinary(4)));
    }
}
