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

import org.apache.paimon.data.BinaryRow;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ManifestEntry 的简化版本，包含更少的信息（减少内存占用，缓解 OOM）.
 *
 * <p>A simple {@link FileEntry} only contains identifier and min max key.
 */
public class SimpleFileEntry implements FileEntry {

    private final FileKind kind;
    private final BinaryRow partition;
    private final int bucket;
    private final int level;
    private final String fileName;
    private final BinaryRow minKey;
    private final BinaryRow maxKey;

    public SimpleFileEntry(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            int level,
            String fileName,
            BinaryRow minKey,
            BinaryRow maxKey) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.level = level;
        this.fileName = fileName;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public static SimpleFileEntry from(ManifestEntry entry) {
        // 从 ManifestEntry 转换而来
        return new SimpleFileEntry(
                entry.kind(),
                entry.partition(),
                entry.bucket(),
                entry.level(),
                entry.fileName(),
                entry.minKey(),
                entry.maxKey());
    }

    public static List<SimpleFileEntry> from(List<ManifestEntry> entries) {
        // 批量转换 ManifestEntry
        return entries.stream().map(SimpleFileEntry::from).collect(Collectors.toList());
    }

    @Override
    public FileKind kind() {
        return kind;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public int level() {
        return level;
    }

    @Override
    public String fileName() {
        return fileName;
    }

    @Override
    public Identifier identifier() {
        return new Identifier(partition, bucket, level, fileName);
    }

    @Override
    public BinaryRow minKey() {
        return minKey;
    }

    @Override
    public BinaryRow maxKey() {
        return maxKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleFileEntry that = (SimpleFileEntry) o;
        return bucket == that.bucket
                && level == that.level
                && kind == that.kind
                && Objects.equals(partition, that.partition)
                && Objects.equals(fileName, that.fileName)
                && Objects.equals(minKey, that.minKey)
                && Objects.equals(maxKey, that.maxKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, level, fileName, minKey, maxKey);
    }

    @Override
    public String toString() {
        return "{"
                + "kind="
                + kind
                + ", partition="
                + partition
                + ", bucket="
                + bucket
                + ", level="
                + level
                + ", fileName='"
                + fileName
                + '\''
                + ", minKey="
                + minKey
                + ", maxKey="
                + maxKey
                + '}';
    }
}
