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

package org.apache.paimon.index;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.utils.IntIterator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;

/**
 * 用于与 IndexFile 交互.
 *
 * <p>Handle index files.
 */
public class IndexFileHandler {

    private final SnapshotManager snapshotManager;
    private final PathFactory pathFactory;
    private final IndexManifestFile indexManifestFile;
    private final HashIndexFile hashIndex;
    private final DeletionVectorsIndexFile deletionVectorsIndex;

    public IndexFileHandler(
            SnapshotManager snapshotManager,
            PathFactory pathFactory,
            IndexManifestFile indexManifestFile,
            HashIndexFile hashIndex,
            DeletionVectorsIndexFile deletionVectorsIndex) {
        this.snapshotManager = snapshotManager;
        this.pathFactory = pathFactory;
        this.indexManifestFile = indexManifestFile;
        this.hashIndex = hashIndex;
        this.deletionVectorsIndex = deletionVectorsIndex;
    }

    public Optional<IndexFileMeta> scan(
            long snapshotId, String indexType, BinaryRow partition, int bucket) {
        List<IndexManifestEntry> entries = scan(snapshotId, indexType, partition);
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry file : entries) {
            if (file.bucket() == bucket) {
                result.add(file);
            }
        }
        // 每个 bucket 对应一个 index file，比如 hash index 只能有一个，deletion vectors index 只能有一个.
        if (result.size() > 1) {
            throw new IllegalArgumentException(
                    "Find multiple index files for one bucket: " + result);
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0).indexFile());
    }

    public List<IndexManifestEntry> scan(String indexType, BinaryRow partition) {
        Long snapshot = snapshotManager.latestSnapshotId();
        if (snapshot == null) {
            return Collections.emptyList();
        }

        return scan(snapshot, indexType, partition);
    }

    public List<IndexManifestEntry> scan(long snapshotId, String indexType, BinaryRow partition) {
        // 读取符合条件的所有 IndexManifestEntry，IndexManifestEntry 表示新增或删除一个 IndexFile.
        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyList();
        }

        List<IndexManifestEntry> allFiles = indexManifestFile.read(indexManifest);
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry file : allFiles) {
            if (file.indexFile().indexType().equals(indexType)
                    && file.partition().equals(partition)) {
                result.add(file);
            }
        }

        return result;
    }

    public Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> scanPartitions(
            long snapshotId, String indexType, Set<BinaryRow> partitions) {
        // 获取多个 partition 下的 index 文件.
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> result = new HashMap<>();
        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyMap();
        }

        List<IndexManifestEntry> allFiles = indexManifestFile.read(indexManifest);
        for (IndexManifestEntry file : allFiles) {
            if (file.indexFile().indexType().equals(indexType)
                    && partitions.contains(file.partition())) {
                result.computeIfAbsent(
                                Pair.of(file.partition(), file.bucket()), k -> new ArrayList<>())
                        .add(file.indexFile());
            }
        }

        return result;
    }

    public Path filePath(IndexFileMeta file) {
        return pathFactory.toPath(file.fileName());
    }

    public List<Integer> readHashIndexList(IndexFileMeta file) {
        return IntIterator.toIntList(readHashIndex(file));
    }

    public IntIterator readHashIndex(IndexFileMeta file) {
        // 读取 hash index 存储的 hash 值.
        if (!file.indexType().equals(HASH_INDEX)) {
            throw new IllegalArgumentException("Input file is not hash index: " + file.indexType());
        }

        try {
            return hashIndex.read(file.fileName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public IndexFileMeta writeHashIndex(int[] ints) {
        return writeHashIndex(ints.length, IntIterator.create(ints));
    }

    public IndexFileMeta writeHashIndex(int size, IntIterator iterator) {
        // 写入 hash index.
        String file;
        try {
            file = hashIndex.write(iterator);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new IndexFileMeta(HASH_INDEX, file, hashIndex.fileSize(file), size);
    }

    public boolean existsManifest(String indexManifest) {
        // 判断 index manifest 是否存在.
        return indexManifestFile.exists(indexManifest);
    }

    public List<IndexManifestEntry> readManifest(String indexManifest) {
        // 读取 IndexManifestFile，抛出 RuntimeException.
        return indexManifestFile.read(indexManifest);
    }

    public List<IndexManifestEntry> readManifestWithIOException(String indexManifest)
            throws IOException {
        // 读取 IndexManifestFile，抛出 IOException.
        return indexManifestFile.readWithIOException(indexManifest);
    }

    public boolean existsIndexFile(IndexManifestEntry file) {
        // 判断 IndexFile 是否存在.
        return hashIndex.exists(file.indexFile().fileName());
    }

    public void deleteIndexFile(IndexManifestEntry file) {
        // 删除 IndexFile.
        hashIndex.delete(file.indexFile().fileName());
    }

    public void deleteManifest(String indexManifest) {
        // 删除 IndexManifestFile.
        indexManifestFile.delete(indexManifest);
    }

    public Map<String, DeletionVector> readAllDeletionVectors(IndexFileMeta fileMeta) {
        // 读取 deletion vectors.
        if (!fileMeta.indexType().equals(DELETION_VECTORS_INDEX)) {
            throw new IllegalArgumentException(
                    "Input file is not deletion vectors index " + fileMeta.indexType());
        }
        LinkedHashMap<String, Pair<Integer, Integer>> deleteIndexRange =
                fileMeta.deletionVectorsRanges();
        if (deleteIndexRange == null || deleteIndexRange.isEmpty()) {
            return Collections.emptyMap();
        }
        return deletionVectorsIndex.readAllDeletionVectors(fileMeta.fileName(), deleteIndexRange);
    }

    public Optional<DeletionVector> readDeletionVector(IndexFileMeta fileMeta, String fileName) {
        // 读取某个文件的 deletion vector.
        if (!fileMeta.indexType().equals(DELETION_VECTORS_INDEX)) {
            throw new IllegalArgumentException(
                    "Input file is not deletion vectors index " + fileMeta.indexType());
        }
        Map<String, Pair<Integer, Integer>> deleteIndexRange = fileMeta.deletionVectorsRanges();
        if (deleteIndexRange == null || !deleteIndexRange.containsKey(fileName)) {
            return Optional.empty();
        }
        return Optional.of(
                deletionVectorsIndex.readDeletionVector(
                        fileMeta.fileName(), deleteIndexRange.get(fileName)));
    }

    public IndexFileMeta writeDeletionVectorsIndex(Map<String, DeletionVector> deletionVectors) {
        // 写出 deletion vector.
        Pair<String, LinkedHashMap<String, Pair<Integer, Integer>>> pair =
                deletionVectorsIndex.write(deletionVectors);
        // IndexFileMeta 会保存在 ManifestEntry 中.
        return new IndexFileMeta(
                DELETION_VECTORS_INDEX,
                pair.getLeft(),
                deletionVectorsIndex.fileSize(pair.getLeft()),
                deletionVectors.size(),
                pair.getRight());
    }
}
