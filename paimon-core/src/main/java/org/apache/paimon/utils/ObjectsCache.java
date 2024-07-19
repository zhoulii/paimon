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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentSource;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * 对象缓存，基于 SegmentsCache.
 *
 * <p>Cache records to {@link SegmentsCache} by compacted serializer.
 */
public class ObjectsCache<K, V> {

    private final SegmentsCache<K> cache;
    private final ObjectSerializer<V> serializer;
    private final InternalRowSerializer rowSerializer;
    private final BiFunction<K, Long, CloseableIterator<InternalRow>> reader;

    public ObjectsCache(
            SegmentsCache<K> cache,
            ObjectSerializer<V> serializer,
            BiFunction<K, Long, CloseableIterator<InternalRow>> reader) {
        this.cache = cache;
        this.serializer = serializer;
        this.rowSerializer = new InternalRowSerializer(serializer.fieldTypes());
        this.reader = reader;
    }

    /**
     * 从缓存中读取对象.
     *
     * @param key cache key 是文件名称
     * @param fileSize file size
     * @param loadFilter filter for loading data
     * @param readFilter filter for reading data
     */
    public List<V> read(
            K key,
            @Nullable Long fileSize,
            Filter<InternalRow> loadFilter,
            Filter<InternalRow> readFilter)
            throws IOException {
        // 先从缓存中读取，没有则从文件读取并将读取结果放回缓存.
        Segments segments = cache.getSegments(key, k -> readSegments(k, fileSize, loadFilter));
        List<V> entries = new ArrayList<>();
        RandomAccessInputView view =
                new RandomAccessInputView(
                        segments.segments(), cache.pageSize(), segments.limitInLastSegment());
        BinaryRow binaryRow = new BinaryRow(rowSerializer.getArity());
        while (true) {
            try {
                rowSerializer.mapFromPages(binaryRow, view); // 将 binary row 指向一段内存对象
                if (readFilter.test(binaryRow)) {
                    entries.add(serializer.fromRow(binaryRow)); // 将 binary row 转换为特定的对象
                }
            } catch (EOFException e) {
                return entries;
            }
        }
    }

    private Segments readSegments(K key, @Nullable Long fileSize, Filter<InternalRow> loadFilter) {
        // 从文件读取数据，并创建 Segments.
        try (CloseableIterator<InternalRow> iterator = reader.apply(key, fileSize)) { // 读取文件内容
            ArrayList<MemorySegment> segments = new ArrayList<>();
            MemorySegmentSource segmentSource =
                    () -> MemorySegment.allocateHeapMemory(cache.pageSize());
            SimpleCollectingOutputView output =
                    new SimpleCollectingOutputView(segments, segmentSource, cache.pageSize());
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (loadFilter.test(row)) {
                    // 序列化写出
                    rowSerializer.serializeToPages(row, output);
                }
            }
            // 创建 Segments，output.getCurrentPositionInSegment() 为最后一个 MemorySegment 的当前写入位置.
            return new Segments(segments, output.getCurrentPositionInSegment());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
