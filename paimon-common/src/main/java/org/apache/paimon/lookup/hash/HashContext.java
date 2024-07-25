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

import org.apache.paimon.lookup.LookupStoreFactory.Context;

/**
 * Reader、Writer 之间的上下文信息.
 *
 * <p>Context for {@link HashLookupStoreFactory}.
 */
public class HashContext implements Context {

    // is bloom filter enabled 是否启用布隆过滤器
    final boolean bloomFilterEnabled;
    // expected entries for bloom filter
    final long bloomFilterExpectedEntries;
    // bytes for bloom filter 布隆过滤器字节大小
    final int bloomFilterBytes;

    // Key count for each key length 每个 key 长度对应的 key 数量
    final int[] keyCounts;
    // Slot size for each key length
    final int[] slotSizes; // 每个 key 长度对应的 slot 大小
    // Number of slots for each key length
    final int[] slots; // 每个 key 长度对应的 slot 数量
    // Offset of the index for different key length
    final int[] indexOffsets; // 每个 key 长度对应的 index 偏移量（合并后的大文件中）
    // Offset of the data for different key length 每个 key 长度对应的 data 偏移量（合并后的大文件中）
    final long[] dataOffsets;

    final long uncompressBytes; // 未压缩的字节大小
    final long[] compressPages; // 压缩后每个 page 对应的偏移量

    public HashContext(
            boolean bloomFilterEnabled,
            long bloomFilterExpectedEntries,
            int bloomFilterBytes,
            int[] keyCounts,
            int[] slotSizes,
            int[] slots,
            int[] indexOffsets,
            long[] dataOffsets,
            long uncompressBytes,
            long[] compressPages) {
        this.bloomFilterEnabled = bloomFilterEnabled;
        this.bloomFilterExpectedEntries = bloomFilterExpectedEntries;
        this.bloomFilterBytes = bloomFilterBytes;
        this.keyCounts = keyCounts;
        this.slotSizes = slotSizes;
        this.slots = slots;
        this.indexOffsets = indexOffsets;
        this.dataOffsets = dataOffsets;
        this.uncompressBytes = uncompressBytes;
        this.compressPages = compressPages;
    }

    // 设置压缩前大小以及每个 page 对应的压缩后的偏移量
    public HashContext copy(long uncompressBytes, long[] compressPages) {
        return new HashContext(
                bloomFilterEnabled,
                bloomFilterExpectedEntries,
                bloomFilterBytes,
                keyCounts,
                slotSizes,
                slots,
                indexOffsets,
                dataOffsets,
                uncompressBytes,
                compressPages);
    }
}
