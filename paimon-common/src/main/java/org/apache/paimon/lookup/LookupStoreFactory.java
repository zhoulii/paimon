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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

/**
 * 用于创建 LookupStoreWriter 和 LookupStoreReader.
 *
 * <p>LookupStoreWriter 会重写一个 bucket 文件，目的是方便根据 key 查询值，只写一次. LookupStoreReader 会根据 key bytes 查找
 * value.
 *
 * <p>A key-value store for lookup, key-value store should be single binary file written once and
 * ready to be used. This factory provide two interfaces:
 *
 * <ul>
 *   <li>Writer: written once to prepare binary file.
 *   <li>Reader: lookup value by key bytes.
 * </ul>
 */
public interface LookupStoreFactory {

    // 创建 Writer 用于转写文件
    LookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException;

    // 创建 Reader 用于查找 value
    LookupStoreReader createReader(File file, Context context) throws IOException;

    static Function<Long, BloomFilter.Builder> bfGenerator(Options options) {
        // 默认没有布隆过滤器
        Function<Long, BloomFilter.Builder> bfGenerator = rowCount -> null;
        // lookup 文件是否开启布隆过滤器
        if (options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_ENABLED)) {
            // 获取允许的假阳率
            double bfFpp = options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_FPP);
            bfGenerator =
                    rowCount -> {
                        if (rowCount > 0) {
                            // 文件行数大于 0，创建过滤器
                            return BloomFilter.builder(rowCount, bfFpp);
                        }
                        return null;
                    };
        }
        return bfGenerator;
    }

    /**
     * Writer 和 Reader 之间的上下文信息.
     *
     * <p>记录写入时的一些元数据信息，读取时使用.
     *
     * <p>Context between writer and reader.
     */
    interface Context {}
}
