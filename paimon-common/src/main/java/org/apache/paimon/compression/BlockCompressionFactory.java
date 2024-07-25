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

package org.apache.paimon.compression;

import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import javax.annotation.Nullable;

/**
 * 块压缩工厂类，用于创建压缩与解压类.
 *
 * <p>Each compression codec has an implementation of {@link BlockCompressionFactory} to create
 * compressors and decompressors.
 */
public interface BlockCompressionFactory {

    BlockCompressor getCompressor();

    BlockDecompressor getDecompressor();

    /**
     * 根据压缩算法创建相应的工厂类.
     *
     * <p>Creates {@link BlockCompressionFactory} according to the configuration.
     */
    @Nullable
    static BlockCompressionFactory create(String compression) {
        switch (compression.toUpperCase()) {
            case "NONE":
                return null;
            case "LZ4":
                return new Lz4BlockCompressionFactory();
            case "LZO":
                return new AirCompressorFactory(new LzoCompressor(), new LzoDecompressor());
            case "ZSTD":
                return new AirCompressorFactory(new ZstdCompressor(), new ZstdDecompressor());
            default:
                throw new IllegalStateException("Unknown CompressionMethod " + compression);
        }
    }
}
