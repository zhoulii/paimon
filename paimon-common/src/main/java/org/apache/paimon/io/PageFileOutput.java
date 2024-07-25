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

package org.apache.paimon.io;

import org.apache.paimon.compression.BlockCompressionFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * 按 page 写出字节到文件.
 *
 * <p>An interface to write bytes with pages into file.
 */
public interface PageFileOutput extends Closeable {

    void write(byte[] bytes, int off, int len) throws IOException;

    static PageFileOutput create(
            File file, int pageSize, @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        if (compressionFactory == null) {
            // 不压缩写出
            return new UncompressedPageFileOutput(file);
        }
        // 压缩写出
        return new CompressedPageFileOutput(file, pageSize, compressionFactory);
    }
}
