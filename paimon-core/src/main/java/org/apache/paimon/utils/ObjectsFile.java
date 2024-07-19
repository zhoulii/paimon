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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.utils.FileUtils.createFormatReader;

/**
 * 对象文件，实现类有 ManifestFile、ManifestList、IndexManifestFile.
 *
 * <p>ObjectsFile 也可以不依赖子类直接写出对象，写出的对象为 SimpleEntry.
 *
 * <p>A file which contains several {@link T}s, provides read and write.
 */
public class ObjectsFile<T> {

    protected final FileIO fileIO;
    protected final ObjectSerializer<T> serializer;
    protected final FormatReaderFactory readerFactory;
    protected final FormatWriterFactory writerFactory;
    protected final PathFactory pathFactory;

    @Nullable private final ObjectsCache<String, T> cache;

    public ObjectsFile(
            FileIO fileIO,
            ObjectSerializer<T> serializer,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            PathFactory pathFactory,
            @Nullable SegmentsCache<String> cache) {
        this.fileIO = fileIO;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.pathFactory = pathFactory;
        this.cache =
                cache == null ? null : new ObjectsCache<>(cache, serializer, this::createIterator);
    }

    public long fileSize(String fileName) {
        try {
            // 获取文件大小
            return fileIO.getFileSize(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<T> read(String fileName) {
        // 省略 filesize
        return read(fileName, null);
    }

    public List<T> read(String fileName, @Nullable Long fileSize) {
        // 没有过滤条件
        return read(fileName, fileSize, Filter.alwaysTrue(), Filter.alwaysTrue());
    }

    public List<T> readWithIOException(String fileName) throws IOException {
        // 省略 filesize
        return readWithIOException(fileName, null);
    }

    public List<T> readWithIOException(String fileName, @Nullable Long fileSize)
            throws IOException {
        // 没有过滤条件
        return readWithIOException(fileName, fileSize, Filter.alwaysTrue(), Filter.alwaysTrue());
    }

    public boolean exists(String fileName) {
        try {
            // 判断文件是否存在.
            return fileIO.exists(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<T> read(
            String fileName,
            @Nullable Long fileSize,
            Filter<InternalRow> loadFilter,
            Filter<InternalRow> readFilter) {
        try {
            return readWithIOException(fileName, fileSize, loadFilter, readFilter);
        } catch (IOException e) {
            // 捕获 IO 异常，日志信息有点问题.
            throw new RuntimeException("Failed to read manifest list " + fileName, e);
        }
    }

    private List<T> readWithIOException(
            String fileName,
            @Nullable Long fileSize,
            Filter<InternalRow> loadFilter,
            Filter<InternalRow> readFilter)
            throws IOException {
        // 缓存不为空，表示启用缓存，从缓存读取.
        if (cache != null) {
            return cache.read(fileName, fileSize, loadFilter, readFilter);
        }

        RecordReader<InternalRow> reader =
                createFormatReader(fileIO, readerFactory, pathFactory.toPath(fileName), fileSize);
        if (readFilter != Filter.ALWAYS_TRUE) {
            reader = reader.filter(readFilter);
        }
        List<T> result = new ArrayList<>();
        // 读取剩余的数据，并将其转换为特定对象存储到 result 中.
        reader.forEachRemaining(row -> result.add(serializer.fromRow(row)));
        return result;
    }

    public String writeWithoutRolling(Collection<T> records) {
        // 不滚动写
        return writeWithoutRolling(records.iterator());
    }

    public String writeWithoutRolling(Iterator<T> records) {
        // 不滚动写
        Path path = pathFactory.newPath();
        try {
            try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
                // 创建一个 FormatWriter，写出到 PositionOutputStream
                FormatWriter writer =
                        writerFactory.create(out, CoreOptions.FILE_COMPRESSION.defaultValue());
                try {
                    while (records.hasNext()) {
                        writer.addElement(serializer.toRow(records.next()));
                    }
                } finally {
                    writer.flush();
                    writer.finish();
                }
            }
            return path.getName();
        } catch (Throwable e) {
            fileIO.deleteQuietly(path);
            throw new RuntimeException(
                    "Exception occurs when writing records to " + path + ". Clean up.", e);
        }
    }

    private CloseableIterator<InternalRow> createIterator(
            String fileName, @Nullable Long fileSize) {
        try {
            // 读取一个文件为 InternalRow Iterator，读取长度为 fileSize.
            return createFormatReader(fileIO, readerFactory, pathFactory.toPath(fileName), fileSize)
                    .toCloseableIterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(String fileName) {
        // 删除一个文件
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }
}
