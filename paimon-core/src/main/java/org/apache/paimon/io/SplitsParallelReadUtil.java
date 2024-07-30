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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ParallelExecution;
import org.apache.paimon.utils.ParallelExecution.ParallelBatch;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 并发读取 Split.
 *
 * <p>An util class to wrap {@link ParallelExecution} to parallel execution for {@link Split}
 * reader.
 */
public class SplitsParallelReadUtil {

    public static RecordReader<InternalRow> parallelExecute(
            RowType projectedType,
            FunctionWithIOException<Split, RecordReader<InternalRow>> readBuilder,
            List<Split> splits,
            int pageSize,
            int parallelism) {
        return parallelExecute(
                projectedType,
                readBuilder,
                splits,
                pageSize,
                parallelism,
                split -> null,
                (row, unused) -> row);
    }

    // extraFunction: 用于根据 Split 来生成一些额外信息，如根据 Split 来获取 partition 和 bucket 信息.
    // addExtraToRow：将 extraFunction 提取到的信息拼接到数据 row 中.
    public static <EXTRA> RecordReader<InternalRow> parallelExecute(
            RowType projectedType,
            FunctionWithIOException<Split, RecordReader<InternalRow>> readBuilder,
            List<Split> splits,
            int pageSize,
            int parallelism,
            Function<Split, EXTRA> extraFunction,
            BiFunction<InternalRow, EXTRA, InternalRow> addExtraToRow) {
        // 创建每个 Split 的 reader，并封装一些从 Split 提取到的信息
        List<Supplier<Pair<RecordReader<InternalRow>, EXTRA>>> suppliers = new ArrayList<>();
        for (Split split : splits) {
            suppliers.add(
                    () -> {
                        try {
                            RecordReader<InternalRow> reader = readBuilder.apply(split);
                            return Pair.of(reader, extraFunction.apply(split));
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }

        ParallelExecution<InternalRow, EXTRA> execution =
                new ParallelExecution<>(
                        InternalSerializers.create(projectedType),
                        pageSize,
                        parallelism,
                        suppliers);

        return new RecordReader<InternalRow>() {
            @Nullable
            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                ParallelBatch<InternalRow, EXTRA> batch;
                try {
                    batch = execution.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }

                if (batch == null) {
                    return null;
                }

                // 将 ParallelBatch 转换为 RecordIterator，实际上就是使用 ParallelBatch 的 next() 方法来获取数据
                // 然后拼接额外信息
                return new RecordIterator<InternalRow>() {
                    @Nullable
                    @Override
                    public InternalRow next() throws IOException {
                        InternalRow row = batch.next();
                        if (row == null) {
                            return null;
                        }

                        return addExtraToRow.apply(row, batch.extraMessage());
                    }

                    @Override
                    public void releaseBatch() {
                        batch.releaseBatch();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                execution.close();
            }
        };
    }
}
