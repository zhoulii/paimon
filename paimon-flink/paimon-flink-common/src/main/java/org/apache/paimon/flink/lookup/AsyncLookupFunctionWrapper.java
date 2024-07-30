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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.utils.ExecutorThreadFactory;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 继承自 AsyncLookupFunction 接口，支持异步 lookup，底层仍是基于 NewLookupFunction 来实现.
 *
 * <p>注：paimon 的 async lookup 并不是真正的异步，由于加了同步，实际上还是一个线程在做关联.
 *
 * <p>A {@link AsyncLookupFunction} to wrap sync function.
 */
public class AsyncLookupFunctionWrapper extends AsyncLookupFunction {

    private final NewLookupFunction function;
    private final int threadNumber;

    private transient ExecutorService lazyExecutor;

    public AsyncLookupFunctionWrapper(NewLookupFunction function, int threadNumber) {
        this.function = function;
        this.threadNumber = threadNumber;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        function.open(context);
    }

    private Collection<RowData> lookup(RowData keyRow) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread()
                .setContextClassLoader(AsyncLookupFunctionWrapper.class.getClassLoader());
        try {
            // 加了同步锁，所以虽然使用了线程池，但还是同步执行
            // 之所以加锁，是因为 NewLookupFunction 关联过程中，使用到的很多对象都是非线程安全的
            // 但是也可以进行优化：
            // - 不同文件可以同时做 lookup，相同文件不支持并发 lookup
            // - 构建不同文件的 lookup file 可以并行执行，可以使用文件锁
            synchronized (function) {
                // 还是基于 NewLookupFunction 来做关联
                return function.lookup(keyRow);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        // 异步 lookup，就是丢个 task 给 executor 去做.
        return CompletableFuture.supplyAsync(() -> lookup(keyRow), executor());
    }

    @Override
    public void close() throws Exception {
        function.close();
        if (lazyExecutor != null) {
            lazyExecutor.shutdownNow();
            lazyExecutor = null;
        }
    }

    private ExecutorService executor() {
        if (lazyExecutor == null) {
            // 创建一个固定大小的线程池
            lazyExecutor =
                    Executors.newFixedThreadPool(
                            threadNumber,
                            new ExecutorThreadFactory(Thread.currentThread().getName() + "-async"));
        }
        return lazyExecutor;
    }
}
