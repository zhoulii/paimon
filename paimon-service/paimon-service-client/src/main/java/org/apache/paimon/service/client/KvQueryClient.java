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

package org.apache.paimon.service.client;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.query.QueryLocation;
import org.apache.paimon.service.exceptions.UnknownPartitionBucketException;
import org.apache.paimon.service.messages.KvRequest;
import org.apache.paimon.service.messages.KvResponse;
import org.apache.paimon.service.network.NetworkClient;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.utils.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 从远程服务获取 KV 值的客户端.
 *
 * <p>A class for the Client to get values from Servers.
 */
public class KvQueryClient {

    private static final Logger LOG = LoggerFactory.getLogger(KvQueryClient.class);

    private final NetworkClient<KvRequest, KvResponse> networkClient;
    // bucket 所在服务地址
    private final QueryLocation queryLocation;

    public KvQueryClient(QueryLocation queryLocation, int numEventLoopThreads) {
        this.queryLocation = queryLocation;
        final MessageSerializer<KvRequest, KvResponse> messageSerializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());

        this.networkClient =
                new NetworkClient<>(
                        "Kv Query Client",
                        numEventLoopThreads,
                        messageSerializer,
                        new DisabledServiceRequestStats());
    }

    public CompletableFuture<BinaryRow[]> getValues(
            BinaryRow partition, int bucket, BinaryRow[] keys) {
        CompletableFuture<BinaryRow[]> response = new CompletableFuture<>();
        // 异步执行查询，update 表示是否强制刷新缓存的服务地址
        executeActionAsync(response, new KvRequest(partition, bucket, keys), false);
        return response;
    }

    private void executeActionAsync(
            final CompletableFuture<BinaryRow[]> result,
            final KvRequest request,
            final boolean update) {
        if (!result.isDone()) {
            final CompletableFuture<KvResponse> operationFuture = getResponse(request, update);
            operationFuture.whenCompleteAsync(
                    (t, throwable) -> {
                        if (throwable != null) {
                            if (throwable instanceof UnknownPartitionBucketException
                                    || throwable.getCause() instanceof ConnectException) {
                                // 服务地址不同步导致的，进行重试
                                // These failures are likely to be caused by out-of-sync location.
                                // Therefore, we retry this query and force lookup the location.

                                LOG.debug(
                                        "Retrying after failing to retrieve state due to: {}.",
                                        throwable.getMessage());
                                executeActionAsync(result, request, true);
                            } else {
                                // 其他异常则直接失败
                                result.completeExceptionally(throwable);
                            }
                        } else {
                            // 保存查询结果
                            result.complete(t.values());
                        }
                    });

            // 注册获取到结果时的回调，关闭之前的请求操作
            // 什么情况下会发生已经获取到结果，而 operationFuture 没关闭的情况呢?
            result.whenComplete((t, throwable) -> operationFuture.cancel(false));
        }
    }

    private CompletableFuture<KvResponse> getResponse(
            final KvRequest request, final boolean forceUpdate) {
        // 获取服务地址
        InetSocketAddress serverAddress =
                queryLocation.getLocation(request.partition(), request.bucket(), forceUpdate);
        if (serverAddress == null) {
            return FutureUtils.completedExceptionally(
                    new RuntimeException("Cannot find address for bucket: " + request.bucket()));
        }
        // 发送请求
        return networkClient.sendRequest(serverAddress, request);
    }

    public void shutdown() {
        // 关闭客户端
        try {
            networkClient.shutdown().get(10L, TimeUnit.SECONDS);
            LOG.info("{} was shutdown successfully.", networkClient.getClientName());
        } catch (Exception e) {
            LOG.warn(String.format("%s shutdown failed.", networkClient.getClientName()), e);
        }
    }
}
