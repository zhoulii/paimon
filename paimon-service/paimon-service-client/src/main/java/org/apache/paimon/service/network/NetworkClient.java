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

package org.apache.paimon.service.network;

import org.apache.paimon.service.network.messages.MessageBody;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.ServiceRequestStats;
import org.apache.paimon.utils.FutureUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.paimon.shade.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelInitializer;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelOption;
import org.apache.paimon.shade.netty4.io.netty.channel.EventLoopGroup;
import org.apache.paimon.shade.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.paimon.shade.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.paimon.shade.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.paimon.shade.netty4.io.netty.handler.stream.ChunkedWriteHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * 基于 NETTY 实现，用于发送请求与接收响应.
 *
 * <p>The base class for every client. It is using pure netty to send and receive messages of type
 * {@link MessageBody}.
 *
 * @param <REQ> the type of request the client will send.
 * @param <RESP> the type of response the client expects to receive.
 */
public class NetworkClient<REQ extends MessageBody, RESP extends MessageBody> {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkClient.class);

    /**
     * 客户端名称.
     *
     * <p>The name of the client. Used for logging and stack traces.
     */
    private final String clientName;

    /** Netty's Bootstrap. */
    private final Bootstrap bootstrap;

    /** The serializer to be used for (de-)serializing messages. */
    private final MessageSerializer<REQ, RESP> messageSerializer;

    /**
     * 追踪统计信息.
     *
     * <p>Statistics tracker.
     */
    private final ServiceRequestStats stats;

    // 存储连接到某台服务节点的连接.
    private final Map<InetSocketAddress, ServerConnection<REQ, RESP>> connections =
            new ConcurrentHashMap<>();

    /**
     * 标识客户端是否关闭.
     *
     * <p>Atomic shut down future.
     */
    private final AtomicReference<CompletableFuture<Void>> clientShutdownFuture =
            new AtomicReference<>(null);

    /**
     * Creates a client with the specified number of event loop threads.
     *
     * @param clientName the name of the client.
     * @param numEventLoopThreads number of event loop threads (minimum 1).
     * @param serializer the serializer used to (de-)serialize messages.
     * @param stats the statistics collector.
     */
    public NetworkClient(
            final String clientName,
            final int numEventLoopThreads,
            final MessageSerializer<REQ, RESP> serializer,
            final ServiceRequestStats stats) {

        // event loop 线程数
        Preconditions.checkArgument(
                numEventLoopThreads >= 1, "Non-positive number of event loop threads.");

        this.clientName = Preconditions.checkNotNull(clientName);
        this.messageSerializer = Preconditions.checkNotNull(serializer);
        this.stats = Preconditions.checkNotNull(stats);

        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("Paimon " + clientName + " Event Loop Thread %d")
                        .build();

        final EventLoopGroup nioGroup = new NioEventLoopGroup(numEventLoopThreads, threadFactory);
        // 用于获取 buffer，Pool 大小为 numEventLoopThreads.
        final ByteBufAllocator bufferPool = new NettyBufferPool(numEventLoopThreads);

        this.bootstrap =
                new Bootstrap()
                        .group(nioGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.ALLOCATOR, bufferPool)
                        .handler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel channel) {
                                        // LengthFieldBasedFrameDecoder：用于根据消息的长度字段对数据帧进行解码，能解决拆包、粘包问题
                                        // ChunkedWriteHandler：将较大的消息流分块写出
                                        channel.pipeline()
                                                .addLast(
                                                        new LengthFieldBasedFrameDecoder(
                                                                Integer.MAX_VALUE, 0, 4, 0, 4))
                                                .addLast(new ChunkedWriteHandler());
                                    }
                                });
    }

    public String getClientName() {
        return clientName;
    }

    public CompletableFuture<RESP> sendRequest(
            final InetSocketAddress serverAddress, final REQ request) {
        if (clientShutdownFuture.get() != null) {
            // 客户端已关闭
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(clientName + " is already shut down."));
        }

        final ServerConnection<REQ, RESP> connection =
                connections.computeIfAbsent(
                        serverAddress,
                        ignored -> {
                            // 创建一个 Pending Connection
                            final ServerConnection<REQ, RESP> newConnection =
                                    ServerConnection.createPendingConnection(
                                            clientName, messageSerializer, stats);

                            // 建立连接，ServerConnection 内部 Connection 类型发生改变
                            bootstrap
                                    .connect(serverAddress.getAddress(), serverAddress.getPort())
                                    .addListener(
                                            (ChannelFutureListener)
                                                    newConnection::establishConnection);

                            // 注册关闭回调
                            newConnection
                                    .getCloseFuture()
                                    .handle(
                                            (ignoredA, ignoredB) ->
                                                    connections.remove(
                                                            serverAddress, newConnection));

                            return newConnection;
                        });

        // 通过连接发送请求
        return connection.sendRequest(request);
    }

    /**
     * 关闭 client 及所有的连接.
     *
     * <p>Shuts down the client and closes all connections.
     *
     * <p>After a call to this method, all returned futures will be failed.
     *
     * @return A {@link CompletableFuture} that will be completed when the shutdown process is done.
     */
    public CompletableFuture<Void> shutdown() {
        final CompletableFuture<Void> newShutdownFuture = new CompletableFuture<>();
        // 原子性关闭
        if (clientShutdownFuture.compareAndSet(null, newShutdownFuture)) {

            final List<CompletableFuture<Void>> connectionFutures = new ArrayList<>();

            for (Map.Entry<InetSocketAddress, ServerConnection<REQ, RESP>> conn :
                    connections.entrySet()) {
                if (connections.remove(conn.getKey(), conn.getValue())) {
                    // 关闭 connection，返回关闭 CompletableFuture.
                    connectionFutures.add(conn.getValue().close());
                }
            }

            CompletableFuture.allOf(connectionFutures.toArray(new CompletableFuture<?>[0]))
                    // 所有连接都关闭之后
                    .whenComplete(
                            (result, throwable) -> {
                                // 发生异常
                                if (throwable != null) {
                                    LOG.warn(
                                            "Problem while shutting down the connections at the {}: {}",
                                            clientName,
                                            throwable);
                                }

                                // 关闭 NIO 线程组
                                if (bootstrap != null) {
                                    EventLoopGroup group = bootstrap.config().group();
                                    if (group != null && !group.isShutdown()) {
                                        // 等待时间、超时时间
                                        group.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS)
                                                .addListener(
                                                        // 设置关闭监听
                                                        finished -> {
                                                            if (finished.isSuccess()) {
                                                                newShutdownFuture.complete(null);
                                                            } else {
                                                                newShutdownFuture
                                                                        .completeExceptionally(
                                                                                finished.cause());
                                                            }
                                                        });
                                    } else {
                                        newShutdownFuture.complete(null);
                                    }
                                } else {
                                    newShutdownFuture.complete(null);
                                }
                            });

            // 返回关闭 Future
            return newShutdownFuture;
        }
        return clientShutdownFuture.get();
    }

    public boolean isEventGroupShutdown() {
        // 客户端是否关闭
        return bootstrap == null || bootstrap.config().group().isTerminated();
    }
}
