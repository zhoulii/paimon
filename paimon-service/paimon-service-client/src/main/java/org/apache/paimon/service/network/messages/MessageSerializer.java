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

package org.apache.paimon.service.network.messages;

import org.apache.paimon.service.network.NetworkClient;
import org.apache.paimon.service.network.NetworkServer;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;
import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Query Service 使用的消息的序列化/反序列化器.
 *
 * <p>Serialization and deserialization of messages exchanged between {@link NetworkClient} and
 * {@link NetworkServer}.
 *
 * <p>The binary messages have the following format:
 *
 * <p>消息长度 - 版本号 - 消息类型(MessageType) - 消息内容.
 *
 * <pre>
 *                     <------ Frame ------------------------->
 *                    +----------------------------------------+
 *                    |        HEADER (8)      | PAYLOAD (VAR) |
 * +------------------+----------------------------------------+
 * | FRAME LENGTH (4) | VERSION (4) | TYPE (4) | CONTENT (VAR) |
 * +------------------+----------------------------------------+
 * </pre>
 *
 * <p>The concrete content of a message depends on the {@link MessageType}.
 *
 * @param <REQ> Type of the requests of the protocol.
 * @param <RESP> Type of the responses of the protocol.
 */
public final class MessageSerializer<REQ extends MessageBody, RESP extends MessageBody> {

    /** The serialization version ID. */
    private static final int VERSION = 1; // 版本占 4 个字节

    /** Byte length of the header. */
    private static final int HEADER_LENGTH = 2 * Integer.BYTES; // header 占 8 个字节

    /** Byte length of the request id. */
    private static final int REQUEST_ID_SIZE = Long.BYTES; // request id 占 8 个字节

    /**
     * Request 的反序列化器.
     *
     * <p>The constructor of the {@link MessageBody client requests}. Used for deserialization.
     */
    private final MessageDeserializer<REQ> requestDeserializer;

    /**
     * Response 的反序列化器.
     *
     * <p>The constructor of the {@link MessageBody server responses}. Used for deserialization.
     */
    private final MessageDeserializer<RESP> responseDeserializer;

    public MessageSerializer(
            MessageDeserializer<REQ> requestDeser, MessageDeserializer<RESP> responseDeser) {
        requestDeserializer = Preconditions.checkNotNull(requestDeser);
        responseDeserializer = Preconditions.checkNotNull(responseDeser);
    }

    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    /**
     * 序列化 Request.
     *
     * <p>Serializes the request sent to the {@link NetworkServer}.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param requestId The id of the request to which the message refers to.
     * @param request The request to be serialized.
     * @return A {@link ByteBuf} containing the serialized message.
     */
    public static <REQ extends MessageBody> ByteBuf serializeRequest(
            final ByteBufAllocator alloc, final long requestId, final REQ request) {
        Preconditions.checkNotNull(request);
        return writePayload(alloc, requestId, MessageType.REQUEST, request.serialize());
    }

    /**
     * 序列化响应消息.
     *
     * <p>Serializes the response sent to the {@link NetworkClient}.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param requestId The id of the request to which the message refers to.
     * @param response The response to be serialized.
     * @return A {@link ByteBuf} containing the serialized message.
     */
    public static <RESP extends MessageBody> ByteBuf serializeResponse(
            final ByteBufAllocator alloc, final long requestId, final RESP response) {
        Preconditions.checkNotNull(response);
        return writePayload(alloc, requestId, MessageType.REQUEST_RESULT, response.serialize());
    }

    /**
     * 序列化异常消息.
     *
     * <p>Serializes the exception containing the failure message sent to the {@link NetworkClient}
     * in case of protocol related errors.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param requestId The id of the request to which the message refers to.
     * @param cause The exception thrown at the server.
     * @return A {@link ByteBuf} containing the serialized message.
     */
    public static ByteBuf serializeRequestFailure(
            final ByteBufAllocator alloc, final long requestId, final Throwable cause)
            throws IOException {

        final ByteBuf buf = alloc.ioBuffer();

        // 预留位，用于写 frame 长度
        // Frame length is set at the end
        buf.writeInt(0);
        // 写入头信息
        writeHeader(buf, MessageType.REQUEST_FAILURE);
        // 写入 request id
        buf.writeLong(requestId);

        try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
                ObjectOutput out = new ObjectOutputStream(bbos)) {
            // 写入异常对象
            out.writeObject(cause);
        }

        // 设置消息长度
        // Set frame length
        int frameLength = buf.readableBytes() - Integer.BYTES;
        buf.setInt(0, frameLength);
        return buf;
    }

    /**
     * 序列化服务端异常消息，整体流程和写入异常消息一样，区别在于不需要写 request id.
     *
     * <p>Serializes the failure message sent to the {@link NetworkClient} in case of server related
     * errors.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param cause The exception thrown at the server.
     * @return The failure message.
     */
    public static ByteBuf serializeServerFailure(
            final ByteBufAllocator alloc, final Throwable cause) throws IOException {

        final ByteBuf buf = alloc.ioBuffer();

        // Frame length is set at end
        buf.writeInt(0);
        writeHeader(buf, MessageType.SERVER_FAILURE);

        try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
                ObjectOutput out = new ObjectOutputStream(bbos)) {
            out.writeObject(cause);
        }

        // Set frame length
        int frameLength = buf.readableBytes() - Integer.BYTES;
        buf.setInt(0, frameLength);
        return buf;
    }

    /**
     * header 内容：版本号 + 消息类型. Helper for serializing the header.
     *
     * @param buf The {@link ByteBuf} to serialize the header into.
     * @param messageType The {@link MessageType} of the message this header refers to.
     */
    private static void writeHeader(final ByteBuf buf, final MessageType messageType) {
        buf.writeInt(VERSION);
        buf.writeInt(messageType.ordinal());
    }

    /**
     * 序列化消息主体.
     *
     * <p>Helper for serializing the messages.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param requestId The id of the request to which the message refers to.
     * @param messageType The {@link MessageType type of the message}.
     * @param payload The serialized version of the message.
     * @return A {@link ByteBuf} containing the serialized message.
     */
    private static ByteBuf writePayload(
            final ByteBufAllocator alloc,
            final long requestId,
            final MessageType messageType,
            final byte[] payload) {

        // 数据帧长度
        final int frameLength = HEADER_LENGTH + REQUEST_ID_SIZE + payload.length;
        // 消息序列化后的完整长度
        final ByteBuf buf = alloc.ioBuffer(frameLength + Integer.BYTES);

        buf.writeInt(frameLength); // 消息长度
        writeHeader(buf, messageType); // 消息头
        buf.writeLong(requestId); // 消息 id
        buf.writeBytes(payload); // 消息主体
        return buf;
    }

    // ------------------------------------------------------------------------
    // Deserialization
    // ------------------------------------------------------------------------

    /**
     * 反序列化消息类型.
     *
     * <p>De-serializes the header and returns the {@link MessageType}.
     *
     * <pre>
     *  <b>The buffer is expected to be at the header position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized header.
     * @return The message type.
     * @throws IllegalStateException If unexpected message version or message type.
     */
    public static MessageType deserializeHeader(final ByteBuf buf) {

        // checking the version
        int version = buf.readInt();
        Preconditions.checkState(
                version == VERSION,
                "Version Mismatch:  Found " + version + ", Expected: " + VERSION + '.');

        // fetching the message type
        int msgType = buf.readInt();
        MessageType[] values = MessageType.values();
        Preconditions.checkState(
                msgType >= 0 && msgType < values.length,
                "Illegal message type with index " + msgType + '.');
        return values[msgType];
    }

    /**
     * 获取 request id.
     *
     * <p>De-serializes the header and returns the {@link MessageType}.
     *
     * <pre>
     *  <b>The buffer is expected to be at the request id position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized request id.
     * @return The request id.
     */
    public static long getRequestId(final ByteBuf buf) {
        return buf.readLong();
    }

    /**
     * 反序列化 request，参数 buf 的 position 位于消息主体开始.
     *
     * <p>De-serializes the request sent to the {@link NetworkServer}.
     *
     * <pre>
     *  <b>The buffer is expected to be at the request position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized request.
     * @return The request.
     */
    public REQ deserializeRequest(final ByteBuf buf) {
        Preconditions.checkNotNull(buf);
        return requestDeserializer.deserializeMessage(buf);
    }

    /**
     * 反序列化 reponse，参数 buf 的 position 位于消息主体开始.
     *
     * <p>De-serializes the response sent to the {@link NetworkClient}.
     *
     * <pre>
     *  <b>The buffer is expected to be at the response position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized response.
     * @return The response.
     */
    public RESP deserializeResponse(final ByteBuf buf) {
        Preconditions.checkNotNull(buf);
        return responseDeserializer.deserializeMessage(buf);
    }

    /**
     * 反序列化异常响应，参数 buf 的 position 位于消息主体开始.
     *
     * <p>De-serializes the {@link RequestFailure} sent to the {@link NetworkClient} in case of
     * protocol related errors.
     *
     * <pre>
     *  <b>The buffer is expected to be at the correct position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized failure message.
     * @return The failure message.
     */
    public static RequestFailure deserializeRequestFailure(final ByteBuf buf)
            throws IOException, ClassNotFoundException {
        long requestId = buf.readLong();

        Throwable cause;
        try (ByteBufInputStream bis = new ByteBufInputStream(buf);
                ObjectInputStream in = new ObjectInputStream(bis)) {
            cause = (Throwable) in.readObject();
        }
        return new RequestFailure(requestId, cause);
    }

    /**
     * 反序列化服务器异常响应，参数 buf 的 position 位于消息主体开始.
     *
     * <p>De-serializes the failure message sent to the {@link NetworkClient} in case of server
     * related errors.
     *
     * <pre>
     *  <b>The buffer is expected to be at the correct position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized failure message.
     * @return The failure message.
     */
    public static Throwable deserializeServerFailure(final ByteBuf buf)
            throws IOException, ClassNotFoundException {
        try (ByteBufInputStream bis = new ByteBufInputStream(buf);
                ObjectInputStream in = new ObjectInputStream(bis)) {
            return (Throwable) in.readObject();
        }
    }
}
