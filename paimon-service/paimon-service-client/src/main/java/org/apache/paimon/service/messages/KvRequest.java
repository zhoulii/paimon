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

package org.apache.paimon.service.messages;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.service.network.messages.MessageBody;
import org.apache.paimon.service.network.messages.MessageDeserializer;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.service.network.messages.MessageDeserializer.readBytes;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * 表示根据 key 查询 value 的请求消息.
 *
 * <p>通过 partition + bucket 来表示查询那个桶的数据.
 *
 * <p>The request to query values for keys.
 */
public class KvRequest extends MessageBody {

    private final BinaryRow partition;
    private final int bucket;
    private final BinaryRow[] keys;

    public KvRequest(BinaryRow partition, int bucket, BinaryRow[] keys) {
        this.partition = partition;
        this.bucket = bucket;
        this.keys = keys;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public BinaryRow[] keys() {
        return keys;
    }

    // partition length - partition bytes + bucket + key num - (key1 length + key1 bytes) - ...
    @Override
    public byte[] serialize() {
        // 消息序列化后的大小
        int size = 0;

        byte[] partitionBytes = serializeBinaryRow(partition);
        // 4 用来存储 partition 的长度，partitionBytes.length 用来存储 partition 的字节数组
        size += 4 + partitionBytes.length;

        // bucket
        // bucket 使用 4 个字节表示
        size += 4;

        // key size
        // key 的数量使用一个字节表示
        size += 4;

        List<byte[]> keyBytesList = new ArrayList<>();
        for (BinaryRow key : keys) {
            byte[] keyBytes = serializeBinaryRow(key);
            keyBytesList.add(keyBytes);
            size += 4 + keyBytes.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(partitionBytes.length)
                .put(partitionBytes)
                .putInt(bucket)
                .putInt(keyBytesList.size());

        for (byte[] keyBytes : keyBytesList) {
            buffer.putInt(keyBytes.length).put(keyBytes);
        }

        return buffer.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KvRequest kvRequest = (KvRequest) o;
        return bucket == kvRequest.bucket
                && Objects.equals(partition, kvRequest.partition)
                && Arrays.equals(keys, kvRequest.keys);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(partition, bucket);
        result = 31 * result + Arrays.hashCode(keys);
        return result;
    }

    /**
     * 反序列化 KVRequest 消息.
     *
     * <p>A {@link MessageDeserializer deserializer} for {@link KvRequest}.
     */
    public static class KvRequestDeserializer implements MessageDeserializer<KvRequest> {

        @Override
        public KvRequest deserializeMessage(ByteBuf buf) {
            BinaryRow partition = deserializeBinaryRow(readBytes(buf, buf.readInt()));
            int bucket = buf.readInt();

            int keySize = buf.readInt();
            List<BinaryRow> keys = new ArrayList<>(keySize);
            for (int i = 0; i < keySize; i++) {
                keys.add(deserializeBinaryRow(readBytes(buf, buf.readInt())));
            }

            return new KvRequest(partition, bucket, keys.toArray(new BinaryRow[0]));
        }
    }
}
