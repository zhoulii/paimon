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

/**
 * 客户端与服务端传递的消息类.
 *
 * <p>The base class for every message exchanged during the communication between {@link
 * NetworkClient} and {@link NetworkServer}.
 *
 * <p>Every such message should also have a {@link MessageDeserializer}.
 */
public abstract class MessageBody {

    /**
     * 获取消息的序列化后的字节数组.
     *
     * <p>Serializes the message into a byte array.
     *
     * @return A byte array with the serialized content of the message.
     */
    public abstract byte[] serialize();
}
