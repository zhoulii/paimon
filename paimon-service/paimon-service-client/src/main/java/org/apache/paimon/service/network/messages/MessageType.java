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
 * 客户端与服务端传递的消息类型.
 *
 * <p>Expected message types during the communication between {@link NetworkClient} and {@link
 * NetworkServer}.
 */
public enum MessageType {

    /** The message is a request.（请求消息）. */
    REQUEST,

    /** The message is a successful response. （请求响应消息）. */
    REQUEST_RESULT,

    /** The message indicates a protocol-related failure. （请求失败消息）. */
    REQUEST_FAILURE,

    /** The message indicates a server failure.（服务端异常消息）. */
    SERVER_FAILURE
}
