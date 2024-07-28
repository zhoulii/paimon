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

package org.apache.paimon.query;

import org.apache.paimon.data.BinaryRow;

import java.net.InetSocketAddress;

/**
 * bucket 所在远程服务的地址.
 *
 * <p>An interface to get query location.
 */
public interface QueryLocation {

    /**
     * 获取 partition and bucket 的远程服务地址，forceUpdate 是否强制更新地址缓存.
     *
     * <p>Get location from partition and bucket.
     *
     * @param forceUpdate whether to refresh location cache.
     */
    InetSocketAddress getLocation(BinaryRow partition, int bucket, boolean forceUpdate);
}
