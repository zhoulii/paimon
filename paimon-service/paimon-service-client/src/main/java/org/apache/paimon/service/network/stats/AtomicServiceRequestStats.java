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

package org.apache.paimon.service.network.stats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计连接和请求信息，支持原子性更新.
 *
 * <p>Atomic {@link ServiceRequestStats} implementation.
 */
public class AtomicServiceRequestStats implements ServiceRequestStats {

    /** Number of active connections. （建立的连接数）. */
    private final AtomicLong numConnections = new AtomicLong();

    /** Total number of reported requests. （上报请求数）. */
    private final AtomicLong numRequests = new AtomicLong();

    /** Total number of successful requests (<= reported requests).（成功请求数）. */
    private final AtomicLong numSuccessful = new AtomicLong();

    /** Total duration of all successful requests. （成功请求总耗时）. */
    private final AtomicLong successfulDuration = new AtomicLong();

    /** Total number of failed requests (<= reported requests). （失败请求书）. */
    private final AtomicLong numFailed = new AtomicLong();

    // 建立连接时更新
    @Override
    public void reportActiveConnection() {
        numConnections.incrementAndGet();
    }

    // 断开连接时更新，这里不应该使用 numConnections
    @Override
    public void reportInactiveConnection() {
        numConnections.decrementAndGet();
    }

    @Override
    public void reportRequest() {
        numRequests.incrementAndGet();
    }

    @Override
    public void reportSuccessfulRequest(long durationTotalMillis) {
        numSuccessful.incrementAndGet();
        successfulDuration.addAndGet(durationTotalMillis);
    }

    @Override
    public void reportFailedRequest() {
        numFailed.incrementAndGet();
    }

    public long getNumConnections() {
        return numConnections.get();
    }

    public long getNumRequests() {
        return numRequests.get();
    }

    public long getNumSuccessful() {
        return numSuccessful.get();
    }

    public long getNumFailed() {
        return numFailed.get();
    }

    @Override
    public String toString() {
        return "AtomicServiceRequestStats{"
                + "numConnections="
                + numConnections
                + ", numRequests="
                + numRequests
                + ", numSuccessful="
                + numSuccessful
                + ", numFailed="
                + numFailed
                + '}';
    }
}
