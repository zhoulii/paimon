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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 在维表 join 时，用于 lookup 数据，并能够刷新 table 的数据.
 *
 * <p>A lookup table which provides get and refresh.
 */
public interface LookupTable extends Closeable {

    // 根据 partition 过滤
    void specificPartitionFilter(Predicate filter);

    void open() throws Exception;

    // 根据 key 获取数据
    List<InternalRow> get(InternalRow key) throws IOException;

    // 刷新表数据
    void refresh() throws Exception;
}
