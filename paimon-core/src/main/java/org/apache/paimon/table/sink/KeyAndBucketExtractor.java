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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 用于提取 partition, bucket, primary keys.
 *
 * <p>Utility interface to extract partition keys, bucket id, primary keys for file store ({@code
 * trimmedPrimaryKey}) and primary keys for external log system ({@code logPrimaryKey}) from the
 * given record.
 *
 * @param <T> type of record
 */
public interface KeyAndBucketExtractor<T> {

    // 传进来一条数据
    void setRecord(T record);

    // 获取 partition
    BinaryRow partition();

    // 获取 bucket
    int bucket();

    // 获取删除分区字段的 primary key
    BinaryRow trimmedPrimaryKey();

    // 日志的 primary key
    BinaryRow logPrimaryKey();

    // bucket 的 HASH 值
    static int bucketKeyHashCode(BinaryRow bucketKey) {
        assert bucketKey.getRowKind() == RowKind.INSERT;
        return bucketKey.hashCode();
    }

    // 计算 bucket
    static int bucket(int hashcode, int numBuckets) {
        checkArgument(numBuckets > 0, "Num bucket is illegal: " + numBuckets);
        return Math.abs(hashcode % numBuckets);
    }
}
