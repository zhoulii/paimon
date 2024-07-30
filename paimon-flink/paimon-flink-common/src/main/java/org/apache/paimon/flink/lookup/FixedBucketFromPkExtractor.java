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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 从 PK 中提取 bucket. (BUCKET KEY 必须是 PRIMARY KEY 的子集)
 *
 * <p>Extractor to extract bucket from the primary key.
 */
public class FixedBucketFromPkExtractor implements KeyAndBucketExtractor<InternalRow> {

    // 主键
    private transient InternalRow primaryKey;

    // bucket key 是否等于删除了 partition key 的 primary key
    private final boolean sameBucketKeyAndTrimmedPrimaryKey;

    // bucket 数量
    private final int numBuckets;

    // 从 primary key 中提取 bucket 的 projection
    private final Projection bucketKeyProjection;

    // 从 primary key 中提取 trimmed primary key 的 projection
    private final Projection trimmedPrimaryKeyProjection;

    // 从 primary key 中提取 partition 的 projection
    private final Projection partitionProjection;

    // 提取 log primary key 的 projection
    private final Projection logPrimaryKeyProjection;

    public FixedBucketFromPkExtractor(TableSchema schema) {
        this.numBuckets = new CoreOptions(schema.options()).bucket();
        checkArgument(numBuckets > 0, "Num bucket is illegal: " + numBuckets);
        this.sameBucketKeyAndTrimmedPrimaryKey =
                schema.bucketKeys().equals(schema.trimmedPrimaryKeys());
        // 从 primary key 中提取 bucket 的 projection
        this.bucketKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalPrimaryKeysType(),
                        schema.bucketKeys().stream()
                                .mapToInt(schema.primaryKeys()::indexOf)
                                .toArray());
        // 从 primary key 中提取 trimmed primary key 的 projection
        this.trimmedPrimaryKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalPrimaryKeysType(),
                        schema.trimmedPrimaryKeys().stream()
                                .mapToInt(schema.primaryKeys()::indexOf)
                                .toArray());
        // 从 primary key 中提取 partition 的 projection
        this.partitionProjection =
                CodeGenUtils.newProjection(
                        schema.logicalPrimaryKeysType(),
                        schema.partitionKeys().stream()
                                .mapToInt(schema.primaryKeys()::indexOf)
                                .toArray());
        // 提取 log primary key 的 projection
        this.logPrimaryKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.primaryKeys()));
    }

    // 设置 primary key
    @Override
    public void setRecord(InternalRow record) {
        this.primaryKey = record;
    }

    @Override
    public BinaryRow partition() {
        // 提取 partition
        return partitionProjection.apply(primaryKey);
    }

    private BinaryRow bucketKey() {
        // 提取 bucket KEY
        if (sameBucketKeyAndTrimmedPrimaryKey) {
            return trimmedPrimaryKey();
        }

        return bucketKeyProjection.apply(primaryKey);
    }

    @Override
    public int bucket() {
        // 计算 bucket
        BinaryRow bucketKey = bucketKey();
        return KeyAndBucketExtractor.bucket(
                KeyAndBucketExtractor.bucketKeyHashCode(bucketKey), numBuckets);
    }

    @Override
    public BinaryRow trimmedPrimaryKey() {
        // 获取 trimmed primary key
        return trimmedPrimaryKeyProjection.apply(primaryKey);
    }

    @Override
    public BinaryRow logPrimaryKey() {
        // 获取 log primary key
        return logPrimaryKeyProjection.apply(primaryKey);
    }
}
