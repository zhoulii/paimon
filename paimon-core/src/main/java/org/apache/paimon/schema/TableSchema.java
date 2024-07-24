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

package org.apache.paimon.schema;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET_KEY;

/**
 * 相比 Schema，会包含更多信息，如 schemaId.
 *
 * <p>Schema of a table. Unlike schema, it has more information than {@link Schema}, including
 * schemaId and fieldId.
 */
public class TableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int PAIMON_07_VERSION = 1; // PAIMON 0.7 及之前使用的版本
    public static final int CURRENT_VERSION = 2; // 当前版本

    // version of schema for paimon
    private final int version; // 使用 schema 的版本号

    private final long id; // schema id

    private final List<DataField> fields;

    /** Not available from fields, as some fields may have been deleted. */
    private final int highestFieldId; // 最大的 field id，不一定是所有字段中最大的 id，因为有可能删除了某些字段

    private final List<String> partitionKeys;

    private final List<String> primaryKeys;

    private final Map<String, String> options;

    private final @Nullable String comment;

    private final long timeMillis; // 创建时间

    public TableSchema(
            long id,
            List<DataField> fields,
            int highestFieldId,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            @Nullable String comment) {
        this(
                CURRENT_VERSION,
                id,
                fields,
                highestFieldId,
                partitionKeys,
                primaryKeys,
                options,
                comment,
                System.currentTimeMillis());
    }

    public TableSchema(
            int version,
            long id,
            List<DataField> fields,
            int highestFieldId,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            @Nullable String comment,
            long timeMillis) {
        this.version = version;
        this.id = id;
        this.fields = fields;
        this.highestFieldId = highestFieldId;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.options = Collections.unmodifiableMap(options);
        this.comment = comment;
        this.timeMillis = timeMillis;

        // try to trim to validate primary keys
        trimmedPrimaryKeys(); // 验证 primary keys 不能和 partition keys 完全相同

        // try to validate bucket keys
        originalBucketKeys(); // 验证 bucket-key 与 primary key 及 partition 之间的关系
    }

    public int version() {
        return version;
    }

    public long id() {
        return id;
    }

    public List<DataField> fields() {
        return fields;
    }

    public List<String> fieldNames() {
        return fields.stream().map(DataField::name).collect(Collectors.toList());
    }

    public int highestFieldId() {
        return highestFieldId;
    }

    public List<String> partitionKeys() {
        return partitionKeys;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public List<String> trimmedPrimaryKeys() {
        // trimmed primary key 表示删除分区字段的 primary key
        if (primaryKeys.size() > 0) {
            List<String> adjusted =
                    primaryKeys.stream()
                            .filter(pk -> !partitionKeys.contains(pk))
                            .collect(Collectors.toList());

            // primary keys 和 partition keys 不能完全相同，否则每个分区只有一条数据
            Preconditions.checkState(
                    adjusted.size() > 0,
                    String.format(
                            "Primary key constraint %s should not be same with partition fields %s,"
                                    + " this will result in only one record in a partition",
                            primaryKeys, partitionKeys));

            return adjusted;
        }

        return primaryKeys;
    }

    public Map<String, String> options() {
        return options;
    }

    public List<String> bucketKeys() {
        // 1.先取 bucket-key
        // 2.回退为 primary key
        // 3.回退为所有字段

        List<String> bucketKeys = originalBucketKeys();
        if (bucketKeys.isEmpty()) {
            bucketKeys = trimmedPrimaryKeys();
        }
        if (bucketKeys.isEmpty()) {
            bucketKeys = fieldNames();
        }
        return bucketKeys;
    }

    public boolean crossPartitionUpdate() {
        if (primaryKeys.isEmpty() || partitionKeys.isEmpty()) {
            return false;
        }
        // primary key 不包含全部的 partition key，就可能发生跨分区更新
        // 也就是同个主键肯能会分布在不同的分区
        return !primaryKeys.containsAll(partitionKeys);
    }

    /** Original bucket keys, maybe empty. */
    private List<String> originalBucketKeys() {
        String key = options.get(BUCKET_KEY.key());
        if (StringUtils.isNullOrWhitespaceOnly(key)) { // 没有指定 bucket-key
            return Collections.emptyList();
        }
        List<String> bucketKeys = Arrays.asList(key.split(","));
        if (!containsAll(fieldNames(), bucketKeys)) { // bucket-key 必须是个表字段
            throw new RuntimeException(
                    String.format(
                            "Field names %s should contains all bucket keys %s.",
                            fieldNames(), bucketKeys));
        }

        // bucket-key 不能是 partition key 中的字段，这样有可能导致数据倾斜
        if (bucketKeys.stream().anyMatch(partitionKeys::contains)) {
            throw new RuntimeException(
                    String.format(
                            "Bucket keys %s should not in partition keys %s.",
                            bucketKeys, partitionKeys));
        }
        // 如果指定了 pk，bucket-key 则必须是主键的子集
        // 1. bucket-key 是 pk 的超集，同个主键数据可能会划分到不同 bucket
        // 2. pk 包含部分 bucket-key，主键相同的数据可能会划分到不同 bucket
        if (primaryKeys.size() > 0) {
            if (!containsAll(primaryKeys, bucketKeys)) {
                throw new RuntimeException(
                        String.format(
                                "Primary keys %s should contains all bucket keys %s.",
                                primaryKeys, bucketKeys));
            }
        }
        return bucketKeys;
    }

    private boolean containsAll(List<String> all, List<String> contains) {
        return new HashSet<>(all).containsAll(new HashSet<>(contains));
    }

    public @Nullable String comment() {
        return comment;
    }

    public long timeMillis() {
        return timeMillis;
    }

    public RowType logicalRowType() {
        return new RowType(fields);
    }

    public RowType logicalPartitionType() {
        return projectedLogicalRowType(partitionKeys);
    }

    public RowType logicalBucketKeyType() {
        return projectedLogicalRowType(bucketKeys());
    }

    public RowType logicalTrimmedPrimaryKeysType() {
        return projectedLogicalRowType(trimmedPrimaryKeys());
    }

    public RowType logicalPrimaryKeysType() {
        return projectedLogicalRowType(primaryKeys());
    }

    public List<DataField> primaryKeysFields() {
        return projectedDataFields(primaryKeys());
    }

    public List<DataField> trimmedPrimaryKeysFields() {
        // 获取 primary key DataField
        return projectedDataFields(trimmedPrimaryKeys());
    }

    public int[] projection(List<String> projectedFieldNames) {
        // project 字段名转换为下标
        List<String> fieldNames = fieldNames();
        return projectedFieldNames.stream().mapToInt(fieldNames::indexOf).toArray();
    }

    private List<DataField> projectedDataFields(List<String> projectedFieldNames) {
        // 投影 DataField
        List<String> fieldNames = fieldNames();
        return projectedFieldNames.stream()
                .map(k -> fields.get(fieldNames.indexOf(k)))
                .collect(Collectors.toList());
    }

    public RowType projectedLogicalRowType(List<String> projectedFieldNames) {
        // project DataField 组合的 rowType
        return new RowType(projectedDataFields(projectedFieldNames));
    }

    public TableSchema copy(Map<String, String> newOptions) {
        return new TableSchema(
                version,
                id,
                fields,
                highestFieldId,
                partitionKeys,
                primaryKeys,
                newOptions,
                comment,
                timeMillis);
    }

    public static TableSchema fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, TableSchema.class);
    }

    @Override
    public String toString() {
        return JsonSerdeUtil.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSchema tableSchema = (TableSchema) o;
        return version == tableSchema.version
                && Objects.equals(fields, tableSchema.fields)
                && Objects.equals(partitionKeys, tableSchema.partitionKeys)
                && Objects.equals(primaryKeys, tableSchema.primaryKeys)
                && Objects.equals(options, tableSchema.options)
                && Objects.equals(comment, tableSchema.comment)
                && timeMillis == tableSchema.timeMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                version, fields, partitionKeys, primaryKeys, options, comment, timeMillis);
    }

    public static List<DataField> newFields(RowType rowType) {
        // 获取 RowType 对应的 DataField
        return rowType.getFields();
    }
}
