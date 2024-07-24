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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 表的 schema.
 *
 * <p>Schema of a table.
 *
 * @since 0.4.0
 */
@Public
public class Schema {

    private final List<DataField> fields;

    private final List<String> partitionKeys;

    private final List<String> primaryKeys;

    private final Map<String, String> options;

    private final String comment; // 表的注释

    public Schema(
            List<DataField> fields,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            String comment) {
        this.options = new HashMap<>(options);
        this.partitionKeys = normalizePartitionKeys(partitionKeys);
        this.primaryKeys = normalizePrimaryKeys(primaryKeys);
        this.fields = normalizeFields(fields, this.primaryKeys, this.partitionKeys);

        this.comment = comment;
    }

    public RowType rowType() {
        return new RowType(false, fields);
    }

    public List<DataField> fields() {
        return fields;
    }

    public List<String> partitionKeys() {
        return partitionKeys;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public Map<String, String> options() {
        return options;
    }

    public String comment() {
        return comment;
    }

    private static List<DataField> normalizeFields(
            List<DataField> fields, List<String> primaryKeys, List<String> partitionKeys) {
        // 校验字段信息并给主键字段添加非空约束

        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());

        Set<String> duplicateColumns = duplicate(fieldNames); // 获取重复的字段名

        // 不能包含重复的字段
        Preconditions.checkState(
                duplicateColumns.isEmpty(),
                "Table column %s must not contain duplicate fields. Found: %s",
                fieldNames,
                duplicateColumns);

        Set<String> allFields = new HashSet<>(fieldNames);

        // 分区字段不能重复
        duplicateColumns = duplicate(partitionKeys);
        Preconditions.checkState(
                duplicateColumns.isEmpty(),
                "Partition key constraint %s must not contain duplicate columns. Found: %s",
                partitionKeys,
                duplicateColumns);

        // 分区字段必须在表字段中
        Preconditions.checkState(
                allFields.containsAll(partitionKeys),
                "Table column %s should include all partition fields %s",
                fieldNames,
                partitionKeys);

        if (primaryKeys.isEmpty()) { // primary key 为空，直接返回所有字段
            return fields;
        }
        duplicateColumns = duplicate(primaryKeys);

        // 主键字段不能重复
        Preconditions.checkState(
                duplicateColumns.isEmpty(),
                "Primary key constraint %s must not contain duplicate columns. Found: %s",
                primaryKeys,
                duplicateColumns);

        // 主键字段必须在表字段中
        Preconditions.checkState(
                allFields.containsAll(primaryKeys),
                "Table column %s should include all primary key constraint %s",
                fieldNames,
                primaryKeys);

        // primary key should not nullable
        Set<String> pkSet = new HashSet<>(primaryKeys);
        List<DataField> newFields = new ArrayList<>();
        for (DataField field : fields) {
            if (pkSet.contains(field.name()) && field.type().isNullable()) {
                // 给主键字段添加不能为约束
                newFields.add(
                        new DataField(
                                field.id(),
                                field.name(),
                                field.type().copy(false),
                                field.description()));
            } else {
                newFields.add(field);
            }
        }
        return newFields;
    }

    private List<String> normalizePrimaryKeys(List<String> primaryKeys) {
        // option 里也可以定义 primary key
        // 但是不能既使用 ddl 定义又使用 options 定义
        if (options.containsKey(CoreOptions.PRIMARY_KEY.key())) {
            if (!primaryKeys.isEmpty()) {
                throw new RuntimeException(
                        "Cannot define primary key on DDL and table options at the same time.");
            }
            String pk = options.get(CoreOptions.PRIMARY_KEY.key());
            primaryKeys = Arrays.asList(pk.split(","));
            options.remove(CoreOptions.PRIMARY_KEY.key());
        }
        return primaryKeys;
    }

    private List<String> normalizePartitionKeys(List<String> partitionKeys) {
        // 类似于 normalizePrimaryKeys 方法
        if (options.containsKey(CoreOptions.PARTITION.key())) {
            if (!partitionKeys.isEmpty()) {
                throw new RuntimeException(
                        "Cannot define partition on DDL and table options at the same time.");
            }
            String partitions = options.get(CoreOptions.PARTITION.key());
            partitionKeys = Arrays.asList(partitions.split(","));
            options.remove(CoreOptions.PARTITION.key());
        }
        return partitionKeys;
    }

    private static Set<String> duplicate(List<String> names) {
        // 过滤出重复字段
        return names.stream()
                .filter(name -> Collections.frequency(names, name) > 1)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schema that = (Schema) o;
        return Objects.equals(fields, that.fields)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(options, that.options)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, partitionKeys, primaryKeys, options, comment);
    }

    @Override
    public String toString() {
        return "UpdateSchema{"
                + "fields="
                + fields
                + ", partitionKeys="
                + partitionKeys
                + ", primaryKeys="
                + primaryKeys
                + ", options="
                + options
                + ", comment="
                + comment
                + '}';
    }

    /** Builder for configuring and creating instances of {@link Schema}. */
    public static Schema.Builder newBuilder() {
        return new Builder();
    }

    /** A builder for constructing an immutable but still unresolved {@link Schema}. */
    public static final class Builder {

        private final List<DataField> columns = new ArrayList<>();

        private List<String> partitionKeys = new ArrayList<>();

        private List<String> primaryKeys = new ArrayList<>();

        private final Map<String, String> options = new HashMap<>();

        @Nullable private String comment;

        private final AtomicInteger highestFieldId = new AtomicInteger(-1); // 字段最大 ID

        public int getHighestFieldId() {
            return highestFieldId.get();
        }

        /**
         * Declares a column that is appended to this schema.
         *
         * @param columnName column name
         * @param dataType data type of the column
         */
        public Builder column(String columnName, DataType dataType) {
            return column(columnName, dataType, null);
        }

        /**
         * Declares a column that is appended to this schema.
         *
         * @param columnName column name
         * @param dataType data type of the column
         * @param description description of the column
         */
        public Builder column(String columnName, DataType dataType, @Nullable String description) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");

            int id = highestFieldId.incrementAndGet();
            // 目的是给 RowType 中的 DataField 重新编号，可参考 ReassignFieldId 类的注释
            DataType reassignDataType = ReassignFieldId.reassign(dataType, highestFieldId);
            columns.add(new DataField(id, columnName, reassignDataType, description));
            return this;
        }

        /** Declares partition keys. */
        public Builder partitionKeys(String... columnNames) {
            return partitionKeys(Arrays.asList(columnNames));
        }

        /** Declares partition keys. */
        public Builder partitionKeys(List<String> columnNames) {
            this.partitionKeys = new ArrayList<>(columnNames);
            return this;
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(String... columnNames) {
            return primaryKey(Arrays.asList(columnNames));
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(List<String> columnNames) {
            this.primaryKeys = new ArrayList<>(columnNames);
            return this;
        }

        /** Declares options. */
        public Builder options(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }

        /** Declares an option. */
        public Builder option(String key, String value) {
            this.options.put(key, value);
            return this;
        }

        /** Declares table comment. */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /** Returns an instance of an unresolved {@link Schema}. */
        public Schema build() {
            return new Schema(columns, partitionKeys, primaryKeys, options, comment);
        }
    }
}
