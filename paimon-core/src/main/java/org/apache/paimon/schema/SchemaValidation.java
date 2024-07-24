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
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.CHANGELOG_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.CHANGELOG_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_WATERMARK;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.STREAMING_READ_OVERWRITE;
import static org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.SEQUENCE_GROUP;
import static org.apache.paimon.schema.SystemColumns.KEY_FIELD_PREFIX;
import static org.apache.paimon.schema.SystemColumns.SYSTEM_FIELD_NAMES;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 校验 TableSchema 的工具类.
 *
 * <p>Validation utils for {@link TableSchema}.
 */
public class SchemaValidation {

    // 不支持作业 key 的字段类型
    public static final List<Class<? extends DataType>> PRIMARY_KEY_UNSUPPORTED_LOGICAL_TYPES =
            Arrays.asList(MapType.class, ArrayType.class, RowType.class, MultisetType.class);

    /**
     * Validate the {@link TableSchema} and {@link CoreOptions}.
     *
     * <p>TODO validate all items in schema and all keys in options.
     *
     * @param schema the schema to be validated
     */
    public static void validateTableSchema(TableSchema schema) {
        // primary key 和分区字段只支持基本类型
        validateOnlyContainPrimitiveType(schema.fields(), schema.primaryKeys(), "primary key");
        validateOnlyContainPrimitiveType(schema.fields(), schema.partitionKeys(), "partition");

        CoreOptions options = new CoreOptions(schema.options());

        validateBucket(schema, options);

        validateDefaultValues(schema);

        validateStartupMode(options);

        validateFieldsPrefix(schema, options);

        validateSequenceField(schema, options);

        validateSequenceGroup(schema, options);

        ChangelogProducer changelogProducer = options.changelogProducer();

        // changelog producer 只针对主键表
        if (schema.primaryKeys().isEmpty() && changelogProducer != ChangelogProducer.NONE) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Can not set %s on table without primary keys, please define primary keys.",
                            CHANGELOG_PRODUCER.key()));
        }

        // https://github.com/apache/paimon/pull/2180
        // 1.full_compaction 和 lookup changelog producer 在 compaction 生成 changelog
        // 2.overwrite snapshot 不生成 changelog，读取 change 的方式是 ScanMode.DELTA，也就是读取 snapshot 变化文件（可以从
        // org/apache/paimon/table/source/InnerStreamTableScanImpl.java:188 跟进去看）
        // 3.如果指定了 changelog producer，那么流读增量数据时会读取 changelog
        // 4.比如说，overwrite 时写入 3 条数据，删除原先所有数据，overwrite snapshot 是不会生成 changelog 的
        // 5.如果 full_compaction 和 lookup changelog producer 配置时，允许流读 overwrite
        // snapshot（ScanMode.DELTA 方式），那么新增的三条数据会读取到一次
        // 6.读接下来的 changelog 时，这三条数据又被读取一次，所以造成数据重复
        if (options.streamingReadOverwrite()
                && (changelogProducer == ChangelogProducer.FULL_COMPACTION
                        || changelogProducer == ChangelogProducer.LOOKUP)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Cannot set %s to true when changelog producer is %s or %s because it will read duplicated changes.",
                            STREAMING_READ_OVERWRITE.key(),
                            ChangelogProducer.FULL_COMPACTION,
                            ChangelogProducer.LOOKUP));
        }

        // 校验 snapshot 保存数量配置
        checkArgument(
                options.snapshotNumRetainMin() > 0,
                SNAPSHOT_NUM_RETAINED_MIN.key() + " should be at least 1");
        checkArgument(
                options.snapshotNumRetainMin() <= options.snapshotNumRetainMax(),
                SNAPSHOT_NUM_RETAINED_MIN.key()
                        + " should not be larger than "
                        + SNAPSHOT_NUM_RETAINED_MAX.key());

        // 校验 changelog 保存数量配置
        checkArgument(
                options.changelogNumRetainMin() > 0,
                CHANGELOG_NUM_RETAINED_MIN.key() + " should be at least 1");
        checkArgument(
                options.changelogNumRetainMin() <= options.changelogNumRetainMax(),
                CHANGELOG_NUM_RETAINED_MIN.key()
                        + " should not be larger than "
                        + CHANGELOG_NUM_RETAINED_MAX.key());

        // 校验 format 是否支持所有字段类型
        // Get the format type here which will try to convert string value to {@Code
        // FileFormatType}. If the string value is illegal, an exception will be thrown.
        CoreOptions.FileFormatType fileFormatType = options.formatType();
        FileFormat fileFormat =
                FileFormat.fromIdentifier(fileFormatType.name(), new Options(schema.options()));
        fileFormat.validateDataFields(new RowType(schema.fields()));

        // Check column names in schema
        schema.fieldNames()
                .forEach(
                        f -> {
                            // 字段中不能出现系统保留字段
                            checkState(
                                    !SYSTEM_FIELD_NAMES.contains(f),
                                    String.format(
                                            "Field name[%s] in schema cannot be exist in %s",
                                            f, SYSTEM_FIELD_NAMES));
                            // 不能以 _KEY_ 开头
                            checkState(
                                    !f.startsWith(KEY_FIELD_PREFIX),
                                    String.format(
                                            "Field name[%s] in schema cannot start with [%s]",
                                            f, KEY_FIELD_PREFIX));
                        });

        // streamingReadOverwrite 只支持主键表
        if (schema.primaryKeys().isEmpty() && options.streamingReadOverwrite()) {
            throw new RuntimeException(
                    "Doesn't support streaming read the changes from overwrite when the primary keys are not defined.");
        }

        // 分区表才能指定 PARTITION_EXPIRATION_TIME
        if (schema.options().containsKey(CoreOptions.PARTITION_EXPIRATION_TIME.key())) {
            if (schema.partitionKeys().isEmpty()) {
                throw new IllegalArgumentException(
                        "Can not set 'partition.expiration-time' for non-partitioned table.");
            }
        }

        if (options.mergeEngine() == MergeEngine.FIRST_ROW) {
            // 之前 FIRST_ROW 只有配合 lookup changelog-producer 才能使用
            // 但有时并不需要产生 changelog，所以 https://github.com/apache/paimon/pull/3452 支持在 none
            // changelog-producer 时也能使用 FIRST_ROW
            if (options.changelogProducer() != ChangelogProducer.LOOKUP
                    && options.changelogProducer() != ChangelogProducer.NONE) {
                throw new IllegalArgumentException(
                        "Only support 'none' and 'lookup' changelog-producer on FIRST_MERGE merge engine");
            }
        }

        options.rowkindField()
                .ifPresent(
                        field ->
                                checkArgument(
                                        schema.fieldNames().contains(field), // 字段必须存在
                                        "Rowkind field: '%s' can not be found in table schema.",
                                        field));

        if (options.deletionVectorsEnabled()) { // 校验 deletion vector 配置
            validateForDeletionVectors(schema, options);
        }
    }

    private static void validateOnlyContainPrimitiveType(
            List<DataField> fields, List<String> fieldNames, String errorMessageIntro) {
        // fieldNames 必须是 PrimitiveType
        if (!fieldNames.isEmpty()) {
            Map<String, DataField> rowFields = new HashMap<>();
            for (DataField rowField : fields) {
                rowFields.put(rowField.name(), rowField);
            }
            for (String fieldName : fieldNames) {
                DataField rowField = rowFields.get(fieldName);
                DataType dataType = rowField.type();
                if (PRIMARY_KEY_UNSUPPORTED_LOGICAL_TYPES.stream()
                        .anyMatch(c -> c.isInstance(dataType))) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The type %s in %s field %s is unsupported",
                                    dataType.getClass().getSimpleName(),
                                    errorMessageIntro,
                                    fieldName));
                }
            }
        }
    }

    private static void validateStartupMode(CoreOptions options) {
        if (options.startupMode() == CoreOptions.StartupMode.FROM_TIMESTAMP) {
            // FROM_TIMESTAMP mode 必须设置 SCAN_TIMESTAMP_MILLIS
            checkOptionExistInMode(
                    options, SCAN_TIMESTAMP_MILLIS, CoreOptions.StartupMode.FROM_TIMESTAMP);
            checkOptionsConflict(
                    options,
                    // 不能设置的参数
                    Arrays.asList(
                            SCAN_SNAPSHOT_ID,
                            SCAN_FILE_CREATION_TIME_MILLIS,
                            SCAN_TAG_NAME,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_BETWEEN),
                    Collections.singletonList(SCAN_TIMESTAMP_MILLIS));
        } else if (options.startupMode() == CoreOptions.StartupMode.FROM_SNAPSHOT) {
            // 对于 FROM_SNAPSHOT 模式，SCAN_SNAPSHOT_ID、SCAN_WATERMARK、SCAN_TAG_NAME 只能设置一个
            checkExactOneOptionExistInMode(
                    options,
                    options.startupMode(),
                    SCAN_SNAPSHOT_ID,
                    SCAN_TAG_NAME,
                    SCAN_WATERMARK);
            checkOptionsConflict(
                    options,
                    // 不能设置的参数
                    Arrays.asList(
                            SCAN_TIMESTAMP_MILLIS,
                            SCAN_FILE_CREATION_TIME_MILLIS,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_BETWEEN),
                    Arrays.asList(SCAN_SNAPSHOT_ID, SCAN_TAG_NAME));
        } else if (options.startupMode() == CoreOptions.StartupMode.INCREMENTAL) {
            // INCREMENTAL_BETWEEN 与 INCREMENTAL_BETWEEN_TIMESTAMP 只能设置一个
            checkExactOneOptionExistInMode(
                    options,
                    options.startupMode(),
                    INCREMENTAL_BETWEEN,
                    INCREMENTAL_BETWEEN_TIMESTAMP);
            // 不能设置的参数
            checkOptionsConflict(
                    options,
                    Arrays.asList(
                            SCAN_SNAPSHOT_ID,
                            SCAN_TIMESTAMP_MILLIS,
                            SCAN_FILE_CREATION_TIME_MILLIS,
                            SCAN_TAG_NAME),
                    Arrays.asList(INCREMENTAL_BETWEEN, INCREMENTAL_BETWEEN_TIMESTAMP));
        } else if (options.startupMode() == CoreOptions.StartupMode.FROM_SNAPSHOT_FULL) {
            // 必须设置 SCAN_SNAPSHOT_ID
            checkOptionExistInMode(options, SCAN_SNAPSHOT_ID, options.startupMode());
            checkOptionsConflict(
                    options,
                    // 不能设置的参数
                    Arrays.asList(
                            SCAN_TIMESTAMP_MILLIS,
                            SCAN_FILE_CREATION_TIME_MILLIS,
                            SCAN_TAG_NAME,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_BETWEEN),
                    Collections.singletonList(SCAN_SNAPSHOT_ID));
        } else if (options.startupMode() == CoreOptions.StartupMode.FROM_FILE_CREATION_TIME) {
            // 必须设置的参数
            checkOptionExistInMode(
                    options,
                    SCAN_FILE_CREATION_TIME_MILLIS,
                    CoreOptions.StartupMode.FROM_FILE_CREATION_TIME);
            // 不能设置的参数
            checkOptionsConflict(
                    options,
                    Arrays.asList(
                            SCAN_SNAPSHOT_ID,
                            SCAN_TIMESTAMP_MILLIS,
                            SCAN_TAG_NAME,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_BETWEEN),
                    Collections.singletonList(SCAN_FILE_CREATION_TIME_MILLIS));
        } else {
            // 其他模式不能设置的参数
            checkOptionNotExistInMode(options, SCAN_TIMESTAMP_MILLIS, options.startupMode());
            checkOptionNotExistInMode(
                    options, SCAN_FILE_CREATION_TIME_MILLIS, options.startupMode());
            checkOptionNotExistInMode(options, SCAN_SNAPSHOT_ID, options.startupMode());
            checkOptionNotExistInMode(options, SCAN_TAG_NAME, options.startupMode());
            checkOptionNotExistInMode(
                    options, INCREMENTAL_BETWEEN_TIMESTAMP, options.startupMode());
            checkOptionNotExistInMode(options, INCREMENTAL_BETWEEN, options.startupMode());
        }
    }

    private static void checkOptionExistInMode(
            CoreOptions options, ConfigOption<?> option, CoreOptions.StartupMode startupMode) {
        // 检验某个 startupMode 必须包含某个配置
        checkArgument(
                options.toConfiguration().contains(option),
                String.format(
                        "%s can not be null when you use %s for %s",
                        option.key(), startupMode, SCAN_MODE.key()));
    }

    private static void checkOptionNotExistInMode(
            CoreOptions options, ConfigOption<?> option, CoreOptions.StartupMode startupMode) {
        // 检验某个 startupMode 不能包含某个配置
        checkArgument(
                !options.toConfiguration().contains(option),
                String.format(
                        "%s must be null when you use %s for %s",
                        option.key(), startupMode, SCAN_MODE.key()));
    }

    private static void checkExactOneOptionExistInMode(
            CoreOptions options,
            CoreOptions.StartupMode startupMode,
            ConfigOption<?>... configOptions) { // 检验某个 startupMode 的配置是否合法
        checkArgument(
                Arrays.stream(configOptions)
                                .filter(op -> options.toConfiguration().contains(op))
                                .count()
                        == 1,
                String.format(
                        "must set only one key in [%s] when you use %s for %s",
                        concatConfigKeys(Arrays.asList(configOptions)),
                        startupMode,
                        SCAN_MODE.key()));
    }

    private static void checkOptionsConflict(
            CoreOptions options,
            List<ConfigOption<?>> illegalOptions,
            List<ConfigOption<?>> legalOptions) {
        // 当设置了 legalOptions 就不能设置 illegalOptions
        for (ConfigOption<?> illegalOption : illegalOptions) { // 是否存在冲突配置
            checkArgument(
                    !options.toConfiguration().contains(illegalOption),
                    "[%s] must be null when you set [%s]",
                    illegalOption.key(),
                    concatConfigKeys(legalOptions));
        }
    }

    private static String concatConfigKeys(List<ConfigOption<?>> configOptions) {
        // 将配置参数的 key 使用逗号拼接
        return configOptions.stream().map(ConfigOption::key).collect(Collectors.joining(","));
    }

    private static void validateFieldsPrefix(TableSchema schema, CoreOptions options) {
        List<String> fieldNames = schema.fieldNames();
        options.toMap()
                .keySet()
                .forEach(
                        k -> {
                            if (k.startsWith(FIELDS_PREFIX)) {
                                String fieldName = k.split("\\.")[1];
                                checkArgument(
                                        fieldNames.contains(fieldName), // 字段必须在表字段中
                                        String.format(
                                                "Field %s can not be found in table schema.",
                                                fieldName));
                            }
                        });
    }

    private static void validateSequenceGroup(TableSchema schema, CoreOptions options) {
        Map<String, Set<String>> fields2Group = new HashMap<>();
        for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            List<String> fieldNames = schema.fieldNames();
            if (k.startsWith(FIELDS_PREFIX)
                    && k.endsWith(SEQUENCE_GROUP)) { // 找到 sequence group option
                String sequenceFieldName =
                        k.substring(
                                FIELDS_PREFIX.length() + 1,
                                k.length() - SEQUENCE_GROUP.length() - 1); // 获取 sequence field name
                if (!fieldNames.contains(sequenceFieldName)) { // sequenceFieldName 必须是表的字段
                    throw new IllegalArgumentException(
                            String.format(
                                    "The sequence field group: %s can not be found in table schema.",
                                    sequenceFieldName));
                }

                for (String field : v.split(",")) {
                    if (!fieldNames.contains(field)) { // sequence group 更新的字段必须在表的字段中
                        throw new IllegalArgumentException(
                                String.format("Field %s can not be found in table schema.", field));
                    }
                    Set<String> group = fields2Group.computeIfAbsent(field, p -> new HashSet<>());
                    if (group.add(sequenceFieldName)
                            && group.size() > 1) { // 每一个要个更新的字段只能对应一个 sequence group
                        throw new IllegalArgumentException(
                                String.format(
                                        "Field %s is defined repeatedly by multiple groups: %s.",
                                        field, group));
                    }
                }
            }
        }
        Set<String> illegalGroup =
                fields2Group.values().stream()
                        .flatMap(Collection::stream)
                        .filter(g -> options.fieldAggFunc(g) != null)
                        .collect(Collectors.toSet());
        if (!illegalGroup.isEmpty()) { // sequence group 本身不支持设置 aggregation function.
            throw new IllegalArgumentException(
                    "Should not defined aggregation function on sequence group: " + illegalGroup);
        }
    }

    private static void validateDefaultValues(TableSchema schema) {
        CoreOptions coreOptions = new CoreOptions(schema.options());
        Map<String, String> defaultValues = coreOptions.getFieldDefaultValues(); // 字段的默认值

        if (!defaultValues.isEmpty()) {

            List<String> partitionKeys = schema.partitionKeys();
            for (String partitionKey : partitionKeys) { // 分区字段不能指定默认值
                if (defaultValues.containsKey(partitionKey)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Partition key %s should not be assign default column.",
                                    partitionKey));
                }
            }

            List<String> primaryKeys = schema.primaryKeys();
            for (String primaryKey : primaryKeys) { // 主键字段不能指定默认值
                if (defaultValues.containsKey(primaryKey)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Primary key %s should not be assign default column.",
                                    primaryKey));
                }
            }

            List<DataField> fields = schema.fields();

            for (DataField field : fields) {
                String defaultValueStr = defaultValues.get(field.name());
                if (defaultValueStr == null) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                CastExecutor<Object, Object> resolve =
                        (CastExecutor<Object, Object>)
                                CastExecutors.resolve(VarCharType.STRING_TYPE, field.type());
                if (resolve == null) { // 字段类型不支持由字符串转换而来，也就是说这个字段类型不支持默认值
                    throw new IllegalArgumentException(
                            String.format(
                                    "The column %s with datatype %s is currently not supported for default value.",
                                    field.name(), field.type().asSQLString()));
                }

                try {
                    resolve.cast(BinaryString.fromString(defaultValueStr)); // 尝试将字符串转换成字段类型数据
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "The default value %s of the column %s can not be cast to datatype: %s",
                                    defaultValueStr, field.name(), field.type()),
                            e);
                }
            }
        }
    }

    private static void validateForDeletionVectors(TableSchema schema, CoreOptions options) {
        // 只支持主键表
        checkArgument(
                !schema.primaryKeys().isEmpty(),
                "Deletion vectors mode is only supported for tables with primary keys.");
        // 只支持 NONE 或 LOOKUP changelog producer.
        checkArgument(
                options.changelogProducer() == ChangelogProducer.NONE
                        || options.changelogProducer() == ChangelogProducer.LOOKUP,
                "Deletion vectors mode is only supported for none or lookup changelog producer now.");
        // FIRST_ROW 不会产生 DELETE 消息.
        checkArgument(
                !options.mergeEngine().equals(MergeEngine.FIRST_ROW),
                "First row merge engine does not need deletion vectors because there is no deletion of old data in this merge engine.");
    }

    private static void validateSequenceField(TableSchema schema, CoreOptions options) {
        List<String> sequenceField = options.sequenceField();
        if (sequenceField.size() > 0) {
            Map<String, Integer> fieldCount =
                    sequenceField.stream()
                            .collect(Collectors.toMap(field -> field, field -> 1, Integer::sum));

            sequenceField.forEach(
                    field -> {
                        // seq 字段必须在 schema 中
                        checkArgument(
                                schema.fieldNames().contains(field),
                                "Sequence field: '%s' can not be found in table schema.",
                                field);
                        // seq 字段不能定义聚合函数
                        checkArgument(
                                options.fieldAggFunc(field) == null,
                                "Should not define aggregation on sequence field: '%s'.",
                                field);
                        // seq 字段不能重复
                        checkArgument(
                                fieldCount.get(field) == 1,
                                "Sequence field '%s' is defined repeatedly.",
                                field);
                    });

            if (options.mergeEngine() == MergeEngine.FIRST_ROW) {
                // first_row merge_engine 不支持使用 seq 字段
                throw new IllegalArgumentException(
                        "Do not support use sequence field on FIRST_MERGE merge engine.");
            }

            if (schema.crossPartitionUpdate()) {
                // 跨分区更新模式下不支持使用 seq 字段（为什么？）
                throw new IllegalArgumentException(
                        String.format(
                                "You can not use sequence.field in cross partition update case "
                                        + "(Primary key constraint '%s' not include all partition fields '%s').",
                                schema.primaryKeys(), schema.partitionKeys()));
            }
        }
    }

    private static void validateBucket(TableSchema schema, CoreOptions options) {
        int bucket = options.bucket();
        if (bucket == -1) {
            // unware 或者 dynamic bucket 模式下不支持指定 bucket-key
            if (options.toMap().get(BUCKET_KEY.key()) != null) {
                throw new RuntimeException(
                        "Cannot define 'bucket-key' in unaware or dynamic bucket mode.");
            }

            // unware 或者 dynamic bucket 模式下不支持指定 full-compaction.delta-commits
            if (schema.primaryKeys().isEmpty()
                    && options.toMap().get(FULL_COMPACTION_DELTA_COMMITS.key()) != null) {
                throw new RuntimeException(
                        "AppendOnlyTable of unware or dynamic bucket does not support 'full-compaction.delta-commits'");
            }
        } else if (bucket < 1) {
            throw new RuntimeException("The number of buckets needs to be greater than 0.");
        } else {
            // 跨 partition 更新必须使用 dynamic bucket 模式（是为了让数据分布更均匀吗？）
            if (schema.crossPartitionUpdate()) {
                throw new IllegalArgumentException(
                        String.format(
                                "You should use dynamic bucket (bucket = -1) mode in cross partition update case "
                                        + "(Primary key constraint %s not include all partition fields %s).",
                                schema.primaryKeys(), schema.partitionKeys()));
            }
        }
    }
}
