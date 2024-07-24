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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.SchemaChange.AddColumn;
import org.apache.paimon.schema.SchemaChange.DropColumn;
import org.apache.paimon.schema.SchemaChange.RemoveOption;
import org.apache.paimon.schema.SchemaChange.RenameColumn;
import org.apache.paimon.schema.SchemaChange.SetOption;
import org.apache.paimon.schema.SchemaChange.UpdateColumnComment;
import org.apache.paimon.schema.SchemaChange.UpdateColumnNullability;
import org.apache.paimon.schema.SchemaChange.UpdateColumnPosition;
import org.apache.paimon.schema.SchemaChange.UpdateColumnType;
import org.apache.paimon.schema.SchemaChange.UpdateComment;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.AbstractCatalog.DB_SUFFIX;
import static org.apache.paimon.catalog.Identifier.UNKNOWN_DATABASE;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.getBranchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 管理多版本 schema.
 *
 * <p>Schema Manager to manage schema versions.
 */
@ThreadSafe
public class SchemaManager implements Serializable {

    private static final String SCHEMA_PREFIX = "schema-";

    private final FileIO fileIO;
    private final Path tableRoot;

    @Nullable private transient Lock lock;

    public SchemaManager(FileIO fileIO, Path tableRoot) {
        this.fileIO = fileIO;
        this.tableRoot = tableRoot;
    }

    public SchemaManager withLock(@Nullable Lock lock) {
        this.lock = lock;
        return this;
    }

    /** @return latest schema. */
    public Optional<TableSchema> latest() {
        return latest(DEFAULT_MAIN_BRANCH);
    }

    public Optional<TableSchema> latest(String branchName) {
        // 找到分支下最新 schema
        Path directoryPath =
                branchName.equals(DEFAULT_MAIN_BRANCH)
                        ? schemaDirectory()
                        : branchSchemaDirectory(branchName);
        try {
            return listVersionedFiles(fileIO, directoryPath, SCHEMA_PREFIX)
                    .reduce(Math::max)
                    .map(this::schema);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** List all schema. */
    public List<TableSchema> listAll() {
        // 获取主分支下所有 schema 对象
        return listAllIds().stream().map(this::schema).collect(Collectors.toList());
    }

    /** List all schema IDs. */
    public List<Long> listAllIds() {
        // 列出主分支下所有 schema id
        try {
            return listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Create a new schema from {@link Schema}. */
    public TableSchema createTable(Schema schema) throws Exception {
        // 创建 TableSchema 对象
        return createTable(schema, false);
    }

    public TableSchema createTable(Schema schema, boolean ignoreIfExistsSame) throws Exception {
        // 创建 TableSchema 对象
        while (true) { // 如果未发生异常会循环到创建成功
            Optional<TableSchema> latest = latest();
            if (latest.isPresent()) { // 检验是否存在相同 Schema
                TableSchema oldSchema = latest.get();
                boolean isSame =
                        Objects.equals(oldSchema.fields(), schema.fields())
                                && Objects.equals(oldSchema.partitionKeys(), schema.partitionKeys())
                                && Objects.equals(oldSchema.primaryKeys(), schema.primaryKeys())
                                && Objects.equals(oldSchema.options(), schema.options());
                if (ignoreIfExistsSame && isSame) {
                    return oldSchema;
                }

                throw new IllegalStateException(
                        "Schema in filesystem exists, please use updating,"
                                + " latest schema is: "
                                + oldSchema);
            }

            List<DataField> fields = schema.fields();
            List<String> partitionKeys = schema.partitionKeys();
            List<String> primaryKeys = schema.primaryKeys();
            Map<String, String> options = schema.options();
            int highestFieldId = RowType.currentHighestFieldId(fields);

            TableSchema newSchema =
                    new TableSchema(
                            0,
                            fields,
                            highestFieldId,
                            partitionKeys,
                            primaryKeys,
                            options,
                            schema.comment());

            boolean success = commit(newSchema);
            if (success) {
                return newSchema;
            }
        }
    }

    /** Update {@link SchemaChange}s. */
    public TableSchema commitChanges(SchemaChange... changes) throws Exception {
        // 提交 schema 修改.
        return commitChanges(Arrays.asList(changes));
    }

    /** Update {@link SchemaChange}s. */
    public TableSchema commitChanges(List<SchemaChange> changes)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
                    Catalog.ColumnNotExistException {
        while (true) { // 未发生异常则循环到提交成功
            // 获取最新 schema
            TableSchema schema =
                    latest().orElseThrow(
                                    () ->
                                            new Catalog.TableNotExistException(
                                                    fromPath(tableRoot.toString(), true)));
            // 获取 schema 信息
            Map<String, String> newOptions = new HashMap<>(schema.options());
            List<DataField> newFields = new ArrayList<>(schema.fields());
            AtomicInteger highestFieldId = new AtomicInteger(schema.highestFieldId());
            String newComment = schema.comment();

            for (SchemaChange change : changes) {
                if (change instanceof SetOption) {
                    // 检测 option 是否支持修改，支持则修改，不支持则抛出异常
                    SetOption setOption = (SetOption) change;
                    checkAlterTableOption(setOption.key());
                    newOptions.put(setOption.key(), setOption.value());
                } else if (change instanceof RemoveOption) {
                    // 检测 option 是否支持删除，支持则删除，不支持则抛出异常
                    RemoveOption removeOption = (RemoveOption) change;
                    checkAlterTableOption(removeOption.key());
                    newOptions.remove(removeOption.key());
                } else if (change instanceof UpdateComment) {
                    // 修改表的 comment
                    UpdateComment updateComment = (UpdateComment) change;
                    newComment = updateComment.comment();
                } else if (change instanceof AddColumn) {
                    AddColumn addColumn = (AddColumn) change;
                    SchemaChange.Move move = addColumn.move();

                    // 检查列是否存在，存在则抛出异常
                    if (newFields.stream().anyMatch(f -> f.name().equals(addColumn.fieldName()))) {
                        throw new Catalog.ColumnAlreadyExistException(
                                fromPath(tableRoot.toString(), true), addColumn.fieldName());
                    }

                    // 新增列必须是 nullable 的
                    Preconditions.checkArgument(
                            addColumn.dataType().isNullable(),
                            "Column %s cannot specify NOT NULL in the %s table.",
                            addColumn.fieldName(),
                            fromPath(tableRoot.toString(), true).getFullName());

                    // 获取新增列的 ID
                    int id = highestFieldId.incrementAndGet();

                    // 给嵌套类型的字段 ID 重新编号
                    DataType dataType =
                            ReassignFieldId.reassign(addColumn.dataType(), highestFieldId);

                    // 创建出新的 DataField
                    DataField dataField =
                            new DataField(
                                    id, addColumn.fieldName(), dataType, addColumn.description());

                    // key: name ; value : index
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < newFields.size(); i++) {
                        map.put(newFields.get(i).name(), i); // 获取到 name 和 index 的映射关系
                    }

                    if (null != move) {
                        if (move.type().equals(SchemaChange.Move.MoveType.FIRST)) {
                            // 指定为开始位置，放到第一个字段
                            newFields.add(0, dataField);
                        } else if (move.type().equals(SchemaChange.Move.MoveType.AFTER)) {
                            // 指定添加到某个字段之后
                            int fieldIndex = map.get(move.referenceFieldName());
                            newFields.add(fieldIndex + 1, dataField);
                        }
                    } else { // 如果没指定位置，直接放到最后
                        newFields.add(dataField);
                    }

                } else if (change instanceof RenameColumn) {
                    RenameColumn rename = (RenameColumn) change;
                    // 不支持修改主键列和分区列的名称
                    validateNotPrimaryAndPartitionKey(schema, rename.fieldName());
                    // 检查修改后的名称是否与原有字段重名
                    if (newFields.stream().anyMatch(f -> f.name().equals(rename.newName()))) {
                        throw new Catalog.ColumnAlreadyExistException(
                                fromPath(tableRoot.toString(), true), rename.fieldName());
                    }

                    updateNestedColumn(
                            newFields,
                            new String[] {rename.fieldName()},
                            0,
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            rename.newName(),
                                            field.type(),
                                            field.description()));
                } else if (change instanceof DropColumn) {
                    DropColumn drop = (DropColumn) change;
                    validateNotPrimaryAndPartitionKey(schema, drop.fieldName());
                    if (!newFields.removeIf(
                            f -> f.name().equals(((DropColumn) change).fieldName()))) {
                        throw new Catalog.ColumnNotExistException(
                                fromPath(tableRoot.toString(), true), drop.fieldName());
                    }
                    if (newFields.isEmpty()) {
                        throw new IllegalArgumentException("Cannot drop all fields in table");
                    }
                } else if (change instanceof UpdateColumnType) {
                    UpdateColumnType update = (UpdateColumnType) change;

                    // 不支持修改分区列类型
                    if (schema.partitionKeys().contains(update.fieldName())) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Cannot update partition column [%s] type in the table[%s].",
                                        update.fieldName(), tableRoot.getName()));
                    }
                    // 内部也是调用 updateNestedColumn 来实现
                    updateColumn(
                            newFields,
                            update.fieldName(),
                            (field) -> {
                                checkState(
                                        // 原始类型是否可显示转换为新的类型
                                        DataTypeCasts.supportsExplicitCast(
                                                        field.type(), update.newDataType())
                                                // 对应的类型转换器必须存在
                                                && CastExecutors.resolve(
                                                                field.type(), update.newDataType())
                                                        != null,
                                        String.format(
                                                "Column type %s[%s] cannot be converted to %s without loosing information.",
                                                field.name(), field.type(), update.newDataType()));
                                // todo 下面两行代码是多余的
                                AtomicInteger dummyId = new AtomicInteger(0);
                                if (dummyId.get() != 0) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Update column to nested row type '%s' is not supported.",
                                                    update.newDataType()));
                                }
                                return new DataField(
                                        field.id(),
                                        field.name(),
                                        update.newDataType(),
                                        field.description());
                            });
                } else if (change instanceof UpdateColumnNullability) {
                    UpdateColumnNullability update = (UpdateColumnNullability) change;
                    // 不支持将主键列转换为 nullable 类型
                    if (update.fieldNames().length == 1
                            && update.newNullability()
                            && schema.primaryKeys().contains(update.fieldNames()[0])) {
                        throw new UnsupportedOperationException(
                                "Cannot change nullability of primary key");
                    }
                    updateNestedColumn(
                            newFields,
                            update.fieldNames(),
                            0,
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            field.name(),
                                            field.type().copy(update.newNullability()),
                                            field.description()));
                } else if (change instanceof UpdateColumnComment) {
                    // 更改字段 comment
                    UpdateColumnComment update = (UpdateColumnComment) change;
                    updateNestedColumn(
                            newFields,
                            update.fieldNames(),
                            0,
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            field.name(),
                                            field.type(),
                                            update.newDescription()));
                } else if (change instanceof UpdateColumnPosition) {
                    // 更改字段位置
                    UpdateColumnPosition update = (UpdateColumnPosition) change;
                    SchemaChange.Move move = update.move();

                    // key: name ; value : index
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < newFields.size(); i++) {
                        map.put(newFields.get(i).name(), i);
                    }

                    int fieldIndex = map.get(move.fieldName());
                    int refIndex = 0;
                    if (move.type().equals(SchemaChange.Move.MoveType.FIRST)) {
                        checkMoveIndexEqual(move, fieldIndex, refIndex);
                        newFields.add(refIndex, newFields.remove(fieldIndex));
                    } else if (move.type().equals(SchemaChange.Move.MoveType.AFTER)) {
                        refIndex = map.get(move.referenceFieldName());
                        checkMoveIndexEqual(move, fieldIndex, refIndex);
                        if (fieldIndex > refIndex) {
                            newFields.add(refIndex + 1, newFields.remove(fieldIndex));
                        } else {
                            newFields.add(refIndex, newFields.remove(fieldIndex));
                        }
                    }

                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported change: " + change.getClass());
                }
            }

            TableSchema newSchema =
                    new TableSchema(
                            schema.id() + 1,
                            newFields,
                            highestFieldId.get(),
                            schema.partitionKeys(),
                            schema.primaryKeys(),
                            newOptions,
                            newComment);

            try {
                boolean success = commit(newSchema); // 写出新的 schema
                if (success) {
                    return newSchema;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean mergeSchema(RowType rowType, boolean allowExplicitCast) {
        // 合并 schema
        TableSchema current =
                latest().orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "It requires that the current schema to exist when calling 'mergeSchema'"));
        // 两边不存在的都留下，两边都存在的进行类型合并
        TableSchema update = SchemaMergingUtils.mergeSchemas(current, rowType, allowExplicitCast);
        if (current.equals(update)) {
            return false;
        } else {
            try {
                return commit(update);
            } catch (Exception e) {
                throw new RuntimeException("Failed to commit the schema.", e);
            }
        }
    }

    private static void checkMoveIndexEqual(SchemaChange.Move move, int fieldIndex, int refIndex) {
        // 位置没有发生移动则抛出异常
        if (refIndex == fieldIndex) {
            throw new UnsupportedOperationException(
                    String.format("Cannot move itself for column %s", move.fieldName()));
        }
    }

    private void validateNotPrimaryAndPartitionKey(TableSchema schema, String fieldName) {
        // 不支持修改主键列和分区列的名称
        /// TODO support partition and primary keys schema evolution
        if (schema.partitionKeys().contains(fieldName)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot drop/rename partition key[%s]", fieldName));
        }
        if (schema.primaryKeys().contains(fieldName)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot drop/rename primary key[%s]", fieldName));
        }
    }

    /** This method is hacky, newFields may be immutable. We should use {@link DataTypeVisitor}. */
    private void updateNestedColumn(
            List<DataField> newFields,
            String[] updateFieldNames, // updateFieldNames 长度为 1，表示修改最外层字段名，大于 1 表示修改内层字段
            int index,
            Function<DataField, DataField> updateFunc)
            throws Catalog.ColumnNotExistException {
        boolean found = false;
        for (int i = 0; i < newFields.size(); i++) {
            DataField field = newFields.get(i);
            if (field.name().equals(updateFieldNames[index])) { // 找到要修改的字段
                found = true;
                if (index == updateFieldNames.length - 1) {
                    newFields.set(i, updateFunc.apply(field)); // 替换 DataField
                    break;
                } else {
                    List<DataField> nestedFields = // 获取内层字段
                            new ArrayList<>(
                                    ((org.apache.paimon.types.RowType) field.type()).getFields());
                    // 递归嵌套修改
                    updateNestedColumn(nestedFields, updateFieldNames, index + 1, updateFunc);
                    // 替换最外层字段
                    newFields.set(
                            i,
                            new DataField(
                                    field.id(),
                                    field.name(),
                                    new org.apache.paimon.types.RowType(
                                            field.type().isNullable(), nestedFields),
                                    field.description()));
                }
            }
        }
        if (!found) {
            // 没找到修改列，抛出异常
            throw new Catalog.ColumnNotExistException(
                    fromPath(tableRoot.toString(), true), Arrays.toString(updateFieldNames));
        }
    }

    private void updateColumn(
            List<DataField> newFields,
            String updateFieldName,
            Function<DataField, DataField> updateFunc)
            throws Catalog.ColumnNotExistException {
        // 更新列
        updateNestedColumn(newFields, new String[] {updateFieldName}, 0, updateFunc);
    }

    @VisibleForTesting
    boolean commit(TableSchema newSchema) throws Exception {
        // 校验 schema
        SchemaValidation.validateTableSchema(newSchema);

        Path schemaPath = toSchemaPath(newSchema.id());
        // 写出 schema json
        Callable<Boolean> callable = () -> fileIO.writeFileUtf8(schemaPath, newSchema.toString());
        if (lock == null) { // 是否使用 lock 由 catalog 及 with 参数决定
            return callable.call();
        }
        return lock.runWithLock(callable);
    }

    /** Read schema for schema id. */
    public TableSchema schema(long id) {
        try {
            // schema json 字符串反序列化为 TableSchema 对象
            return JsonSerdeUtil.fromJson(fileIO.readFileUtf8(toSchemaPath(id)), TableSchema.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static TableSchema fromPath(FileIO fileIO, Path path) {
        try {
            return JsonSerdeUtil.fromJson(fileIO.readFileUtf8(path), TableSchema.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path schemaDirectory() {
        // schema 文件目录
        return new Path(tableRoot + "/schema");
    }

    @VisibleForTesting
    public Path toSchemaPath(long id) {
        // 指定 ID SCHEMA 文件路径
        return new Path(tableRoot + "/schema/" + SCHEMA_PREFIX + id);
    }

    public Path branchSchemaDirectory(String branchName) {
        // 某个 branch 的 schema 文件目录
        return new Path(getBranchPath(tableRoot, branchName) + "/schema");
    }

    public Path branchSchemaPath(String branchName, long schemaId) {
        // 某个 branch 的 schema 文件路径
        return new Path(
                getBranchPath(tableRoot, branchName) + "/schema/" + SCHEMA_PREFIX + schemaId);
    }

    /**
     * Delete schema with specific id.
     *
     * @param schemaId the schema id to delete.
     */
    public void deleteSchema(long schemaId) {
        // 删除 schema 文件
        fileIO.deleteQuietly(toSchemaPath(schemaId));
    }

    public static void checkAlterTableOption(String key) {
        // 检验 option 是否支持修改
        if (CoreOptions.getImmutableOptionKeys().contains(key)) {
            throw new UnsupportedOperationException(
                    String.format("Change '%s' is not supported yet.", key));
        }
    }

    public static void checkAlterTablePath(String key) {
        // 不支持修改表路径
        if (CoreOptions.PATH.key().equalsIgnoreCase(key)) {
            throw new UnsupportedOperationException("Change path is not supported yet.");
        }
    }

    public static Identifier fromPath(String tablePath, boolean ignoreIfUnknownDatabase) {
        // 解析 database 及 tableName
        String[] paths = tablePath.split("/");
        if (paths.length < 2) {
            if (!ignoreIfUnknownDatabase) {
                throw new IllegalArgumentException(
                        String.format(
                                "Path '%s' is not a legacy path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
                                tablePath));
            }
            return new Identifier(UNKNOWN_DATABASE, paths[0]);
        }

        String database = paths[paths.length - 2];
        int index = database.lastIndexOf(DB_SUFFIX);
        if (index == -1) {
            if (!ignoreIfUnknownDatabase) {
                throw new IllegalArgumentException(
                        String.format(
                                "Path '%s' is not a legacy path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
                                tablePath));
            }
            return new Identifier(UNKNOWN_DATABASE, paths[paths.length - 1]);
        }
        database = database.substring(0, index); // 删除 .db 后缀
        return new Identifier(database, paths[paths.length - 1]);
    }
}
