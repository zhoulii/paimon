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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 合并 Schema 的工具类.
 *
 * <p>The util class for merging the schemas.
 */
public class SchemaMergingUtils {

    public static TableSchema mergeSchemas(
            TableSchema currentTableSchema, RowType dataFields, boolean allowExplicitCast) {
        if (currentTableSchema.logicalRowType().equals(dataFields)) {
            return currentTableSchema;
        }

        AtomicInteger highestFieldId = new AtomicInteger(currentTableSchema.highestFieldId());
        RowType newRowType =
                mergeSchemas(
                        currentTableSchema.logicalRowType(),
                        dataFields,
                        highestFieldId,
                        allowExplicitCast);

        // 只做字段合并，主键列、分区列、options、comment 都不会发生改变
        return new TableSchema(
                currentTableSchema.id() + 1,
                newRowType.getFields(),
                highestFieldId.get(),
                currentTableSchema.partitionKeys(),
                currentTableSchema.primaryKeys(),
                currentTableSchema.options(),
                currentTableSchema.comment());
    }

    public static RowType mergeSchemas(
            RowType tableSchema,
            RowType dataSchema,
            AtomicInteger highestFieldId,
            boolean allowExplicitCast) {
        // 合并两个 RowType
        return (RowType) merge(tableSchema, dataSchema, highestFieldId, allowExplicitCast);
    }

    /**
     * 两边都存在，则更新，只存在一边，则保留.
     *
     * <p>Merge the base data type and the update data type if possible.
     *
     * <p>For RowType, find the fields which exists in both the base schema and the update schema,
     * and try to merge them by calling the method iteratively; remain those fields that are only in
     * the base schema and append those fields that are only in the update schema.
     *
     * <p>For other complex type, try to merge the element types.
     *
     * <p>For primitive data type, we treat that's compatible if the original type can be safely
     * cast to the new type.
     */
    public static DataType merge(
            DataType base0,
            DataType update0,
            AtomicInteger highestFieldId,
            boolean allowExplicitCast) {
        // Here we try t0 merge the base0 and update0 without regard to the nullability,
        // and set the base0's nullability to the return's.
        // 对于 RowType，会更新所有字段的非空属性
        DataType base = base0.copy(true);
        DataType update = update0.copy(true);

        if (base.equals(update)) { // 忽略非空性，直接比较
            return base0;
        } else if (base instanceof RowType && update instanceof RowType) { // merge RowType 类型
            List<DataField> baseFields = ((RowType) base).getFields();
            List<DataField> updateFields = ((RowType) update).getFields();
            Map<String, DataField> updateFieldMap =
                    updateFields.stream()
                            .collect(Collectors.toMap(DataField::name, Function.identity()));
            List<DataField> updatedFields =
                    baseFields.stream()
                            .map(
                                    baseField -> {
                                        if (updateFieldMap.containsKey(baseField.name())) {
                                            DataField updateField =
                                                    updateFieldMap.get(baseField.name());
                                            DataType updatedDataType =
                                                    merge(
                                                            baseField.type(),
                                                            updateField.type(),
                                                            highestFieldId,
                                                            allowExplicitCast);
                                            return new DataField(
                                                    baseField.id(),
                                                    baseField.name(),
                                                    updatedDataType,
                                                    baseField.description());
                                        } else {
                                            return baseField;
                                        }
                                    })
                            .collect(Collectors.toList());

            Map<String, DataField> baseFieldMap =
                    baseFields.stream()
                            .collect(Collectors.toMap(DataField::name, Function.identity()));
            List<DataField> newFields =
                    updateFields.stream()
                            .filter(field -> !baseFieldMap.containsKey(field.name()))
                            .map(field -> assignIdForNewField(field, highestFieldId))
                            .collect(Collectors.toList());

            updatedFields.addAll(newFields);
            return new RowType(base.isNullable(), updatedFields);
        } else if (base instanceof MapType && update instanceof MapType) { // merge map 类型
            return new MapType(
                    base.isNullable(),
                    merge(
                            ((MapType) base).getKeyType(),
                            ((MapType) update).getKeyType(),
                            highestFieldId,
                            allowExplicitCast),
                    merge(
                            ((MapType) base).getValueType(),
                            ((MapType) update).getValueType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof ArrayType && update instanceof ArrayType) { // merge array 类型
            return new ArrayType(
                    base.isNullable(),
                    merge(
                            ((ArrayType) base).getElementType(),
                            ((ArrayType) update).getElementType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof MultisetType && update instanceof MultisetType) {
            return new MultisetType(
                    base.isNullable(),
                    merge(
                            ((MultisetType) base).getElementType(),
                            ((MultisetType) update).getElementType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof DecimalType && update instanceof DecimalType) {
            if (base.equals(update)) {
                return base0;
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Failed to merge decimal types with different precision or scale: %s and %s",
                                base, update));
            }
        } else if (supportsDataTypesCast(base, update, allowExplicitCast)) {
            if (DataTypes.getLength(base).isPresent() && DataTypes.getLength(update).isPresent()) {
                // 允许小的长度类型转换到大的长度类型
                // this will check and merge types which has a `length` attribute, like BinaryType,
                // CharType, VarBinaryType, VarCharType.
                if (allowExplicitCast
                        || DataTypes.getLength(base).getAsInt()
                                <= DataTypes.getLength(update).getAsInt()) {
                    return update.copy(base0.isNullable());
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Failed to merge the target type that has a smaller length: %s and %s",
                                    base, update));
                }
            } else if (DataTypes.getPrecision(base).isPresent()
                    && DataTypes.getPrecision(update).isPresent()) {
                // this will check and merge types which has a `precision` attribute, like
                // LocalZonedTimestampType, TimeType, TimestampType.
                // 允许小的精度类型转换到大的精度类型
                if (allowExplicitCast
                        || DataTypes.getPrecision(base).getAsInt()
                                <= DataTypes.getPrecision(update).getAsInt()) {
                    return update.copy(base0.isNullable());
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Failed to merge the target type that has a lower precision: %s and %s",
                                    base, update));
                }
            } else {
                return update.copy(base0.isNullable());
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Failed to merge data types %s and %s", base, update));
        }
    }

    private static boolean supportsDataTypesCast(
            DataType sourceType, DataType targetType, boolean allowExplicitCast) {
        // 是否支持转换到 targetType
        boolean canImplicitCast = DataTypeCasts.supportsImplicitCast(sourceType, targetType);
        boolean canExplicitCast =
                allowExplicitCast && DataTypeCasts.supportsExplicitCast(sourceType, targetType);
        return canImplicitCast || canExplicitCast;
    }

    private static DataField assignIdForNewField(DataField field, AtomicInteger highestFieldId) {
        // 给 row type 的 field 分配新的 id
        DataType dataType = ReassignFieldId.reassign(field.type(), highestFieldId);
        // 给新的字段分配 id
        return new DataField(
                highestFieldId.incrementAndGet(), field.name(), dataType, field.description());
    }
}
