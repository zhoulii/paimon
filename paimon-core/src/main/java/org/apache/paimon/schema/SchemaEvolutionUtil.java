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

import org.apache.paimon.KeyValue;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateReplaceVisitor;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Utils for schema evolution. */
public class SchemaEvolutionUtil {

    private static final int NULL_FIELD_INDEX = -1;

    /**
     * 根据 field id 将表的字段与数据字段建立映射关系，无法.
     *
     * <p>Create index mapping from table fields to underlying data fields. For example, the table
     * and data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c, 6->b, 3->a
     *   <li>data fields: 1->a, 3->c
     * </ul>
     *
     * <p>We can get the index mapping [0, -1, 1], in which 0 is the index of table field 1->c in
     * data fields, -1 is the index of 6->b in data fields and 1 is the index of 3->a in data
     * fields.
     *
     * <p>1->c 对应 1->a、6->b 没有对应的数据字段、3->a 对应 3->c.
     *
     * <p>/// TODO should support nest index mapping when nest schema evolution is supported.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @return the index mapping
     */
    @Nullable
    public static int[] createIndexMapping(
            List<DataField> tableFields, List<DataField> dataFields) {
        int[] indexMapping = new int[tableFields.size()];
        Map<Integer, Integer> fieldIdToIndex = new HashMap<>();
        for (int i = 0; i < dataFields.size(); i++) {
            fieldIdToIndex.put(dataFields.get(i).id(), i);
        }

        for (int i = 0; i < tableFields.size(); i++) {
            int fieldId = tableFields.get(i).id();
            Integer dataFieldIndex = fieldIdToIndex.get(fieldId);
            if (dataFieldIndex != null) {
                indexMapping[i] = dataFieldIndex;
            } else {
                indexMapping[i] = NULL_FIELD_INDEX;
            }
        }

        for (int i = 0; i < indexMapping.length; i++) {
            if (indexMapping[i] != i) {
                return indexMapping;
            }
        }
        return null;
    }

    /**
     * 先做投影，再做映射.
     *
     * <p>Create index mapping from table projection to underlying data projection. For example, the
     * table and data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c, 3->a, 4->e, 5->d, 6->b
     *   <li>data fields: 1->a, 2->b, 3->c, 4->d
     * </ul>
     *
     * <p>The table and data top projections are as follows
     *
     * <ul>
     *   <li>table projection: [0, 4, 1] 1->c, 6->b, 3->a
     *   <li>data projection: [0, 2] 1->a, 3->c
     * </ul>
     *
     * <p>We can first get fields list for table and data projections from their fields as follows
     *
     * <ul>
     *   <li>table projection field list: [1->c, 6->b, 3->a]
     *   <li>data projection field list: [1->a, 3->c]
     * </ul>
     *
     * <p>Then create index mapping based on the fields list and create cast mapping based on index
     * mapping.
     *
     * <p>/// TODO should support nest index mapping when nest schema evolution is supported.
     *
     * @param tableProjection the table projection
     * @param tableFields the fields in table
     * @param dataProjection the underlying data projection
     * @param dataFields the fields in underlying data
     * @return the index mapping
     */
    public static IndexCastMapping createIndexCastMapping(
            int[] tableProjection,
            List<DataField> tableFields,
            int[] dataProjection,
            List<DataField> dataFields) {
        return createIndexCastMapping(
                projectDataFields(tableProjection, tableFields),
                projectDataFields(dataProjection, dataFields));
    }

    /**
     * 建立 table field 与 data field 映射关系及转换方式.
     *
     * <p>Create index mapping from table fields to underlying data fields.
     */
    public static IndexCastMapping createIndexCastMapping(
            List<DataField> tableFields, List<DataField> dataFields) {
        int[] indexMapping = createIndexMapping(tableFields, dataFields);
        CastFieldGetter[] castMapping =
                createCastFieldGetterMapping(tableFields, dataFields, indexMapping);
        return new IndexCastMapping() {
            @Nullable
            @Override
            public int[] getIndexMapping() {
                return indexMapping;
            }

            @Nullable
            @Override
            public CastFieldGetter[] getCastMapping() {
                return castMapping;
            }
        };
    }

    private static List<DataField> projectDataFields(int[] projection, List<DataField> dataFields) {
        // 取投影字段
        List<DataField> projectFields = new ArrayList<>(projection.length);
        for (int index : projection) {
            projectFields.add(dataFields.get(index));
        }

        return projectFields;
    }

    /**
     * 保证 table seq/kind id 和 data seq/kind id 一致，给 value field 重新编号，再投影，然后建立映射转换关系.
     *
     * <p>Create index mapping from table projection to data with key and value fields. We should
     * first create table and data fields with their key/value fields, then create index mapping
     * with their projections and fields. For example, the table and data projections and fields are
     * as follows
     *
     * <ul>
     *   <li>Table key fields: 1->ka, 3->kb, 5->kc, 6->kd; value fields: 0->a, 2->d, 4->b;
     *       projection: [0, 2, 3, 4, 5, 7] where 0 is 1->ka, 2 is 5->kc, 3 is 6->kd, 4/5 are seq
     *       and kind, 7 is 2->d
     *   <li>Data key fields: 1->kb, 5->ka; value fields: 2->aa, 4->f; projection: [0, 1, 2, 3, 4]
     *       where 0 is 1->kb, 1 is 5->ka, 2/3 are seq and kind, 4 is 2->aa
     * </ul>
     *
     * <p>First we will get max key id from table and data fields which is 6, then create table and
     * data fields on it
     *
     * <ul>
     *   <li>Table fields: 1->ka, 3->kb, 5->kc, 6->kd, 7->seq, 8->kind, 9->a, 11->d, 13->b
     *   <li>Data fields: 1->kb, 5->ka, 7->seq, 8->kind, 11->aa, 13->f
     * </ul>
     *
     * <p>Finally we can create index mapping with table/data projections and fields, and create
     * cast mapping based on index mapping.
     *
     * <p>/// TODO should support nest index mapping when nest schema evolution is supported.
     *
     * @param tableProjection the table projection
     * @param tableKeyFields the table key fields
     * @param tableValueFields the table value fields
     * @param dataProjection the data projection
     * @param dataKeyFields the data key fields
     * @param dataValueFields the data value fields
     * @return the result index and cast mapping
     */
    public static IndexCastMapping createIndexCastMapping(
            int[] tableProjection,
            List<DataField> tableKeyFields,
            List<DataField> tableValueFields,
            int[] dataProjection,
            List<DataField> dataKeyFields,
            List<DataField> dataValueFields) {
        // 取 tableKeyFields 与 dataKeyFields 的最大 id，目的是为了使二者的 seq、kind 字段 id 一致
        // 目的是让 table field 与 data field 的 seq、kind 字段 id 能能根据 id 建立正确的映射关系
        // key id 没变，建立映射关系不会有问题
        // value id 加上一个同样的值，映射关系也不会有问题
        int maxKeyId =
                Math.max(
                        tableKeyFields.stream().mapToInt(DataField::id).max().orElse(0),
                        dataKeyFields.stream().mapToInt(DataField::id).max().orElse(0));
        List<DataField> tableFields = // 重编码 tableValueFields id
                KeyValue.createKeyValueFields(tableKeyFields, tableValueFields, maxKeyId);
        List<DataField> dataFields = // 重编码 dataValueFields id
                KeyValue.createKeyValueFields(dataKeyFields, dataValueFields, maxKeyId);
        // 建立映射及转换关系
        return createIndexCastMapping(tableProjection, tableFields, dataProjection, dataFields);
    }

    /**
     * 根据 table field 的 projection，创建 data field 的 projection.
     *
     * <p>Create data projection from table projection. For example, the table and data fields are
     * as follows
     *
     * <ul>
     *   <li>table fields: 1->c, 3->a, 4->e, 5->d, 6->b
     *   <li>data fields: 1->a, 2->b, 3->c, 4->d
     * </ul>
     *
     * <p>When we project 1->c, 6->b, 3->a from table fields, the table projection is [[0], [4],
     * [1]], in which 0 is the index of field 1->c, 4 is the index of field 6->b, 1 is the index of
     * field 3->a in table fields. We need to create data projection from [[0], [4], [1]] as
     * follows:
     *
     * <ul>
     *   <li>Get field id of each index in table projection from table fields
     *   <li>Get index of each field above from data fields
     * </ul>
     *
     * <p>The we can create table projection as follows: [[0], [-1], [2]], in which 0, -1 and 2 are
     * the index of fields [1->c, 6->b, 3->a] in data fields. When we project column from underlying
     * data, we need to specify the field index and name. It is difficult to assign a proper field
     * id and name for 6->b in data projection and add it to data fields, and we can't use 6->b
     * directly because the field index of b in underlying is 2. We can remove the -1 field index in
     * data projection, then the result data projection is: [[0], [2]].
     *
     * <p>We create {@link InternalRow} for 1->a, 3->c after projecting them from underlying data,
     * then create {@link ProjectedRow} with a index mapping and return null for 6->b in table
     * fields.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @param tableProjection the projection of table
     * @return the projection of data
     */
    public static int[][] createDataProjection(
            List<DataField> tableFields, List<DataField> dataFields, int[][] tableProjection) {
        List<Integer> dataFieldIdList =
                dataFields.stream().map(DataField::id).collect(Collectors.toList());
        return Arrays.stream(tableProjection)
                .map(p -> Arrays.copyOf(p, p.length))
                .peek(
                        p -> {
                            int fieldId = tableFields.get(p[0]).id();
                            p[0] = dataFieldIdList.indexOf(fieldId);
                        })
                .filter(p -> p[0] >= 0)
                .toArray(int[][]::new);
    }

    /**
     * Create predicate list from data fields. We will visit all predicate in filters, reset it's
     * field index, name and type, and ignore predicate if the field is not exist.
     *
     * @param tableFields the table fields
     * @param dataFields the underlying data fields
     * @param filters the filters
     * @return the data filters
     */
    @Nullable
    public static List<Predicate> createDataFilters(
            List<DataField> tableFields, List<DataField> dataFields, List<Predicate> filters) {
        if (filters == null) {
            return null;
        }

        Map<String, DataField> nameToTableFields =
                tableFields.stream().collect(Collectors.toMap(DataField::name, f -> f));
        LinkedHashMap<Integer, DataField> idToDataFields = new LinkedHashMap<>();
        dataFields.forEach(f -> idToDataFields.put(f.id(), f));
        List<Predicate> dataFilters = new ArrayList<>(filters.size());

        PredicateReplaceVisitor visitor =
                predicate -> {
                    DataField tableField =
                            checkNotNull(
                                    nameToTableFields.get(predicate.fieldName()), // 谓词对应的字段存在
                                    String.format("Find no field %s", predicate.fieldName()));
                    DataField dataField = idToDataFields.get(tableField.id());
                    if (dataField == null) { // 如果对应的数据字段不存在，这个谓词不需要
                        return Optional.empty();
                    }

                    DataType dataValueType = dataField.type().copy(true);
                    DataType predicateType = predicate.type().copy(true);
                    CastExecutor<Object, Object> castExecutor =
                            dataValueType.equals(predicateType) // 谓词校验类型和数据字段类型一致，不需要转换，否则进行类型转换
                                    ? null
                                    : (CastExecutor<Object, Object>)
                                            CastExecutors.resolve(
                                                    predicate.type(), dataField.type());
                    // Convert value from predicate type to underlying data type which may lose
                    // information, for example, convert double value to int. But it doesn't matter
                    // because it just for predicate push down and the data will be filtered
                    // correctly after reading.
                    // todo 还是可能发生数据丢失的，比如 谓词是 !=3.3，转换后为 !=3，真实数据是 1，2，3，正确结果应该是全过滤出来
                    // todo 而转换后的只会过滤出 1,2
                    List<Object> literals = // 转换谓词常量类型
                            predicate.literals().stream()
                                    .map(v -> castExecutor == null ? v : castExecutor.cast(v))
                                    .collect(Collectors.toList());
                    return Optional.of(
                            new LeafPredicate(
                                    predicate.function(),
                                    dataField.type(),
                                    indexOf(dataField, idToDataFields), // 对第几个 data field 进行校验
                                    dataField.name(),
                                    literals));
                };

        for (Predicate predicate : filters) {
            predicate.visit(visitor).ifPresent(dataFilters::add);
        }
        return dataFilters;
    }

    private static int indexOf(DataField dataField, LinkedHashMap<Integer, DataField> dataFields) {
        // 某个字段的列表索引号
        int index = 0;
        for (Map.Entry<Integer, DataField> entry : dataFields.entrySet()) {
            if (dataField.id() == entry.getKey()) {
                return index;
            }
            index++;
        }

        throw new IllegalArgumentException(
                String.format("Can't find data field %s", dataField.name()));
    }

    /**
     * 废弃代码.
     *
     * <p>Create converter mapping from table fields to underlying data fields. For example, the
     * table and data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c INT, 6->b STRING, 3->a BIGINT
     *   <li>data fields: 1->a BIGINT, 3->c DOUBLE
     * </ul>
     *
     * <p>We can get the column types (1->a BIGINT), (3->c DOUBLE) from data fields for (1->c INT)
     * and (3->a BIGINT) in table fields through index mapping [0, -1, 1], then compare the data
     * type and create converter mapping.
     *
     * <p>/// TODO should support nest index mapping when nest schema evolution is supported.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @param indexMapping the index mapping from table fields to data fields
     * @return the index mapping
     */
    @Nullable
    public static CastExecutor<?, ?>[] createConvertMapping(
            List<DataField> tableFields, List<DataField> dataFields, int[] indexMapping) {
        CastExecutor<?, ?>[] converterMapping = new CastExecutor<?, ?>[tableFields.size()];
        boolean castExist = false;
        for (int i = 0; i < tableFields.size(); i++) {
            int dataIndex = indexMapping == null ? i : indexMapping[i];
            if (dataIndex < 0) {
                converterMapping[i] = CastExecutors.identityCastExecutor();
            } else {
                DataField tableField = tableFields.get(i);
                DataField dataField = dataFields.get(dataIndex);
                if (dataField.type().equalsIgnoreNullable(tableField.type())) {
                    converterMapping[i] = CastExecutors.identityCastExecutor();
                } else {
                    // TODO support column type evolution in nested type
                    checkState(
                            !tableField.type().is(DataTypeFamily.CONSTRUCTED),
                            "Only support column type evolution in atomic data type.");
                    converterMapping[i] =
                            checkNotNull(
                                    CastExecutors.resolve(dataField.type(), tableField.type()));
                    castExist = true;
                }
            }
        }

        return castExist ? converterMapping : null;
    }

    /**
     * 创建 CastFieldGetter 用于将数据文件字段类型转换为 table schema field 对应类型.
     *
     * <p>Create getter and casting mapping from table fields to underlying data fields with given
     * index mapping. For example, the table and data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c INT, 6->b STRING, 3->a BIGINT
     *   <li>data fields: 1->a BIGINT, 3->c DOUBLE
     * </ul>
     *
     * <p>We can get the column types (1->a BIGINT), (3->c DOUBLE) from data fields for (1->c INT)
     * and (3->a BIGINT) in table fields through index mapping [0, -1, 1], then compare the data
     * type and create getter and casting mapping.
     *
     * <p>/// TODO should support nest index mapping when nest schema evolution is supported.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @param indexMapping the index mapping from table fields to data fields
     * @return the getter and casting mapping
     */
    private static CastFieldGetter[] createCastFieldGetterMapping(
            List<DataField> tableFields, List<DataField> dataFields, int[] indexMapping) {
        CastFieldGetter[] converterMapping = new CastFieldGetter[tableFields.size()];
        boolean castExist = false; // 标识是否存在类型转换
        for (int i = 0; i < tableFields.size(); i++) {
            int dataIndex = indexMapping == null ? i : indexMapping[i];
            if (dataIndex < 0) { // 不存在映射关系，返回的 CastFieldGetter 会获取到 null
                converterMapping[i] =
                        new CastFieldGetter(row -> null, CastExecutors.identityCastExecutor());
            } else {
                DataField tableField = tableFields.get(i);
                DataField dataField = dataFields.get(dataIndex);
                if (dataField.type().equalsIgnoreNullable(tableField.type())) { // 类型相同不需要类型转换
                    // Create getter with index i and projected row data will convert to underlying
                    // data
                    converterMapping[i] =
                            new CastFieldGetter(
                                    InternalRowUtils.createNullCheckingFieldGetter(
                                            dataField.type(), i),
                                    CastExecutors.identityCastExecutor());
                } else {
                    // TODO support column type evolution in nested type
                    // 只支持原子类型的类型转换
                    checkState(
                            !(tableField.type() instanceof MapType
                                    || dataField.type() instanceof ArrayType
                                    || dataField.type() instanceof MultisetType
                                    || dataField.type() instanceof RowType),
                            "Only support column type evolution in atomic data type.");
                    // Create getter with index i and projected row data will convert to underlying
                    // data
                    converterMapping[i] =
                            new CastFieldGetter(
                                    InternalRowUtils.createNullCheckingFieldGetter(
                                            dataField.type(), i),
                                    checkNotNull(
                                            // 将数据文件字段类型转换为 table schema field 对应类型
                                            CastExecutors.resolve(
                                                    dataField.type(), tableField.type())));
                    castExist = true;
                }
            }
        }

        return castExist ? converterMapping : null; // 不需要类型转换返回 null
    }
}
