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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.types.DataTypeRoot.ROW;

/**
 * Projection 的抽象表示，有三个实现类. - EmptyProjection：不做任何投影操作 - TopLevelProjection：投影字段都是顶层字段下标表示 -
 * NestedProjection：投影字段都是嵌套字段下标表示
 *
 * <p>{@link Projection} represents a list of (possibly nested) indexes that can be used to project
 * data types. A row projection includes both reducing the accessible fields and reordering them.
 */
public abstract class Projection {

    // sealed class
    private Projection() {}

    public abstract RowType project(RowType rowType);

    /**
     * project 数组中的部分元素.
     *
     * <p>Project array.
     */
    public <T> T[] project(T[] array) {
        int[] project = toTopLevelIndexes();
        @SuppressWarnings("unchecked")
        T[] ret = (T[]) Array.newInstance(array.getClass().getComponentType(), project.length);
        for (int i = 0; i < project.length; i++) {
            ret[i] = array[project[i]];
        }
        return ret;
    }

    /** project 列表中的部分元素. Project list. */
    public <T> List<T> project(List<T> list) {
        int[] project = toTopLevelIndexes();
        List<T> ret = new ArrayList<>();
        for (int i : project) {
            ret.add(list.get(i));
        }
        return ret;
    }

    /**
     * 是否是嵌套式 project.
     *
     * @return {@code true} whether this projection is nested or not.
     */
    public abstract boolean isNested();

    /**
     * 计算差集，从注释和代码实现上来看这个方法实际上可能要涉及到实体 row 的字段删除，所以才需要调整投影编号.
     *
     * <p>关于例子的具体解释可查看下面方法的注释：
     * org.apache.paimon.utils.Projection.TopLevelProjection#difference(org.apache.paimon.utils.Projection)
     *
     * <p>Perform a difference of this {@link Projection} with another {@link Projection}. The
     * result of this operation is a new {@link Projection} retaining the same ordering of this
     * instance but with the indexes from {@code other} removed. For example:
     *
     * <pre>
     * <code>
     * [4, 1, 0, 3, 2] - [4, 2] = [1, 0, 2]
     * </code>
     * </pre>
     *
     * <p>Note how the index {@code 3} in the minuend becomes {@code 2} because it's rescaled to
     * project correctly a {@link InternalRow} or arity 3.
     *
     * @param other the subtrahend
     * @throws IllegalArgumentException when {@code other} is nested.
     */
    public abstract Projection difference(Projection other);

    /**
     * 按序返回当前 Projection 缺少哪些字段.
     *
     * <p>Complement this projection. The returned projection is an ordered projection of fields
     * from 0 to {@code fieldsNumber} except the indexes in this {@link Projection}. For example:
     *
     * <pre>
     * <code>
     * [4, 2].complement(5) = [0, 1, 3]
     * </code>
     * </pre>
     *
     * @param fieldsNumber the size of the universe
     * @throws IllegalStateException if this projection is nested.
     */
    public abstract Projection complement(int fieldsNumber);

    /**
     * 将 Projection 转换为顶层字段索引.
     *
     * <p>Convert this instance to a projection of top level indexes. The array represents the
     * mapping of the fields of the original {@link DataType}. For example, {@code [0, 2, 1]}
     * specifies to include in the following order the 1st field, the 3rd field and the 2nd field of
     * the row.
     *
     * @throws IllegalStateException if this projection is nested.
     */
    public abstract int[] toTopLevelIndexes();

    /**
     * 嵌套式 projection 表示，比如 [[0,2, 1], ...]. - 0：顶层的第一个字段 f0 - 2：f0 的第三个字段 f0-2 - 1：f0-2 的第二个字段
     * f0-2-1
     *
     * <p>Convert this instance to a nested projection index paths. The array represents the mapping
     * of the fields of the original {@link DataType}, including nested rows. For example, {@code
     * [[0, 2, 1], ...]} specifies to include the 2nd field of the 3rd field of the 1st field in the
     * top-level row.
     */
    public abstract int[][] toNestedIndexes();

    /**
     * 不做任何 project，返回一个 0 字段的 RowType
     *
     * <p>Create an empty {@link Projection}, that is a projection that projects no fields,
     * returning an empty {@link DataType}.
     */
    public static Projection empty() {
        return EmptyProjection.INSTANCE;
    }

    /**
     * 创建一个 Projection.
     *
     * <p>Create a {@link Projection} of the provided {@code indexes}.
     *
     * @see #toTopLevelIndexes()
     */
    public static Projection of(int[] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new TopLevelProjection(indexes);
    }

    /**
     * 创建一个嵌套式 Projection.
     *
     * <p>Create a {@link Projection} of the provided {@code indexes}.
     *
     * @see #toNestedIndexes()
     */
    public static Projection of(int[][] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new NestedProjection(indexes);
    }

    /**
     * 范围 projection，包含开始，不包含结束.
     *
     * <p>Create a {@link Projection} of a field range.
     */
    public static Projection range(int startInclusive, int endExclusive) {
        return new TopLevelProjection(IntStream.range(startInclusive, endExclusive).toArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Projection)) {
            return false;
        }
        Projection other = (Projection) o;
        if (!this.isNested() && !other.isNested()) {
            return Arrays.equals(this.toTopLevelIndexes(), other.toTopLevelIndexes());
        }
        return Arrays.deepEquals(this.toNestedIndexes(), other.toNestedIndexes());
    }

    @Override
    public int hashCode() {
        if (isNested()) {
            return Arrays.deepHashCode(toNestedIndexes());
        }
        return Arrays.hashCode(toTopLevelIndexes());
    }

    @Override
    public String toString() {
        if (isNested()) {
            return "Nested projection = " + Arrays.deepToString(toNestedIndexes());
        }
        return "Top level projection = " + Arrays.toString(toTopLevelIndexes());
    }

    private static class EmptyProjection extends Projection {

        static final EmptyProjection INSTANCE = new EmptyProjection();

        private EmptyProjection() {}

        @Override
        public RowType project(RowType rowType) {
            // 返回一个空的 RowType，不包含字段
            return new NestedProjection(toNestedIndexes()).project(rowType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Projection difference(Projection projection) {
            return this;
        }

        @Override
        public Projection complement(int fieldsNumber) {
            // 补集就是所有字段
            return new TopLevelProjection(IntStream.range(0, fieldsNumber).toArray());
        }

        @Override
        public int[] toTopLevelIndexes() {
            // 返回一个空数组，表示不做投影
            return new int[0];
        }

        @Override
        public int[][] toNestedIndexes() {
            // 返回一个空数组，表示不做投影
            return new int[0][];
        }
    }

    private static class NestedProjection extends Projection {

        final int[][] projection;
        final boolean nested;

        NestedProjection(int[][] projection) {
            this.projection = projection;
            this.nested = Arrays.stream(projection).anyMatch(arr -> arr.length > 1);
        }

        @Override
        public RowType project(RowType rowType) {
            final List<DataField> updatedFields = new ArrayList<>();
            Set<String> nameDomain = new HashSet<>();
            int duplicateCount = 0;
            for (int[] indexPath : this.projection) {
                DataField field = rowType.getFields().get(indexPath[0]); // 顶层字段编号
                StringBuilder builder =
                        new StringBuilder(rowType.getFieldNames().get(indexPath[0])); // 顶层字段名称
                for (int index = 1; index < indexPath.length; index++) {
                    // 嵌套式字段本身必须是 ROW 类型
                    Preconditions.checkArgument(
                            field.type().getTypeRoot() == ROW, "Row data type expected.");
                    RowType rowtype = ((RowType) field.type());
                    // 获取子层对应的字段名称
                    builder.append("_").append(rowtype.getFieldNames().get(indexPath[index]));
                    // 获取最终的字段
                    field = rowtype.getFields().get(indexPath[index]);
                }
                String path = builder.toString();
                while (nameDomain.contains(path)) {
                    // 给取同个子字段的投影取个不重复名称
                    path = builder.append("_$").append(duplicateCount++).toString();
                }
                updatedFields.add(field.newName(path));
                nameDomain.add(path);
            }
            return new RowType(rowType.isNullable(), updatedFields); // 创建一个新的 RowType
        }

        @Override
        public boolean isNested() {
            return nested;
        }

        @Override
        public Projection difference(Projection other) {
            // 不支持计算两个嵌套 Projection 的差集.
            if (other.isNested()) {
                throw new IllegalArgumentException(
                        "Cannot perform difference between nested projection and nested projection");
            }
            // other 为空，直接返回自身
            if (other instanceof EmptyProjection) {
                return this;
            }

            // 非 nested 投影，直接转换为顶层投影求差集
            if (!this.isNested()) {
                return new TopLevelProjection(toTopLevelIndexes()).difference(other);
            }

            // Extract the indexes to exclude and sort them
            int[] indexesToExclude = other.toTopLevelIndexes();
            indexesToExclude = Arrays.copyOf(indexesToExclude, indexesToExclude.length);
            Arrays.sort(indexesToExclude);

            List<int[]> resultProjection =
                    Arrays.stream(projection).collect(Collectors.toCollection(ArrayList::new));

            // 下面代码参考 org.apache.paimon.utils.Projection.TopLevelProjection#difference 方法注释
            ListIterator<int[]> resultProjectionIterator = resultProjection.listIterator();
            while (resultProjectionIterator.hasNext()) {
                int[] indexArr = resultProjectionIterator.next();

                // Let's check if the index is inside the indexesToExclude array
                int searchResult = Arrays.binarySearch(indexesToExclude, indexArr[0]);
                if (searchResult >= 0) {
                    // Found, we need to remove it
                    resultProjectionIterator.remove();
                } else {
                    // Not found, let's compute the offset.
                    // Offset is the index where the projection index should be inserted in the
                    // indexesToExclude array
                    int offset = (-(searchResult) - 1);
                    if (offset != 0) {
                        indexArr[0] = indexArr[0] - offset;
                    }
                }
            }

            return new NestedProjection(resultProjection.toArray(new int[0][]));
        }

        @Override
        public Projection complement(int fieldsNumber) {
            // 求补集
            if (isNested()) {
                throw new IllegalStateException("Cannot perform complement of a nested projection");
            }
            return new TopLevelProjection(toTopLevelIndexes()).complement(fieldsNumber);
        }

        @Override
        public int[] toTopLevelIndexes() {
            // 嵌套式的投影无法转换为顶层字段下标表示.
            if (isNested()) {
                throw new IllegalStateException(
                        "Cannot convert a nested projection to a top level projection");
            }
            return Arrays.stream(projection).mapToInt(arr -> arr[0]).toArray();
        }

        @Override
        public int[][] toNestedIndexes() {
            return projection;
        }
    }

    private static class TopLevelProjection extends Projection {

        final int[] projection;

        TopLevelProjection(int[] projection) {
            this.projection = projection;
        }

        @Override
        public RowType project(RowType rowType) {
            return new NestedProjection(toNestedIndexes()).project(rowType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Projection difference(Projection other) {
            if (other.isNested()) {
                throw new IllegalArgumentException(
                        "Cannot perform difference between top level projection and nested projection");
            }
            if (other instanceof EmptyProjection) {
                return this;
            }

            // Extract the indexes to exclude and sort them
            int[] indexesToExclude = other.toTopLevelIndexes();
            // 防止修改 other 中的 projection 对象
            indexesToExclude = Arrays.copyOf(indexesToExclude, indexesToExclude.length);
            Arrays.sort(indexesToExclude);

            List<Integer> resultProjection =
                    Arrays.stream(projection)
                            .boxed()
                            .collect(Collectors.toCollection(ArrayList::new));

            ListIterator<Integer> resultProjectionIterator = resultProjection.listIterator();
            while (resultProjectionIterator.hasNext()) {
                int index = resultProjectionIterator.next();

                // Let's check if the index is inside the indexesToExclude array
                // searchResult 是怎么决定的？
                // searchResult 是通过 Arrays.binarySearch(indexesToExclude, index) 来得到的，binarySearch
                // 方法返回：
                // - 如果在 indexesToExclude 中找到 index，返回值为该元素在数组中的索引，即 searchResult >= 0。
                // - 如果没找到，返回值将是一个负数，表示插入点的索引（负数形式），即 searchResult 为负数。这个负数值的计算公式实际上是 -(插入点) - 1。
                int searchResult = Arrays.binarySearch(indexesToExclude, index);
                if (searchResult >= 0) {
                    // Found, we need to remove it
                    resultProjectionIterator.remove();
                } else {
                    // 举例说明
                    // 假设有 Projection [4, 1, 0, 3, 2] 和 indexesToExclude [2, 4]：
                    //
                    // 原始 resultProjection 初始化为 [4, 1, 0, 3, 2]。
                    // 进行 binarySearch 和调整：
                    // 4 在 indexesToExclude 中（移除 4），结果变为 [1, 0, 3, 2]。
                    // 1 不在，searchResult = -1，计算 offset = 0，表示这个字段下标在要删除的最小字段之前，所以不用调整
                    // 0 不在，searchResult = -1，计算 offset = 0，表示这个字段下标在要删除的最小字段之前，所以不用调整
                    // 3 不在，searchResult = -2，计算 offset = 1，表示这个字段之前删除了一个字段，所以调整 3 - 1 = 2。
                    // 2 在 indexesToExclude 中（移除 2），结果变为 [1, 0, 2]。
                    // 最终，结果投影为 [1, 0, 2]。
                    // Not found, let's compute the offset.
                    // Offset is the index where the projection index should be inserted in the
                    // indexesToExclude array
                    int offset = (-(searchResult) - 1); // offset 就相当于插入点的位置，也可以理解为前面删除了几个字段
                    if (offset != 0) {
                        resultProjectionIterator.set(
                                index - offset); // index - offset 则表示 index 表示的字段需要前移 offset 位
                    }
                }
            }

            return new TopLevelProjection(resultProjection.stream().mapToInt(i -> i).toArray());
        }

        @Override
        public Projection complement(int fieldsNumber) {
            int[] indexesToExclude = Arrays.copyOf(projection, projection.length);
            Arrays.sort(indexesToExclude);

            // 求补集
            return new TopLevelProjection(
                    IntStream.range(0, fieldsNumber)
                            .filter(i -> Arrays.binarySearch(indexesToExclude, i) < 0)
                            .toArray());
        }

        @Override
        public int[] toTopLevelIndexes() {
            // 直接返回顶层投影.
            return projection;
        }

        @Override
        public int[][] toNestedIndexes() {
            return Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        }
    }
}
