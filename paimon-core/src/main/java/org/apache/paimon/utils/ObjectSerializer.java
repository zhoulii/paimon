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
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用 InternalRowSerializer 实现对象序列化.
 *
 * <p>A serializer to serialize object by {@link InternalRowSerializer}.
 */
public abstract class ObjectSerializer<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final InternalRowSerializer rowSerializer;

    public ObjectSerializer(RowType rowType) {
        this.rowSerializer = InternalSerializers.create(rowType);
    }

    /** Get the number of fields. */
    public int numFields() {
        return rowSerializer.getArity();
    }

    public DataType[] fieldTypes() {
        return rowSerializer.fieldTypes();
    }

    /**
     * 序列化一个对象，先将其转换为 row，再执行序列化.
     *
     * <p>Serializes the given record to the given target output view.
     *
     * @param record The record to serialize.
     * @param target The output view to write the serialized data to.
     * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically
     *     raised by the output view, which may have an underlying I/O channel to which it
     *     delegates.
     */
    public final void serialize(T record, DataOutputView target) throws IOException {
        rowSerializer.serialize(toRow(record), target);
    }

    /**
     * 反序列化为 row，再转换为指定类型对象.
     *
     * <p>De-serializes a record from the given source input view.
     *
     * @param source The input view from which to read the data.
     * @return The deserialized element.
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
     *     Typically raised by the input view, which may have an underlying I/O channel from which
     *     it reads.
     */
    public final T deserialize(DataInputView source) throws IOException {
        return fromRow(rowSerializer.deserialize(source));
    }

    /**
     * 序列化一个 list.
     *
     * <p>Serializes the given record list to the given target output view.
     */
    public final void serializeList(List<T> records, DataOutputView target) throws IOException {
        target.writeInt(records.size());
        for (T t : records) {
            serialize(t, target);
        }
    }

    public final byte[] serializeList(List<T> records) throws IOException {
        // 序列化一个 list.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);
        serializeList(records, view);
        return baos.toByteArray();
    }

    /** De-serializes a record list from the given source input view. */
    public final List<T> deserializeList(DataInputView source) throws IOException {
        // 反序列化一个 list.
        int size = source.readInt();
        List<T> records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            records.add(deserialize(source));
        }
        return records;
    }

    public final List<T> deserializeList(byte[] bytes) throws IOException {
        // 反序列化一个 list.
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bais);
        return deserializeList(view);
    }

    /**
     * 具体子类实现决定如何转换为 row.
     *
     * <p>Convert a {@link T} to {@link InternalRow}.
     */
    public abstract InternalRow toRow(T record);

    /**
     * 具体子类实现决定如何如何从 row 转换成一个具体对象.
     *
     * <p>Convert a {@link InternalRow} to {@link T}.
     */
    public abstract T fromRow(InternalRow rowData);
}
