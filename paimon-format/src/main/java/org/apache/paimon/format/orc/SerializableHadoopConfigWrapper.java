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

package org.apache.paimon.format.orc;

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 工具类，序列化反序列化 hadoop configuration.
 *
 * <p>Utility class to make a {@link Configuration Hadoop Configuration} serializable.
 */
public final class SerializableHadoopConfigWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient Configuration hadoopConfig;

    public SerializableHadoopConfigWrapper(Configuration hadoopConfig) {
        this.hadoopConfig = checkNotNull(hadoopConfig);
    }

    public Configuration getHadoopConfig() {
        return hadoopConfig;
    }

    // ------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        // 通过单独的序列化器将Hadoop配置序列化，是为了避免在序列化过程中出现“cryptic exceptions”（晦涩难懂的异常）
        // 这意味着直接使用默认的序列化方法来序列化Hadoop配置可能会导致一些难以理解的异常。
        // 使用单独的序列化器可以更清晰地处理可能出现的异常，这样可能能更好地理解和调试代码中的任何与序列化相关的问题。
        // we write the Hadoop config through a separate serializer to avoid cryptic exceptions when
        // it
        // corrupts the serialization stream
        final DataOutputSerializer ser = new DataOutputSerializer(256);
        hadoopConfig.write(ser);
        out.writeInt(ser.length());
        out.write(ser.getSharedBuffer(), 0, ser.length());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        final byte[] data = new byte[in.readInt()];
        in.readFully(data);
        final DataInputDeserializer deser = new DataInputDeserializer(data);
        this.hadoopConfig = new Configuration();

        try {
            this.hadoopConfig.readFields(deser);
        } catch (IOException e) {
            throw new IOException(
                    "Could not deserialize Hadoop Configuration, the serialized and de-serialized don't match.",
                    e);
        }
    }
}
