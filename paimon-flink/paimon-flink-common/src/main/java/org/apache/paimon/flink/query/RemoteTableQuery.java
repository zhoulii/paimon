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

package org.apache.paimon.flink.query;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.query.QueryLocationImpl;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.service.client.KvQueryClient;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;

/**
 * 查询远程服务来做维表 join.
 *
 * <p>Implementation for {@link TableQuery} to lookup data from remote service.
 */
public class RemoteTableQuery implements TableQuery {

    private final FileStoreTable table;

    // 根据 KEY 从远程查询 VALUE
    private final KvQueryClient client;
    // KEY 序列化器
    private final InternalRowSerializer keySerializer;

    // VALUE 的投影
    @Nullable private int[] projection;

    public RemoteTableQuery(Table table) {
        this.table = (FileStoreTable) table;
        // 获取 ServiceManager，用于获取一张表中某个 bucket 所在服务地址
        ServiceManager manager = this.table.store().newServiceManager();
        // 创建查询客户端
        this.client = new KvQueryClient(new QueryLocationImpl(manager), 1);
        // KEY 序列化器
        this.keySerializer =
                InternalSerializers.create(TypeUtils.project(table.rowType(), table.primaryKeys()));
    }

    public static boolean isRemoteServiceAvailable(FileStoreTable table) {
        // 判断 lookup 服务是否存在
        return table.store().newServiceManager().service(PRIMARY_KEY_LOOKUP).isPresent();
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        // 从远程服务查询 value.
        BinaryRow row;
        try {
            row =
                    client.getValues(
                                    partition,
                                    bucket,
                                    new BinaryRow[] {keySerializer.toBinaryRow(key)})
                            .get()[0];
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }

        if (projection == null) {
            return row;
        }

        if (row == null) {
            return null;
        }

        return ProjectedRow.from(projection).replaceRow(row);
    }

    @Override
    public RemoteTableQuery withValueProjection(int[] projection) {
        // 设置 value 投影
        return withValueProjection(Projection.of(projection).toNestedIndexes());
    }

    @Override
    public RemoteTableQuery withValueProjection(int[][] projection) {
        this.projection = Projection.of(projection).toTopLevelIndexes();
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        // 创建 value 序列化器
        return InternalSerializers.create(TypeUtils.project(table.rowType(), projection));
    }

    @Override
    public void close() throws IOException {
        // 关闭查询客户端
        client.shutdown();
    }
}
