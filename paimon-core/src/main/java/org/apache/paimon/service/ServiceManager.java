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

package org.apache.paimon.service;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * 用于管理远程服（现在仅支持 lookuo 服务），一张表对应一个实例.
 *
 * <p>A manager to manage services, for example, the service to lookup row from the primary key.
 */
public class ServiceManager implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SERVICE_PREFIX = "service-";

    // 服务 ID ，用于标识服务类型
    public static final String PRIMARY_KEY_LOOKUP = "primary-key-lookup";

    // 操作表所在文件系统
    private final FileIO fileIO;
    // 表的路径
    private final Path tablePath;

    public ServiceManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public Path tablePath() {
        return tablePath;
    }

    public Optional<InetSocketAddress[]> service(String id) {
        try {
            // 获取某种服务地址，ID 表示服务类型
            return fileIO.readOverwrittenFileUtf8(servicePath(id))
                    .map(s -> JsonSerdeUtil.fromJson(s, InetSocketAddress[].class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void resetService(String id, InetSocketAddress[] addresses) {
        try {
            // 重置某个类型服务地址
            fileIO.overwriteFileUtf8(servicePath(id), JsonSerdeUtil.toJson(addresses));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void deleteService(String id) {
        // 删除某个类型服务
        fileIO.deleteQuietly(servicePath(id));
    }

    private Path servicePath(String id) {
        // 获取某种服务的元数据文件地址
        return new Path(tablePath + "/service/" + SERVICE_PREFIX + id);
    }
}
