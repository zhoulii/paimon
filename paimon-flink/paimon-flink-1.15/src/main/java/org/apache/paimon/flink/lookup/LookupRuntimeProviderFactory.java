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

package org.apache.paimon.flink.lookup;

import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;

/**
 * 用于获取 lookup 运行时所需的一些信息.
 *
 * <p>和 paimon-flink-common 中同名类的区别是： - 使用 OldLookupFunction - 不支持异步 lookup join
 *
 * <p>Factory to create {@link LookupRuntimeProvider}.
 */
public class LookupRuntimeProviderFactory {

    public static LookupRuntimeProvider create(
            FileStoreLookupFunction function, boolean enableAsync, int asyncThreadNumber) {
        // TableFunctionProvider 是 LookupRuntimeProvider 子类，能获取 lookup function.
        return TableFunctionProvider.of(new OldLookupFunction(function));
    }
}
