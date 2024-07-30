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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

/**
 * flink 1.15 及以下版本使用，以上版本使用 {@link NewLookupFunction}.
 *
 * <p>二者的继承体系发生了变化，NewLookupFunction 继承自 LookupFunction，而 OldLookupFunction 继承自 TableFunction.
 *
 * <p>Old lookup {@link TableFunction} for 1.15-.
 */
public class OldLookupFunction extends TableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    // 不管是 OldLookupFunction 还是 NewLookupFunction，都依赖 FileStoreLookupFunction 来关联数据.
    private final FileStoreLookupFunction function;

    public OldLookupFunction(FileStoreLookupFunction function) {
        this.function = function;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        function.open(context);
    }

    /** Used by code generation. */
    @SuppressWarnings("unused")
    public void eval(Object... values) {
        function.lookup(GenericRowData.of(values)).forEach(this::collect);
    }

    @Override
    public void close() throws Exception {
        function.close();
    }
}
