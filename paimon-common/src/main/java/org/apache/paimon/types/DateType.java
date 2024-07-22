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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

/**
 * 日期类型，使用整数存储，表示与 1970-01-01 的偏移天数。
 *
 * <p>Data type of a date consisting of {@code year-month-day} with values ranging from {@code
 * 0000-01-01} to {@code 9999-12-31}. Compared to the SQL standard, the range starts at year {@code
 * 0000}.
 *
 * <p>A conversion from and to {@code int} describes the number of days since epoch.
 *
 * @since 0.4.0
 */
@Public
public final class DateType extends DataType {

    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "DATE";

    public DateType(boolean isNullable) {
        super(isNullable, DataTypeRoot.DATE);
    }

    public DateType() {
        this(true);
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new DateType(isNullable);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
