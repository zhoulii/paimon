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

package org.apache.paimon.mergetree.compact;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Factory to create {@link MergeFunction}. */
@FunctionalInterface
public interface MergeFunctionFactory<T> extends Serializable {

    default MergeFunction<T> create() {
        return create(null);
    }

    MergeFunction<T> create(@Nullable int[][] projection);

    default AdjustedProjection adjustProjection(@Nullable int[][] projection) {
        return new AdjustedProjection(projection, null);
    }

    /**
     * 调整后的 projection. 可参考
     * org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.Factory#adjustProjection 来理解.
     *
     * <p>Result of adjusted projection.
     */
    class AdjustedProjection {

        // 中间层需要处理的字段索引，可能在 outer projection 上加了一些字段
        // 如加入一些 sequence 字段
        @Nullable public final int[][] pushdownProjection;

        // 真正需要的结果字段索引
        @Nullable public final int[][] outerProjection;

        public AdjustedProjection(int[][] pushdownProjection, int[][] outerProjection) {
            this.pushdownProjection = pushdownProjection;
            this.outerProjection = outerProjection;
        }
    }
}
