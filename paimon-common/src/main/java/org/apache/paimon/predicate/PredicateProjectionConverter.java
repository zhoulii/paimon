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

package org.apache.paimon.predicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 组合投影字段的谓词判断.
 *
 * <p>A {@link PredicateVisitor} which converts {@link Predicate} with projection.
 */
public class PredicateProjectionConverter implements PredicateVisitor<Optional<Predicate>> {

    // 存储原始字段和投影字段映射
    private final Map<Integer, Integer> reversed;

    public PredicateProjectionConverter(int[] projection) {
        this.reversed = new HashMap<>();
        for (int i = 0; i < projection.length; i++) {
            reversed.put(projection[i], i);
        }
    }

    @Override
    public Optional<Predicate> visit(LeafPredicate predicate) {
        int index = predicate.index(); // 是对第几个字段的判断
        Integer adjusted = reversed.get(index); // 是否是投影字段
        if (adjusted == null) { // 不是则不需要这个判断
            return Optional.empty();
        }

        return Optional.of(predicate.copyWithNewIndex(adjusted)); // 需要将 index 修改为投影字段 index.
    }

    @Override
    public Optional<Predicate> visit(CompoundPredicate predicate) {
        List<Predicate> converted = new ArrayList<>();
        boolean isAnd = predicate.function() instanceof And;
        for (Predicate child : predicate.children()) {
            Optional<Predicate> optional = child.visit(this);
            if (optional.isPresent()) {
                converted.add(optional.get());
            } else { // 不是对投影字段的判断，如果是 and function，则取出是投影字段的谓词，因为是 and 关系，这个投影字段的谓词判断是必须满足的
                if (!isAnd) { // 如果是 or function，则忽略这个谓词，因为对于 or 关系，不能要求投影字段的谓词判断必须满足
                    // 例如 A OR B，谓词 A 是投影字段谓词、谓词 B 是非投影字段谓词，组合条件时不能只把谓词A 加入，要求必须满足，因为不满足实际上也不代表不行
                    // 比如谓词 B 是 1==1，谓词 A 满不满足都无所谓，而如果将其加入组合，那么还可能丢失部分数据.
                    return Optional.empty();
                }
            }
        }
        return Optional.of(new CompoundPredicate(predicate.function(), converted));
    }
}
