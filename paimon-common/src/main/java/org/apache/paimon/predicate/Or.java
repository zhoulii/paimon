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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** A {@link CompoundPredicate.Function} to eval or. */
public class Or extends CompoundPredicate.Function {

    private static final long serialVersionUID = 1L;

    public static final Or INSTANCE = new Or();

    private Or() {}

    @Override
    public boolean test(InternalRow row, List<Predicate> children) {
        for (Predicate child : children) {
            if (child.test(row)) {
                // 任意一个匹配则返回true
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean test(
            long rowCount,
            InternalRow minValues,
            InternalRow maxValues,
            InternalArray nullCounts,
            List<Predicate> children) {
        for (Predicate child : children) {
            if (child.test(rowCount, minValues, maxValues, nullCounts)) {
                // 任意一个匹配则返回true
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<Predicate> negate(List<Predicate> children) {
        // 对每一个条件取反，如果某个子条件没反，则整体没反
        List<Predicate> negatedChildren = new ArrayList<>();
        for (Predicate child : children) {
            Optional<Predicate> negatedChild = child.negate();
            if (negatedChild.isPresent()) {
                negatedChildren.add(negatedChild.get());
            } else {
                return Optional.empty();
            }
        }
        // 组合为 AND 条件
        // 比如 ： x == a or x == b，相反表示则为 x != a and x != b
        return Optional.of(new CompoundPredicate(And.INSTANCE, negatedChildren));
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, List<T> children) {
        return visitor.visitOr(children);
    }
}
