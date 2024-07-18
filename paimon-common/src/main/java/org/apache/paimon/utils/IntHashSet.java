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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.EOFException;

/**
 * 基于 IntOpenHashSet 实现，IntOpenHashSet
 * 是专门针对整数类型进行优化的哈希集合实现。它使用了基本的整数数组来存储元素，因此在存储大量整数时，相比于通用的集合实现，可以实现更高的性能和更低的内存占用.
 *
 * <p>A hash set for ints.
 */
public class IntHashSet {

    private final IntOpenHashSet set;

    public IntHashSet() {
        this.set = new IntOpenHashSet();
    }

    public IntHashSet(int expected) {
        // 预期元素数量
        this.set = new IntOpenHashSet(expected);
    }

    public boolean add(int value) {
        return set.add(value);
    }

    public int size() {
        return set.size();
    }

    public IntIterator toIntIterator() {
        it.unimi.dsi.fastutil.ints.IntIterator iterator = set.intIterator();
        return new IntIterator() {

            @Override
            public int next() throws EOFException {
                if (!iterator.hasNext()) {
                    throw new EOFException();
                }
                return iterator.nextInt();
            }

            @Override
            public void close() {}
        };
    }

    public int[] toInts() {
        return set.toIntArray();
    }
}
