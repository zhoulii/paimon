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

package org.apache.paimon.data;

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentSource;
import org.apache.paimon.utils.MathUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The list with the full segments contains at any point all completely full segments, plus the
 * segment that is currently filled.
 */
public class SimpleCollectingOutputView extends AbstractPagedOutputView {

    // output 的真正存储.
    private final List<MemorySegment> fullSegments;

    // 用于创建 MemorySegment.
    private final MemorySegmentSource memorySource;

    // MemorySegment 的大小对应以 2 为底的对数
    private final int segmentSizeBits;

    // fullSegments 中已经写满的 MemorySegment 的数量
    private int segmentNum;

    public SimpleCollectingOutputView(MemorySegmentSource memSource, int segmentSize) {
        this(new ArrayList<>(), memSource, segmentSize);
    }

    public SimpleCollectingOutputView(
            List<MemorySegment> fullSegmentTarget, MemorySegmentSource memSource, int segmentSize) {
        super(memSource.nextSegment(), segmentSize);
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        this.fullSegments = fullSegmentTarget;
        this.memorySource = memSource;
        this.fullSegments.add(getCurrentSegment());
    }

    public void reset() {
        // 重置状态，要求重置前 this.fullSegments 不包含元素.
        if (this.fullSegments.size() != 0) {
            throw new IllegalStateException("The target list still contains memory segments.");
        }

        clear(); // 清除写入状态
        try {
            advance(); // 初始化 current MemorySegment
        } catch (IOException ioex) {
            throw new RuntimeException("Error getting first segment for record collector.", ioex);
        }
        this.segmentNum = 0;
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws EOFException {
        // 获取一个 MemorySegment
        final MemorySegment next = this.memorySource.nextSegment();
        if (next != null) {
            this.fullSegments.add(next);
            this.segmentNum++;
            return next;
        } else {
            throw new EOFException("Can't collect further: memorySource depleted");
        }
    }

    public long getCurrentOffset() {
        // 计算当前 offset
        // 例如  this.segmentNum = 2， this.segmentSizeBits = 5，getCurrentPositionInSegment() = 3
        // 则 offset = 2 * 2^5 + 3 = 35
        return (((long) this.segmentNum) << this.segmentSizeBits) + getCurrentPositionInSegment();
    }
}
