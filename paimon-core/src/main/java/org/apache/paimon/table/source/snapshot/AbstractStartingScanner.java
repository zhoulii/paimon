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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

/**
 * StartingScanner 的抽象实现.
 *
 * <p>The abstract class for StartingScanner.
 */
public abstract class AbstractStartingScanner implements StartingScanner {

    protected final SnapshotManager snapshotManager;

    protected Long startingSnapshotId = null;

    AbstractStartingScanner(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    protected ScanMode startingScanMode() {
        return ScanMode.DELTA;
    }

    /**
     * 这个方法的返回值似乎只是给 spark 使用的.
     *
     * @return StartingContext
     */
    @Override
    public StartingContext startingContext() {
        if (startingSnapshotId == null) {
            // 没有指定 startingSnapshotId，则返回 EMPTY，其中 snapshotId 为 1，scanFullSnapshot 为 false.
            return StartingContext.EMPTY;
        } else {
            return new StartingContext(startingSnapshotId, startingScanMode() == ScanMode.ALL);
        }
    }
}
