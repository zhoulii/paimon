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

package org.apache.paimon.mergetree;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 一个 SortedRun 可以包含多个文件，文件所表示的 key 范围不交叉.
 *
 * <p>Level-0 中，一个文件就是一个 sorted run，更上层都是一层一个 SortedRun.
 *
 * <p>A {@link SortedRun} is a list of files sorted by their keys. The key intervals [minKey,
 * maxKey] of these files do not overlap.
 */
public class SortedRun {

    private final List<DataFileMeta> files; // SortedRun 对应的所有文件

    private final long totalSize; // 所有文件大小之和

    private SortedRun(List<DataFileMeta> files) {
        this.files = Collections.unmodifiableList(files);
        long totalSize = 0L;
        for (DataFileMeta file : files) {
            totalSize += file.fileSize();
        }
        this.totalSize = totalSize;
    }

    public static SortedRun empty() {
        // 空的 SortedRun
        return new SortedRun(Collections.emptyList());
    }

    public static SortedRun fromSingle(DataFileMeta file) {
        // 包含单个文件的 SortedRun
        return new SortedRun(Collections.singletonList(file));
    }

    public static SortedRun fromSorted(List<DataFileMeta> sortedFiles) {
        // 包含多个文件的 SortedRun，文件已经按照 minKey 排序
        return new SortedRun(sortedFiles);
    }

    public static SortedRun fromUnsorted(
            List<DataFileMeta> unsortedFiles, Comparator<InternalRow> keyComparator) {
        // 文件先根据 minKey 排序，再创建 SortedRun
        unsortedFiles.sort((o1, o2) -> keyComparator.compare(o1.minKey(), o2.minKey()));
        SortedRun run = new SortedRun(unsortedFiles);
        // 确保文件数据不存在交叉
        run.validate(keyComparator);
        return run;
    }

    public List<DataFileMeta> files() {
        return files;
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }

    public boolean nonEmpty() {
        return !isEmpty();
    }

    public long totalSize() {
        return totalSize;
    }

    @VisibleForTesting
    public void validate(Comparator<InternalRow> comparator) {
        // 后一个文件的最小 KEY 大于前一个文件最大 KEY，以此保证文件数据不交叉
        for (int i = 1; i < files.size(); i++) {
            Preconditions.checkState(
                    comparator.compare(files.get(i).minKey(), files.get(i - 1).maxKey()) > 0,
                    "SortedRun is not sorted and may contain overlapping key intervals. This is a bug.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SortedRun)) {
            return false;
        }
        SortedRun that = (SortedRun) o;
        return files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files);
    }

    @Override
    public String toString() {
        return "["
                + files.stream().map(DataFileMeta::toString).collect(Collectors.joining(", "))
                + "]";
    }
}
