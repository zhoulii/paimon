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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BiFunctionWithIOE;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/** Utils for lookup. */
public class LookupUtils {

    public static <T> T lookup(
            Levels levels,
            InternalRow key,
            int startLevel,
            BiFunctionWithIOE<InternalRow, SortedRun, T> lookup,
            BiFunctionWithIOE<InternalRow, TreeSet<DataFileMeta>, T> level0Lookup)
            throws IOException {

        T result = null;
        for (int i = startLevel; i < levels.numberOfLevels(); i++) {
            if (i == 0) {
                // 查找 level 0
                result = level0Lookup.apply(key, levels.level0());
            } else {
                // 查找 level 0 之上的 SortedRun
                SortedRun level = levels.runOfLevel(i);
                result = lookup.apply(key, level);
            }
            if (result != null) {
                // 找到就退出，Levels 中，level0 中的文件最新的文件排在开始位置
                // lookup join 查找 level-0 的文件可能会导致结果不准确
                // - level-0 max sequence number 相同时，文件不保序
                // 是否给修改成 lookup 只查找 full compaction 或 lookup compaction 类型 snapshot 对应的 bucket 文件？
                break;
            }
        }

        return result;
    }

    public static <T> T lookupLevel0(
            Comparator<InternalRow> keyComparator,
            InternalRow target,
            TreeSet<DataFileMeta> level0,
            BiFunctionWithIOE<InternalRow, DataFileMeta, T> lookup)
            throws IOException {
        // 在 level0 中查找，先判断区间，再根据 KEY 查询
        T result = null;
        for (DataFileMeta file : level0) {
            if (keyComparator.compare(file.maxKey(), target) >= 0
                    && keyComparator.compare(file.minKey(), target) <= 0) {
                result = lookup.apply(target, file);
                if (result != null) {
                    break;
                }
            }
        }

        return result;
    }

    public static <T> T lookup(
            Comparator<InternalRow> keyComparator,
            InternalRow target,
            SortedRun level,
            BiFunctionWithIOE<InternalRow, DataFileMeta, T> lookup)
            throws IOException {
        if (level.isEmpty()) {
            return null;
        }
        List<DataFileMeta> files = level.files();
        int left = 0;
        int right = files.size() - 1;

        // 二分查找 KEY 在 SortedRun 中的那个文件里
        // binary search restart positions to find the restart position immediately before the
        // targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (keyComparator.compare(files.get(mid).maxKey(), target) < 0) {
                // Key at "mid.max" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.max" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }

        int index = right;

        // if the index is now pointing to the last file, check if the largest key in the block is
        // than the target key.  If so, we need to seek beyond the end of this file
        if (index == files.size() - 1
                && keyComparator.compare(files.get(index).maxKey(), target) < 0) {
            // KEY 大于 SortedRun 的最大 key
            index++;
        }

        // 查找文件或直接返回 null.
        // todo 可以进一步优化，如果 KEY 小于最小 KEY 或大于最大 KEY 则直接返回 null，放到二分查找之前
        // todo 这里还可以判断下 KEY 是否在文件表示的区间，不在则直接返回 null
        // if files does not have a next, it means the key does not exist in this level
        return index < files.size() ? lookup.apply(target, files.get(index)) : null;
    }

    // 文件大小的 KB 表示
    public static int fileKibiBytes(File file) {
        long kibiBytes = file.length() >> 10; // 除以 1024
        if (kibiBytes > Integer.MAX_VALUE) {
            throw new RuntimeException(
                    "Lookup file is too big: " + MemorySize.ofKibiBytes(kibiBytes));
        }
        return (int) kibiBytes;
    }
}
