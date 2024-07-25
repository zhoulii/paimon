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
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 保存一个 LSM 结构的所有文件，也就是一个 bucket 中的所有文件.
 *
 * <p>A class which stores all level files of merge tree.
 */
public class Levels {

    private final Comparator<InternalRow> keyComparator;

    private final TreeSet<DataFileMeta> level0; // level0 中的文件

    private final List<SortedRun> levels; // level 1 到 level n 的 SortedRun

    private final List<DropFileCallback> dropFileCallbacks = new ArrayList<>();

    public Levels(
            Comparator<InternalRow> keyComparator, List<DataFileMeta> inputFiles, int numLevels) {
        this.keyComparator = keyComparator;

        // in case the num of levels is not specified explicitly
        // level 层数，如果没指定则取文件最大层 + 1
        int restoredNumLevels =
                Math.max(
                        numLevels,
                        inputFiles.stream().mapToInt(DataFileMeta::level).max().orElse(-1) + 1);
        checkArgument(restoredNumLevels > 1, "Number of levels must be at least 2.");
        this.level0 =
                // 先按 max sequence number 排序，大的在前，小的在后，也就是新文件在前，旧文件在后
                // 如果两个文件 max sequence number 一样，则按名称排序，而不是当做相同的文件被 TreeSet 去重
                new TreeSet<>(
                        (a, b) -> {
                            if (a.maxSequenceNumber() != b.maxSequenceNumber()) {
                                // file with larger sequence number should be in front
                                return Long.compare(b.maxSequenceNumber(), a.maxSequenceNumber());
                            } else {
                                // When two or more jobs are writing the same merge tree, it is
                                // possible that multiple files have the same maxSequenceNumber. In
                                // this case we have to compare their file names so that files with
                                // same maxSequenceNumber won't be "de-duplicated" by the tree set.
                                return a.fileName().compareTo(b.fileName());
                            }
                        });
        this.levels = new ArrayList<>();
        for (int i = 1; i < restoredNumLevels; i++) {
            levels.add(SortedRun.empty()); // level 1 及之后每层先放个空的 SortedMap
        }

        // 获取每一层的文件
        Map<Integer, List<DataFileMeta>> levelMap = new HashMap<>();
        for (DataFileMeta file : inputFiles) {
            levelMap.computeIfAbsent(file.level(), level -> new ArrayList<>()).add(file);
        }
        // 更新每一层
        levelMap.forEach((level, files) -> updateLevel(level, emptyList(), files));

        // 验证文件数量是否对得上
        Preconditions.checkState(
                level0.size() + levels.stream().mapToInt(r -> r.files().size()).sum()
                        == inputFiles.size(),
                "Number of files stored in Levels does not equal to the size of inputFiles. This is unexpected.");
    }

    public TreeSet<DataFileMeta> level0() {
        return level0;
    }

    public void addDropFileCallback(DropFileCallback callback) {
        dropFileCallbacks.add(callback);
    }

    public void addLevel0File(DataFileMeta file) {
        // level-0 添加文件
        checkArgument(file.level() == 0);
        level0.add(file);
    }

    public SortedRun runOfLevel(int level) {
        // 获取某层（level > 0）的 SortedRun，目的是获取某层的所有文件
        checkArgument(level > 0, "Level0 does not have one single sorted run.");
        return levels.get(level - 1);
    }

    public int numberOfLevels() {
        // 文件层数
        return levels.size() + 1;
    }

    public int maxLevel() {
        // 最大层编号
        return levels.size();
    }

    public int numberOfSortedRuns() {
        // 所有非空 SortedRun 的数量
        int numberOfSortedRuns = level0.size();
        for (SortedRun run : levels) {
            if (run.nonEmpty()) {
                numberOfSortedRuns++;
            }
        }
        return numberOfSortedRuns;
    }

    /**
     * 有数据的最高层 level.
     *
     * @return the highest non-empty level or -1 if all levels empty.
     */
    public int nonEmptyHighestLevel() {
        int i;
        for (i = levels.size() - 1; i >= 0; i--) {
            if (levels.get(i).nonEmpty()) {
                return i + 1;
            }
        }
        return level0.isEmpty() ? -1 : 0;
    }

    public List<DataFileMeta> allFiles() {
        // bucket 所有文件
        List<DataFileMeta> files = new ArrayList<>();
        List<LevelSortedRun> runs = levelSortedRuns();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return files;
    }

    public List<LevelSortedRun> levelSortedRuns() {
        List<LevelSortedRun> runs = new ArrayList<>();
        // level-0 每个文件都是一个 SortedRun
        level0.forEach(file -> runs.add(new LevelSortedRun(0, SortedRun.fromSingle(file))));
        for (int i = 0; i < levels.size(); i++) {
            // level-1 及之上，每层一个 SortedRun
            SortedRun run = levels.get(i);
            if (run.nonEmpty()) {
                runs.add(new LevelSortedRun(i + 1, run));
            }
        }
        return runs;
    }

    /**
     * 更新整个 bucket 的文件.
     *
     * @param before 所有层删除的文件
     * @param after 所有层新增的文件.
     */
    public void update(List<DataFileMeta> before, List<DataFileMeta> after) {
        // 将文件按层分组
        Map<Integer, List<DataFileMeta>> groupedBefore = groupByLevel(before);
        Map<Integer, List<DataFileMeta>> groupedAfter = groupByLevel(after);

        // 逐层更新
        for (int i = 0; i < numberOfLevels(); i++) {
            updateLevel(
                    i,
                    groupedBefore.getOrDefault(i, emptyList()),
                    groupedAfter.getOrDefault(i, emptyList()));
        }

        if (dropFileCallbacks.size() > 0) {
            Set<String> droppedFiles =
                    before.stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
            // exclude upgrade files upgrade 表示文件没被删除，而是 level 提升了
            after.stream().map(DataFileMeta::fileName).forEach(droppedFiles::remove);
            for (DropFileCallback callback : dropFileCallbacks) {
                // 执行删除文件的回调操作
                droppedFiles.forEach(callback::notifyDropFile);
            }
        }
    }

    /**
     * 更新指定 level 的文件.
     *
     * @param level 更新哪个 level.
     * @param before 删除的文件.
     * @param after 新增的文件.
     */
    private void updateLevel(int level, List<DataFileMeta> before, List<DataFileMeta> after) {
        if (before.isEmpty() && after.isEmpty()) {
            return;
        }

        if (level == 0) {
            before.forEach(level0::remove);
            level0.addAll(after);
        } else {
            // 获取这层的所有文件
            List<DataFileMeta> files = new ArrayList<>(runOfLevel(level).files());
            // 删除文件
            files.removeAll(before);
            // 新增文件
            files.addAll(after);
            // 设置新文件列表
            levels.set(level - 1, SortedRun.fromUnsorted(files, keyComparator));
        }
    }

    private Map<Integer, List<DataFileMeta>> groupByLevel(List<DataFileMeta> files) {
        // 将文件按 Level 分组
        return files.stream()
                .collect(Collectors.groupingBy(DataFileMeta::level, Collectors.toList()));
    }

    /**
     * 删除文件时的回调接口，LookupLevels 是其唯一实现类.
     *
     * <p>A callback to notify dropping file.
     */
    public interface DropFileCallback {

        void notifyDropFile(String file);
    }
}
