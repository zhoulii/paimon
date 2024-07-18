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

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.getBranchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/**
 * 一个工具类，提供与 snapshot、changelog 相关方法.
 *
 * <p>Manager for {@link Snapshot}, providing utility methods related to paths and snapshot hints.
 */
public class SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final String CHANGELOG_PREFIX = "changelog-";

    // LATEST 与 EARLIEST 也被称作 HINT.
    public static final String EARLIEST = "EARLIEST";
    public static final String LATEST = "LATEST";

    // 读取 HINT 重试次数.
    private static final int READ_HINT_RETRY_NUM = 3;

    // 读取 HINT 重试间隔.
    private static final int READ_HINT_RETRY_INTERVAL = 1;

    private final FileIO fileIO;
    private final Path tablePath;

    public SnapshotManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public Path tablePath() {
        return tablePath;
    }

    public Path snapshotDirectory() {
        // main 分支 snapshot 目录.
        return new Path(tablePath + "/snapshot");
    }

    public Path changelogDirectory() {
        // changelog 目录
        return new Path(tablePath + "/changelog");
    }

    public Path longLivedChangelogPath(long snapshotId) {
        // 解耦后的 changelog 目录
        return new Path(tablePath + "/changelog/" + CHANGELOG_PREFIX + snapshotId);
    }

    public Path snapshotPath(long snapshotId) {
        // main 分支 snapshot 路径.
        return new Path(tablePath + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Path branchSnapshotDirectory(String branchName) {
        // 找到 branch 的 snapshot 目录：${tablePath}/branch/branch-${branchName}/snapshot
        return new Path(getBranchPath(tablePath, branchName) + "/snapshot");
    }

    public Path branchSnapshotPath(String branchName, long snapshotId) {
        // 找到 branch 的某个
        // snapshot：${tablePath}/branch/branch-${branchName}/snapshot/snapshot-${snapshotId}
        return new Path(
                getBranchPath(tablePath, branchName) + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Path snapshotPathByBranch(String branchName, long snapshotId) {
        // 找到某个 branch 的某个 snapshot.
        return branchName.equals(DEFAULT_MAIN_BRANCH)
                ? snapshotPath(snapshotId)
                : branchSnapshotPath(branchName, snapshotId);
    }

    public Path snapshotDirByBranch(String branchName) {
        // 找到 branch 的 snapshot 目录.
        return branchName.equals(DEFAULT_MAIN_BRANCH)
                ? snapshotDirectory()
                : branchSnapshotDirectory(branchName);
    }

    public Snapshot snapshot(long snapshotId) {
        // 获取指定的 snapshot.
        return snapshot(DEFAULT_MAIN_BRANCH, snapshotId);
    }

    public Changelog changelog(long snapshotId) {
        // 获取指定的 snapshot 对应的 changelog.
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.fromPath(fileIO, changelogPath);
    }

    public Changelog longLivedChangelog(long snapshotId) {
        // 获取指定的 snapshot 对应的 changelog，和上面的方法一样.
        return Changelog.fromPath(fileIO, longLivedChangelogPath(snapshotId));
    }

    public Snapshot snapshot(String branchName, long snapshotId) {
        // 获取某个 branch 的某个 snapshot.
        Path snapshotPath = snapshotPathByBranch(branchName, snapshotId);
        return Snapshot.fromPath(fileIO, snapshotPath);
    }

    public boolean snapshotExists(long snapshotId) {
        // 判断 snapshot 是否存在.
        Path path = snapshotPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if snapshot #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    public boolean longLivedChangelogExists(long snapshotId) {
        // 判断 changelog 是否存在.
        Path path = longLivedChangelogPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if changelog #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    public @Nullable Snapshot latestSnapshot() {
        // 最新的 snapshot.
        return latestSnapshot(DEFAULT_MAIN_BRANCH);
    }

    public @Nullable Snapshot latestSnapshot(String branchName) {
        // 最新的 snapshot.
        Long snapshotId = latestSnapshotId(branchName);
        return snapshotId == null ? null : snapshot(branchName, snapshotId);
    }

    public @Nullable Long latestSnapshotId() {
        // 最新的 snapshot id.
        return latestSnapshotId(DEFAULT_MAIN_BRANCH);
    }

    public @Nullable Long latestSnapshotId(String branchName) {
        try {
            // 最新的 snapshot id.
            return findLatest(snapshotDirByBranch(branchName), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    public @Nullable Snapshot earliestSnapshot() {
        // 最早的 snapshot.
        Long snapshotId = earliestSnapshotId();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    public @Nullable Long earliestSnapshotId() {
        // 最早的 snapshot.
        return earliestSnapshotId(DEFAULT_MAIN_BRANCH);
    }

    public @Nullable Long earliestLongLivedChangelogId() {
        // 找到最早的 changelog.
        try {
            return findEarliest(
                    changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest changelog id", e);
        }
    }

    public @Nullable Long latestLongLivedChangelogId() {
        try {
            // 找到最新的 changelog.
            return findLatest(changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest changelog id", e);
        }
    }

    public @Nullable Long latestChangelogId() {
        // 找到最新的 changelog id.
        return latestSnapshotId();
    }

    public @Nullable Long earliestSnapshotId(String branchName) {
        try {
            // 找到某个分支的最早 snapshot id.
            return findEarliest(
                    snapshotDirByBranch(branchName), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest snapshot id", e);
        }
    }

    public @Nullable Long pickOrLatest(Predicate<Snapshot> predicate) {
        // 找到符合过滤规则的 snapshot，或者返回最新的 snapshot.
        Long latestId = latestSnapshotId();
        Long earliestId = earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return null;
        }

        for (long snapshotId = latestId; snapshotId >= earliestId; snapshotId--) {
            if (snapshotExists(snapshotId)) {
                Snapshot snapshot = snapshot(snapshotId);
                if (predicate.test(snapshot)) {
                    return snapshot.id();
                }
            }
        }

        return latestId;
    }

    private Snapshot changelogOrSnapshot(long snapshotId) {
        // changelog 如果存在就读 changelog，不存在就读 snapshot.
        if (longLivedChangelogExists(snapshotId)) {
            return changelog(snapshotId);
        } else {
            return snapshot(snapshotId);
        }
    }

    /**
     * 找到 timestampMills 之前的一个 snapshot.
     *
     * <p>Returns the latest snapshot earlier than the timestamp mills. A non-existent snapshot may
     * be returned if all snapshots are equal to or later than the timestamp mills.
     */
    public @Nullable Long earlierThanTimeMills(long timestampMills, boolean startFromChangelog) {
        Long earliestSnapshot = earliestSnapshotId();
        Long earliest;
        if (startFromChangelog) {
            Long earliestChangelog = earliestLongLivedChangelogId();
            earliest = earliestChangelog == null ? earliestSnapshot : earliestChangelog;
        } else {
            earliest = earliestSnapshot;
        }
        Long latest = latestSnapshotId();

        if (earliest == null || latest == null) {
            return null;
        }

        if (changelogOrSnapshot(earliest).timeMillis() >= timestampMills) {
            // 没有比 timestampMills 小的 snapshot，返回 earliest - 1.
            return earliest - 1;
        }

        while (earliest < latest) {
            // 二分查找找到 timestampMills 之前的 snapshot.
            long mid = (earliest + latest + 1) / 2;
            if (changelogOrSnapshot(mid).timeMillis() < timestampMills) {
                earliest = mid;
            } else {
                latest = mid - 1;
            }
        }
        return earliest;
    }

    /**
     * 找到小于等于某个时间戳的 snapshot.
     *
     * <p>Returns a {@link Snapshot} whoes commit time is earlier than or equal to given timestamp
     * mills. If there is no such a snapshot, returns null.
     */
    public @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        if (snapshot(earliest).timeMillis() > timestampMills) {
            return null;
        }
        Snapshot finalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // Search in the left half
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // Search in the right half
                finalSnapshot = snapshot;
            } else {
                finalSnapshot = snapshot; // Found the exact match
                break;
            }
        }
        return finalSnapshot;
    }

    public @Nullable Snapshot laterOrEqualWatermark(long watermark) {
        // 找到 watermark 大于或等于 watermark 的 snapshot.
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        // watermark 来源于数据源的 watermark.
        // org.apache.paimon.flink.sink.CommitterOperator.processWatermark 从这个方法可以看出，对于 batch 作业或者
        // bounded stream
        // 作业，watermark 是不会更新的，默认值就是 Long.MIN_VALUE.
        // 数据源没有指定 watermark 的情况，snapshot 的 watermark 也会被设置为 Long.MIN_VALUE.
        // 对于这种情况，laterOrEqualWatermark 没有太大意义了.
        // If latest == Long.MIN_VALUE don't need next binary search for watermark
        // which can reduce IO cost with snapshot
        if (earliest == null || latest == null || snapshot(latest).watermark() == Long.MIN_VALUE) {
            return null;
        }
        Long earliestWatermark = null;
        // watermark 什么时候为 null ？ paimon <= 0.3 时，watermark 为 null.
        // find the first snapshot with watermark
        if ((earliestWatermark = snapshot(earliest).watermark()) == null) {
            while (earliest < latest) {
                earliest++;
                earliestWatermark = snapshot(earliest).watermark();
                if (earliestWatermark != null) {
                    break;
                }
            }
        }
        if (earliestWatermark == null) {
            return null;
        }

        // 第一个 watermark 不为空的 snapshot，当其 watermark 大于参数时 watermark，返回这个 snapshot.
        if (earliestWatermark >= watermark) {
            return snapshot(earliest);
        }
        Snapshot finalSnapshot = null;

        // 二分查找 watermark.
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            Long commitWatermark = snapshot.watermark();
            if (commitWatermark == null) {
                // find the first snapshot with watermark
                while (mid >= earliest) {
                    mid--;
                    commitWatermark = snapshot(mid).watermark();
                    if (commitWatermark != null) {
                        break;
                    }
                }
            }
            if (commitWatermark == null) {
                earliest = mid + 1;
            } else {
                if (commitWatermark > watermark) {
                    latest = mid - 1; // Search in the left half
                    finalSnapshot = snapshot;
                } else if (commitWatermark < watermark) {
                    earliest = mid + 1; // Search in the right half
                } else {
                    finalSnapshot = snapshot; // Found the exact match
                    break;
                }
            }
        }
        return finalSnapshot;
    }

    public long snapshotCount() throws IOException {
        // snapshot 数量
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX).count();
    }

    public Iterator<Snapshot> snapshots() throws IOException {
        // 获取 snapshot 迭代器
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                .map(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public Iterator<Snapshot> snapshotsWithinRange(
            Optional<Long> optionalMaxSnapshotId, Optional<Long> optionalMinSnapshotId)
            throws IOException {
        // 获取指定区间的 snapshot.
        Long lowerBoundSnapshotId = earliestSnapshotId();
        Long upperBoundSnapshotId = latestSnapshotId();

        // null check on lowerBoundSnapshotId & upperBoundSnapshotId
        if (lowerBoundSnapshotId == null || upperBoundSnapshotId == null) {
            return Collections.emptyIterator();
        }

        if (optionalMaxSnapshotId.isPresent()) {
            upperBoundSnapshotId = optionalMaxSnapshotId.get();
        }

        if (optionalMinSnapshotId.isPresent()) {
            lowerBoundSnapshotId = optionalMinSnapshotId.get();
        }

        // +1 here to include the upperBoundSnapshotId
        return LongStream.range(lowerBoundSnapshotId, upperBoundSnapshotId + 1)
                .mapToObj(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public Iterator<Changelog> changelogs() throws IOException {
        // 获取全量 changelog.
        return listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                .map(this::changelog)
                .sorted(Comparator.comparingLong(Changelog::id))
                .iterator();
    }

    /**
     * 获取所有 snapshot，忽略 FileNotFoundException.
     *
     * <p>If {@link FileNotFoundException} is thrown when reading the snapshot file, this snapshot
     * may be deleted by other processes, so just skip this snapshot.
     */
    public List<Snapshot> safelyGetAllSnapshots() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                        .map(this::snapshotPath)
                        .collect(Collectors.toList());

        List<Snapshot> snapshots = new ArrayList<>();
        for (Path path : paths) {
            Snapshot snapshot = Snapshot.safelyFromPath(fileIO, path);
            if (snapshot != null) {
                snapshots.add(snapshot);
            }
        }

        return snapshots;
    }

    public List<Changelog> safelyGetAllChangelogs() throws IOException {
        // 获取所有 changelog，忽略 FileNotFoundException.
        List<Path> paths =
                listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                        .map(this::longLivedChangelogPath)
                        .collect(Collectors.toList());

        List<Changelog> changelogs = new ArrayList<>();
        for (Path path : paths) {
            try {
                String json = fileIO.readFileUtf8(path);
                changelogs.add(Changelog.fromJson(json));
            } catch (FileNotFoundException ignored) {
            }
        }

        return changelogs;
    }

    /**
     * 获取 snapshot 目录下非 snapshot 文件.
     *
     * <p>Try to get non snapshot files. If any error occurred, just ignore it and return an empty
     * result.
     */
    public List<Path> tryGetNonSnapshotFiles(Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(snapshotDirectory(), fileStatusFilter, nonSnapshotFileFilter());
    }

    public List<Path> tryGetNonChangelogFiles(Predicate<FileStatus> fileStatusFilter) {
        // 获取 changelog 目录下非 changelog 文件.
        return listPathWithFilter(changelogDirectory(), fileStatusFilter, nonChangelogFileFilter());
    }

    private List<Path> listPathWithFilter(
            Path directory, Predicate<FileStatus> fileStatusFilter, Predicate<Path> fileFilter) {
        // 过滤列出的文件.
        try {
            FileStatus[] statuses = fileIO.listStatus(directory);
            if (statuses == null) {
                return Collections.emptyList();
            }

            return Arrays.stream(statuses)
                    .filter(fileStatusFilter)
                    .map(FileStatus::getPath)
                    .filter(fileFilter)
                    .collect(Collectors.toList());
        } catch (IOException ignored) {
            return Collections.emptyList();
        }
    }

    private Predicate<Path> nonSnapshotFileFilter() {
        // 判断文件是不是 snapshot.
        return path -> {
            String name = path.getName();
            return !name.startsWith(SNAPSHOT_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    private Predicate<Path> nonChangelogFileFilter() {
        // 判断文件是不是 changelog.
        return path -> {
            String name = path.getName();
            return !name.startsWith(CHANGELOG_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    public Optional<Snapshot> latestSnapshotOfUser(String user) {
        // 找到某个 user 的最新 snapshot.
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return Optional.empty();
        }

        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");
        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot = snapshot(id);
            if (user.equals(snapshot.commitUser())) {
                return Optional.of(snapshot);
            }
        }
        return Optional.empty();
    }

    /**
     * 找打指定 user、指定 identifier 的 snapshot.
     *
     * <p>Find the snapshot of the specified identifiers written by the specified user.
     */
    public List<Snapshot> findSnapshotsForIdentifiers(
            @Nonnull String user, List<Long> identifiers) {
        if (identifiers.isEmpty()) {
            return Collections.emptyList();
        }
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return Collections.emptyList();
        }
        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");

        long minSearchedIdentifier = identifiers.stream().min(Long::compareTo).get();
        List<Snapshot> matchedSnapshots = new ArrayList<>();
        Set<Long> remainingIdentifiers = new HashSet<>(identifiers);
        for (long id = latestId; id >= earliestId && !remainingIdentifiers.isEmpty(); id--) {
            Snapshot snapshot = snapshot(id);
            if (user.equals(snapshot.commitUser())) {
                if (remainingIdentifiers.remove(snapshot.commitIdentifier())) {
                    matchedSnapshots.add(snapshot);
                }
                if (snapshot.commitIdentifier() <= minSearchedIdentifier) {
                    break;
                }
            }
        }
        return matchedSnapshots;
    }

    public void commitChangelog(Changelog changelog, long id) throws IOException {
        // 提交 changelog.
        fileIO.writeFileUtf8(longLivedChangelogPath(id), changelog.toJson());
    }

    /**
     * 筛选符合条件的最新 snapshot. writer 某些情况会执行这个操作，但是 snapshot 可能因过时被 committer 删除了，这个方法可以忽略掉这些异常.
     *
     * <p>Traversal snapshots from latest to earliest safely, this is applied on the writer side
     * because the committer may delete obsolete snapshots, which may cause the writer to encounter
     * unreadable snapshots.
     */
    @Nullable
    public Snapshot traversalSnapshotsFromLatestSafely(Filter<Snapshot> checker) {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return null;
        }
        Long earliestId = earliestSnapshotId();
        if (earliestId == null) {
            return null;
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id);
            } catch (Exception e) {
                Long newEarliestId = earliestSnapshotId();
                if (newEarliestId == null) {
                    return null;
                }

                // this is a valid snapshot, should not throw exception
                if (id >= newEarliestId) {
                    throw e;
                }

                // ok, this is an expired snapshot
                return null;
            }

            if (checker.test(snapshot)) {
                return snapshot;
            }
        }
        return null;
    }

    private @Nullable Long findLatest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        if (!fileIO.exists(dir)) {
            return null;
        }
        // 先查找 LATEST hint
        Long snapshotId = readHint(LATEST, dir);
        if (snapshotId != null && snapshotId > 0) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!fileIO.exists(file.apply(nextSnapshot))) {
                return snapshotId;
            }
        }

        // 找不到则列出文件列表来找
        return findByListFiles(Math::max, dir, prefix);
    }

    private @Nullable Long findEarliest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        // 找到最早的 snapshot 或者 changelog.
        if (!fileIO.exists(dir)) {
            return null;
        }

        Long snapshotId = readHint(EARLIEST, dir);
        // null and it is the earliest only it exists
        if (snapshotId != null && fileIO.exists(file.apply(snapshotId))) {
            return snapshotId;
        }

        return findByListFiles(Math::min, dir, prefix);
    }

    public Long readHint(String fileName) {
        // 用于读取 EARLIEST 或 LATEST.
        return readHint(fileName, snapshotDirByBranch(DEFAULT_MAIN_BRANCH));
    }

    public Long readHint(String fileName, Path dir) {
        // 用于读取 EARLIEST 或 LATEST.
        Path path = new Path(dir, fileName);
        int retryNumber = 0;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                return fileIO.readOverwrittenFileUtf8(path).map(Long::parseLong).orElse(null);
            } catch (Exception ignored) {
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_HINT_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private Long findByListFiles(BinaryOperator<Long> reducer, Path dir, String prefix)
            throws IOException {
        // 列出文件，并根据 reducer 来选择某个文件
        return listVersionedFiles(fileIO, dir, prefix).reduce(reducer).orElse(null);
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        // 提交 LATEST hint
        commitLatestHint(snapshotId, DEFAULT_MAIN_BRANCH);
    }

    public void commitLatestHint(long snapshotId, String branchName) throws IOException {
        // 提交某个分支 LATEST hint
        commitHint(snapshotId, LATEST, snapshotDirByBranch(branchName));
    }

    public void commitLongLivedChangelogLatestHint(long snapshotId) throws IOException {
        // 提交 changelog 的 LATEST hint.
        commitHint(snapshotId, LATEST, changelogDirectory());
    }

    public void commitLongLivedChangelogEarliestHint(long snapshotId) throws IOException {
        // 提交 changelog 的 EARLIEST hint.
        commitHint(snapshotId, EARLIEST, changelogDirectory());
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        // 提交 EARLIEST hint.
        commitEarliestHint(snapshotId, DEFAULT_MAIN_BRANCH);
    }

    public void commitEarliestHint(long snapshotId, String branchName) throws IOException {
        // 提交 EARLIEST hint.
        commitHint(snapshotId, EARLIEST, snapshotDirByBranch(branchName));
    }

    private void commitHint(long snapshotId, String fileName, Path dir) throws IOException {
        Path hintFile = new Path(dir, fileName);
        // 原子性操作覆写 HINT 文件.
        fileIO.overwriteFileUtf8(hintFile, String.valueOf(snapshotId));
    }
}
