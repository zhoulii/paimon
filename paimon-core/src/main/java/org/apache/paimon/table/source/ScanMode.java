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

package org.apache.paimon.table.source;

/** Scan which part of the snapshot. */
public enum ScanMode {

    /**
     * 扫描 SNAPSHOT 全量文件.
     *
     * <p>Scan complete data files of a snapshot.
     */
    ALL,

    /**
     * 扫描 SNAPSHOT 增量文件.
     *
     * <p>Only scan newly changed files of a snapshot.
     */
    DELTA,

    /**
     * 扫描 changelog 文件，指定 changelog producer 时流读会使用这种模式. overwrite snapshot 除外，读取 overwrite snapshot
     * 时强制使用 DELTA.
     *
     * <p>Only scan changelog files of a snapshot.
     */
    CHANGELOG
}
